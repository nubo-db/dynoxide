#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
use crate::errors::{DynoxideError, Result};
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
use crate::storage_backend::clock::{Clock, SystemClock};
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
use crate::storage_backend::sql_builders::{self, escape_table_name};
use crate::types::AttributeValue;
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
use rusqlite::{Connection, params};
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
use std::{cell::RefCell, collections::HashMap, sync::Arc};

// The schema version this build writes. It lives in `sql_builders` so the
// wasm backend records the same version on open.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
use crate::storage_backend::sql_builders::SCHEMA_VERSION;

/// How long a connection to a database file waits, while it opens, for a lock
/// another process holds before it reports the database locked. Another
/// Dynoxide opening the same file holds its write lock while it brings the
/// file up to date, which can take longer than five seconds on a large file.
/// Once the file is open the connection waits [`REQUEST_BUSY_TIMEOUT`]
/// instead. An in-memory database has no other process to wait for and keeps
/// the default throughout.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
const OPEN_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// How long an open connection waits for a lock another process holds:
/// rusqlite's default. Every request waits behind the one holding the
/// connection, so a request held up by another process gives up after this
/// rather than holding the rest up for [`OPEN_BUSY_TIMEOUT`].
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
const REQUEST_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[cfg(test)]
thread_local! {
    /// The busy timeout a connection on this thread had when it finished
    /// opening: its migrations ran under it. For tests of the two phases.
    static OPENED_WITH_BUSY_TIMEOUT_MS: std::cell::Cell<Option<i64>> =
        const { std::cell::Cell::new(None) };
}

/// Number of hash buckets used for parallel scan segment assignment.
/// Matches dynalite's implementation.
pub(crate) const HASH_BUCKETS: u32 = 4096;

// ---------------------------------------------------------------------------
// MD5-based hash prefix for parallel scan (dynalite-compatible)
// ---------------------------------------------------------------------------

/// Compute the 6-character hex hash prefix for a partition key value.
///
/// Uses `MD5("Outliers" + key_bytes)` where `key_bytes` depends on the
/// attribute type:
/// - `S`: UTF-8 bytes of the string
/// - `N`: Oracle packed BCD encoding via `num_to_buffer`
/// - `B`: raw binary bytes
///
/// This matches dynalite's `hashPrefix` function.
pub fn compute_hash_prefix(pk_value: &AttributeValue) -> String {
    let key_bytes = match pk_value {
        AttributeValue::S(s) => s.as_bytes().to_vec(),
        AttributeValue::N(n) => num_to_buffer(n),
        AttributeValue::B(b) => b.clone(),
        _ => vec![], // Should not happen for valid keys
    };

    let digest = md5::compute([b"Outliers" as &[u8], &key_bytes].concat());
    format!("{:032x}", digest)[..6].to_string()
}

/// Compute the hash bucket (0..4095) from a 6-character hex hash prefix.
pub fn hash_bucket(hash_prefix: &str) -> u32 {
    let prefix_3 = &hash_prefix[..3.min(hash_prefix.len())];
    u32::from_str_radix(prefix_3, 16).unwrap_or(0)
}

/// Convert a DynamoDB number string to Oracle-style packed BCD bytes.
///
/// Faithfully ports dynalite's `numToBuffer` function from `db/index.js`.
/// Uses Big.js semantics: `num.s` (sign), `num.c` (coefficient digits),
/// `num.e` (exponent = position of first digit relative to decimal point - 1).
fn num_to_buffer(num_str: &str) -> Vec<u8> {
    let trimmed = num_str.trim();
    if trimmed.is_empty() {
        return vec![0x80];
    }

    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    let bd = match BigDecimal::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return vec![0x80],
    };

    if bd.sign() == bigdecimal::num_bigint::Sign::NoSign {
        return vec![0x80];
    }

    let is_negative = bd.sign() == bigdecimal::num_bigint::Sign::Minus;
    let bd_abs = if is_negative { -&bd } else { bd.clone() };

    let (mantissa, exponent) = extract_mantissa_and_exponent(&bd_abs);
    if mantissa.is_empty() {
        return vec![0x80];
    }

    // JS: scale = num.s (-1 for negative, 1 for positive)
    // JS: appendZero = exponent % 2 ? 1 : 0
    let append_zero: i64 = if exponent % 2 != 0 { 1 } else { 0 };
    let byte_len_no_exp = ((mantissa.len() as i64 + append_zero + 1) / 2) as usize;

    let mut byte_array: Vec<u8>;
    if byte_len_no_exp < 20 && is_negative {
        byte_array = vec![0u8; byte_len_no_exp + 2];
        byte_array[byte_len_no_exp + 1] = 102;
    } else {
        byte_array = vec![0u8; byte_len_no_exp + 1];
    }

    // byteArray[0] = Math.floor((exponent + appendZero) / 2) - 64
    // For negative exponents, JS Math.floor(-3/2) = -2, matching Rust integer division
    // for negative values we need floor division, not truncation.
    let exp_sum = exponent + append_zero;
    let exp_byte_val = floor_div(exp_sum, 2) - 64;
    if is_negative {
        // byteArray[0] ^= 0xffffffff - JS bitwise XOR on a number
        // This effectively does a bitwise NOT on the low byte
        byte_array[0] = (exp_byte_val ^ !0i64) as u8;
    } else {
        byte_array[0] = exp_byte_val as u8;
    }

    // The main loop faithfully mirrors the JS for loop.
    // JS uses mantissaIndex as a signed int that can be decremented to -1.
    let mut mi: i64 = 0; // mantissaIndex (signed to allow -1)
    let mlen = mantissa.len() as i64;
    let mut appended_zero = false;

    while mi < mlen {
        let bai = ((mi + append_zero) / 2 + 1) as usize; // byteArrayIndex
        if append_zero != 0 && mi == 0 && !appended_zero {
            byte_array[bai] = 0;
            appended_zero = true;
            mi -= 1; // JS: mantissaIndex--
        } else if (mi + append_zero) % 2 == 0 {
            byte_array[bai] = mantissa[mi as usize] * 10;
        } else {
            byte_array[bai] += mantissa[mi as usize];
        }

        // Finalise byte: if odd position or last mantissa digit
        if ((mi + append_zero) % 2 != 0) || (mi == mlen - 1) {
            if is_negative {
                byte_array[bai] = 101u8.wrapping_sub(byte_array[bai]);
            } else {
                byte_array[bai] = byte_array[bai].wrapping_add(1);
            }
        }

        mi += 1; // JS: for loop increment
    }

    byte_array
}

/// Floor division for signed integers (matching JS Math.floor for division).
fn floor_div(a: i64, b: i64) -> i64 {
    let d = a / b;
    let r = a % b;
    if (r != 0) && ((r ^ b) < 0) { d - 1 } else { d }
}

/// Extract mantissa digits and exponent from a BigDecimal.
///
/// Returns (digits, exponent) matching Big.js semantics where:
/// - digits: array of individual digits [2, 5, 1] for "251"
/// - exponent: number of digits before the decimal point
///   e.g., "251" → exponent=3, "0.012345" → exponent=-1
fn extract_mantissa_and_exponent(bd: &bigdecimal::BigDecimal) -> (Vec<u8>, i64) {
    // Normalize to remove trailing zeros
    let normalized = bd.normalized();

    // Get the string representation without scientific notation
    // We need the digits and the position of the decimal point
    let (bigint, scale) = normalized.as_bigint_and_exponent();
    let digits_str = bigint.to_string();
    let digits_str = digits_str.trim_start_matches('-');

    let digits: Vec<u8> = digits_str
        .chars()
        .map(|c| c.to_digit(10).unwrap() as u8)
        .collect();

    // scale = number of digits after decimal point in the representation
    // exponent (Big.js style) = number_of_digits - scale = digits.len() as i64 - scale
    let exponent = digits.len() as i64 - scale;

    (digits, exponent)
}

/// Check whether a hash_prefix falls within the range for a given segment.
///
/// Uses dynalite's 4096-bucket scheme:
/// - bucket = parseInt(hash_prefix[0..3], 16)
/// - segment owns buckets from ceil(4096 * segment / total) to
///   ceil(4096 * (segment+1) / total) - 1
pub fn hash_in_segment(hash_prefix: &str, segment: u32, total_segments: u32) -> bool {
    let bucket = hash_bucket(hash_prefix);
    let start = ceiling_div(HASH_BUCKETS * segment, total_segments);
    let end = ceiling_div(HASH_BUCKETS * (segment + 1), total_segments) - 1;
    bucket >= start && bucket <= end
}

pub(crate) fn ceiling_div(a: u32, b: u32) -> u32 {
    a.div_ceil(b)
}

/// Parameters for scan operations (base table or GSI).
#[derive(Debug, Default)]
pub struct ScanParams<'a> {
    pub limit: Option<usize>,
    pub exclusive_start_pk: Option<&'a str>,
    pub exclusive_start_sk: Option<&'a str>,
    pub segment: Option<u32>,
    pub total_segments: Option<u32>,
    /// For LSI pagination: base table PK for composite cursor.
    pub exclusive_start_base_pk: Option<&'a str>,
    /// For LSI pagination: base table SK for composite cursor.
    pub exclusive_start_base_sk: Option<&'a str>,
}

/// Parameters for inserting table metadata.
#[derive(Debug, Default)]
pub struct CreateTableMetadata<'a> {
    pub table_name: &'a str,
    pub key_schema: &'a str,
    pub attribute_definitions: &'a str,
    pub gsi_definitions: Option<&'a str>,
    pub lsi_definitions: Option<&'a str>,
    pub provisioned_throughput: Option<&'a str>,
    pub created_at: i64,
    pub sse_specification: Option<&'a str>,
    pub table_class: Option<&'a str>,
    pub deletion_protection_enabled: bool,
    pub billing_mode: Option<&'a str>,
    pub on_demand_throughput: Option<&'a str>,
    pub vector_index_definitions: Option<&'a str>,
}

/// Parameters for query operations (base table or GSI).
#[derive(Debug, Default)]
pub struct QueryParams<'a> {
    pub sk_condition: Option<&'a str>,
    pub sk_params: &'a [&'a str],
    pub forward: bool,
    pub limit: Option<usize>,
    pub exclusive_start_sk: Option<&'a str>,
    /// For LSI pagination: base table PK for composite cursor.
    pub exclusive_start_base_pk: Option<&'a str>,
    /// For LSI pagination: base table SK for composite cursor.
    pub exclusive_start_base_sk: Option<&'a str>,
}

/// Low-level SQLite storage layer.
///
/// Manages the SQLite connection, metadata tables, and per-DynamoDB-table
/// data tables. All SQL lives here: higher layers work with Rust types.
///
/// Native-only: this type is the rusqlite-backed backend and is compiled out
/// of backend-neutral builds (for example `wasm-sqlite`).
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
pub struct Storage {
    conn: Connection,
    /// In-memory cache of table metadata to avoid repeated SQLite reads.
    /// Safe to use `RefCell` because `Storage` is always behind `Arc<Mutex<>>`.
    metadata_cache: RefCell<HashMap<String, TableMetadata>>,
    /// Wall-clock used by stream / TTL paths. Defaults to [`SystemClock`].
    clock: Arc<dyn Clock>,
    /// Countdown to a forced GSI insert failure, for tests that need a write
    /// to fail part way through its index fan-out. `None` never fires. Compiled
    /// out of every build but the crate's own unit tests.
    #[cfg(test)]
    gsi_insert_fault: RefCell<Option<u32>>,
    /// Countdown to a forced failure rewriting one row's stored size, for
    /// tests that need a size recompute to fail part way through. `None` never
    /// fires. Compiled out of every build but the crate's own unit tests.
    #[cfg(test)]
    item_size_fault: RefCell<Option<u32>>,
}

#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
impl Storage {
    /// Open a persistent database at the given path.
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(OPEN_BUSY_TIMEOUT)?;
        let mut storage = Self {
            conn,
            metadata_cache: RefCell::new(HashMap::new()),
            #[cfg(test)]
            gsi_insert_fault: RefCell::new(None),
            #[cfg(test)]
            item_size_fault: RefCell::new(None),
            clock: Arc::new(SystemClock),
        };
        storage.initialize().map_err(Self::maybe_encrypted_error)?;
        storage.conn.busy_timeout(REQUEST_BUSY_TIMEOUT)?;
        Ok(storage)
    }

    /// Let `successes` GSI inserts through, then fail every one after that, so
    /// a write fails part way through its index fan-out. Nothing else can
    /// produce that: a fan-out either runs to completion or the whole write
    /// was rejected before it started. Pass 0 to fail the first insert.
    #[cfg(test)]
    pub(crate) fn fail_gsi_insert_after(&self, successes: u32) {
        *self.gsi_insert_fault.borrow_mut() = Some(successes);
    }

    /// Consume one step of the armed countdown, erroring when it reaches zero.
    #[cfg(test)]
    pub(crate) fn check_gsi_insert_fault(&self) -> Result<()> {
        Self::count_down(&self.gsi_insert_fault, "injected GSI insert failure")
    }

    #[cfg(test)]
    fn count_down(fault: &RefCell<Option<u32>>, message: &str) -> Result<()> {
        let mut fault = fault.borrow_mut();
        match fault.as_mut() {
            Some(0) => Err(crate::errors::DynoxideError::InternalServerError(
                message.to_string(),
            )),
            Some(remaining) => {
                *remaining -= 1;
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// Replace the [`Clock`] used by the stream and TTL paths. Returns `self`
    /// so callers can chain construction (`Storage::memory()?.with_clock(c)`).
    ///
    /// Default for every constructor is [`SystemClock`]. Tests use this to
    /// inject a `ManualClock`.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Borrow the active [`Clock`]. Used by stream and TTL paths.
    pub(crate) fn clock(&self) -> &dyn Clock {
        self.clock.as_ref()
    }

    /// If a SQLite error is SQLITE_NOTADB, return a clearer error message
    /// suggesting the database may be encrypted.
    fn maybe_encrypted_error(err: DynoxideError) -> DynoxideError {
        if let DynoxideError::SqliteError(ref sqlite_err) = err
            && let Some(rusqlite::ErrorCode::NotADatabase) = sqlite_err.sqlite_error_code()
        {
            return DynoxideError::InternalServerError(
                "Database file is encrypted or not a valid SQLite database. \
                     If encrypted, enable the `encryption` or `encryption-cc` feature \
                     and use Database::new_encrypted() with the correct key."
                    .to_string(),
            );
        }
        err
    }

    /// Open or create an encrypted persistent database at the given path.
    ///
    /// The key is passed to SQLCipher via `PRAGMA key`. The database file is
    /// encrypted at rest using AES-256-CBC. A database opened without calling
    /// `PRAGMA key` is treated as a normal unencrypted database by SQLCipher.
    #[cfg(feature = "_has-encryption")]
    pub fn new_encrypted(path: &str, key: &str) -> Result<Self> {
        use zeroize::Zeroize;

        let conn = Connection::open(path)?;
        conn.busy_timeout(OPEN_BUSY_TIMEOUT)?;
        // Safety: key is validated to be exactly 64 hex characters [0-9a-fA-F]
        // by Database::new_encrypted(), so no injection is possible in the
        // x'...' hex literal format. Note: pragma_update does NOT use parameter
        // binding for the PRAGMA value: hex validation is the sole injection defense.
        let mut pragma_val = format!("x'{key}'");
        conn.pragma_update(None, "key", &pragma_val)?;
        pragma_val.zeroize();
        // Verify the key works by reading from the database
        conn.execute_batch("SELECT count(*) FROM sqlite_master;")?;
        let mut storage = Self {
            conn,
            metadata_cache: RefCell::new(HashMap::new()),
            #[cfg(test)]
            gsi_insert_fault: RefCell::new(None),
            #[cfg(test)]
            item_size_fault: RefCell::new(None),
            clock: Arc::new(SystemClock),
        };
        storage.initialize()?;
        storage.conn.busy_timeout(REQUEST_BUSY_TIMEOUT)?;
        Ok(storage)
    }

    /// Open an in-memory database (for tests and ephemeral use).
    pub fn memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let mut storage = Self {
            conn,
            metadata_cache: RefCell::new(HashMap::new()),
            #[cfg(test)]
            gsi_insert_fault: RefCell::new(None),
            #[cfg(test)]
            item_size_fault: RefCell::new(None),
            clock: Arc::new(SystemClock),
        };
        storage.initialize()?;
        Ok(storage)
    }

    /// Initialize the database: WAL mode, metadata tables, config.
    fn initialize(&mut self) -> Result<()> {
        // Ahead of everything here that writes, the journal mode included, so
        // a file a newer build wrote is refused as it was found.
        refuse_newer_schema(&self.conn)?;
        // Enable WAL mode for better concurrency
        self.conn.pragma_update(None, "journal_mode", "WAL")?;
        Self::register_functions(&self.conn)?;
        self.migrate_to_current()?;
        #[cfg(test)]
        OPENED_WITH_BUSY_TIMEOUT_MS.set(Some(self.conn.query_row(
            "PRAGMA busy_timeout",
            [],
            |r| r.get(0),
        )?));
        Ok(())
    }

    /// Register the SQL functions every statement this layer runs may call.
    fn register_functions(conn: &Connection) -> Result<()> {
        // Register FNV-1a hash function for parallel scan segment assignment.
        // Uses get_raw() to borrow directly from SQLite's buffer (zero-copy).
        conn.create_scalar_function(
            "fnv1a_hash",
            1,
            rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
                | rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            |ctx: &rusqlite::functions::Context| -> rusqlite::Result<i64> {
                let pk_ref = ctx.get_raw(0);
                let pk_bytes = match pk_ref {
                    rusqlite::types::ValueRef::Text(bytes) => bytes,
                    _ => {
                        return Err(rusqlite::Error::InvalidFunctionParameterType(
                            0,
                            rusqlite::types::Type::Text,
                        ));
                    }
                };
                let mut hash: u32 = 2166136261;
                for &byte in pk_bytes {
                    hash ^= byte as u32;
                    hash = hash.wrapping_mul(16777619);
                }
                Ok(hash as i64)
            },
        )?;
        Ok(())
    }

    /// Create the metadata tables if they are missing and run every migration
    /// the recorded schema version calls for. Runs on open, and on the scratch
    /// copy a restore brings up to date before it replaces this database.
    fn migrate_to_current(&self) -> Result<()> {
        // A database with tables and no recorded version, which the browser
        // build wrote from Dynoxide 0.10 to 0.13, or one whose version isn't a
        // number, runs every step. Each is safe on a database that already
        // has what it adds: a column already there is left alone, an index is
        // created only where it is missing, only a table without an id is
        // given one, and the steps from version 10 on rebuild from the base
        // table.
        let version = self.create_metadata_tables()?.unwrap_or(1);

        if version < 2 {
            self.migrate_v1_to_v2()?;
        }
        if version < 3 {
            self.migrate_v2_to_v3()?;
        }
        if version < 4 {
            self.migrate_v3_to_v4()?;
        }
        if version < 5 {
            self.migrate_v4_to_v5()?;
        }
        if version < 6 {
            self.migrate_v5_to_v6()?;
        }
        if version < 7 {
            self.migrate_v6_to_v7()?;
        }
        if version < 8 {
            self.migrate_v7_to_v8()?;
        }
        if version < 9 {
            self.migrate_v8_to_v9()?;
        }
        // The steps from version 10 on are shared with the wasm backend and run
        // each in its own write transaction. The native futures never suspend,
        // so `block_on` returns as soon as they do.
        pollster::block_on(crate::actions::schema_migrations::run_open_migrations(
            self,
            crate::storage_backend::schema::open_migrations(Some(version)),
        ))
    }

    /// Create the metadata tables if they are missing and, on a new database,
    /// record this build's version, in one write transaction, so another
    /// process opening the same file never finds the tables without the
    /// version. Returns the version recorded once that is done.
    fn create_metadata_tables(&self) -> Result<Option<u32>> {
        let transaction = rusqlite::Transaction::new_unchecked(
            &self.conn,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        let new_database = !table_exists(&self.conn, "_tables")?;
        // The metadata tables, shared with the wasm backend.
        self.conn.execute_batch(sql_builders::INIT_SCHEMA)?;
        // A stream record table made before records carried a user identity
        // gains the column; one that has it fails the ALTER harmlessly.
        let _ = self
            .conn
            .execute_batch("ALTER TABLE _stream_records ADD COLUMN user_identity TEXT");
        // Likewise the event id; records written before it existed keep the
        // id they always reported.
        let has_event_id: bool =
            self.conn
                .query_row(sql_builders::STREAM_RECORDS_HAVE_EVENT_ID, [], |row| {
                    row.get(0)
                })?;
        if !has_event_id {
            self.conn
                .execute_batch(sql_builders::ADD_STREAM_EVENT_ID_COLUMN)?;
            self.conn
                .execute_batch(sql_builders::BACKFILL_STREAM_EVENT_IDS)?;
        }
        if new_database {
            let (sql, params) = sql_builders::init_schema_version(SCHEMA_VERSION);
            self.conn
                .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        }
        let recorded = self.recorded_schema_version()?;
        transaction.commit()?;
        Ok(recorded)
    }

    /// Migrate from schema v1 to v2: add `cached_at REAL` column to all data tables.
    fn migrate_v1_to_v2(&self) -> Result<()> {
        let mut stmt = self.conn.prepare("SELECT table_name FROM _tables")?;
        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        for table_name in &table_names {
            let escaped = format!("\"{}\"", table_name.replace('"', "\"\""));
            let _ = self.conn.execute(
                &format!("ALTER TABLE {escaped} ADD COLUMN cached_at REAL"),
                [],
            );
        }

        self.conn.execute(
            "INSERT OR REPLACE INTO _config (key, value) VALUES ('schema_version', '2')",
            [],
        )?;

        Ok(())
    }

    /// Migrate from schema v2 to v3: add `tags TEXT` column to `_tables`.
    fn migrate_v2_to_v3(&self) -> Result<()> {
        let _ = self
            .conn
            .execute("ALTER TABLE _tables ADD COLUMN tags TEXT", []);

        self.conn.execute(
            "INSERT OR REPLACE INTO _config (key, value) VALUES ('schema_version', '3')",
            [],
        )?;

        Ok(())
    }

    /// Migrate from schema v3 to v4: add SSE, table class, and deletion protection columns.
    fn migrate_v3_to_v4(&self) -> Result<()> {
        let _ = self
            .conn
            .execute("ALTER TABLE _tables ADD COLUMN sse_specification TEXT", []);
        let _ = self
            .conn
            .execute("ALTER TABLE _tables ADD COLUMN table_class TEXT", []);
        let _ = self.conn.execute(
            "ALTER TABLE _tables ADD COLUMN deletion_protection_enabled INTEGER DEFAULT 0",
            [],
        );

        self.conn.execute(
            "INSERT OR REPLACE INTO _config (key, value) VALUES ('schema_version', '4')",
            [],
        )?;

        Ok(())
    }

    /// Migrate from schema v4 to v5: add secondary indexes on GSI and LSI base-key columns.
    fn migrate_v4_to_v5(&self) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT table_name, gsi_definitions, lsi_definitions FROM _tables")?;
        let tables: Vec<(String, Option<String>, Option<String>)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        for (table_name, gsi_json, lsi_json) in &tables {
            if let Some(json) = gsi_json
                && let Ok(gsis) = serde_json::from_str::<Vec<serde_json::Value>>(json)
            {
                for gsi in &gsis {
                    if let Some(idx) = gsi.get("IndexName").and_then(|v| v.as_str()) {
                        let gsi_table = escape_table_name(&format!("{table_name}::gsi::{idx}"));
                        let idx_name =
                            escape_table_name(&format!("{table_name}::gsi::{idx}::base_key"));
                        let _ = self.conn.execute_batch(&format!(
                                "CREATE INDEX IF NOT EXISTS \"{idx_name}\" ON \"{gsi_table}\" (table_pk, table_sk)"
                            ));
                    }
                }
            }
            if let Some(json) = lsi_json
                && let Ok(lsis) = serde_json::from_str::<Vec<serde_json::Value>>(json)
            {
                for lsi in &lsis {
                    if let Some(idx) = lsi.get("IndexName").and_then(|v| v.as_str()) {
                        let lsi_table = escape_table_name(&format!("{table_name}::lsi::{idx}"));
                        let idx_name =
                            escape_table_name(&format!("{table_name}::lsi::{idx}::base_key"));
                        let _ = self.conn.execute_batch(&format!(
                                "CREATE INDEX IF NOT EXISTS \"{idx_name}\" ON \"{lsi_table}\" (base_pk, base_sk)"
                            ));
                    }
                }
            }
        }

        self.conn.execute(
            "INSERT OR REPLACE INTO _config (key, value) VALUES ('schema_version', '5')",
            [],
        )?;

        Ok(())
    }

    /// Migrate from schema v5 to v6: add `hash_prefix TEXT` column to all data tables.
    fn migrate_v5_to_v6(&self) -> Result<()> {
        let mut stmt = self.conn.prepare("SELECT table_name FROM _tables")?;
        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        for table_name in &table_names {
            let escaped = escape_table_name(table_name);
            let _ = self.conn.execute(
                &format!(
                    "ALTER TABLE \"{escaped}\" ADD COLUMN hash_prefix TEXT NOT NULL DEFAULT ''"
                ),
                [],
            );
        }

        self.conn.execute(
            "INSERT OR REPLACE INTO _config (key, value) VALUES ('schema_version', '6')",
            [],
        )?;

        Ok(())
    }

    /// Migrate from schema v6 to v7: add `on_demand_throughput TEXT` column to `_tables`.
    fn migrate_v6_to_v7(&self) -> Result<()> {
        let _ = self.conn.execute(
            "ALTER TABLE _tables ADD COLUMN on_demand_throughput TEXT",
            [],
        );

        self.conn.execute(
            "INSERT OR REPLACE INTO _config (key, value) VALUES ('schema_version', '7')",
            [],
        )?;

        Ok(())
    }

    /// Migrate from schema v7 to v8: add a `table_id TEXT` column to `_tables`
    /// and backfill existing tables with a one-time random UUID.
    ///
    /// TableId must stay stable between reads of the same table (#55). New
    /// tables get their id at insert time; tables created before this column
    /// existed are assigned a persisted id here, once. The backfill `SELECT`
    /// references `table_id`, so if the `ALTER` did not actually add the column
    /// (a genuine failure, swallowed below for the duplicate-column re-run case)
    /// this step errors out before the version is stamped, rather than stamping
    /// a half-applied migration.
    fn migrate_v7_to_v8(&self) -> Result<()> {
        let _ = self
            .conn
            .execute("ALTER TABLE _tables ADD COLUMN table_id TEXT", []);

        let names: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT table_name FROM _tables WHERE table_id IS NULL")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<Vec<String>>>()?
        };
        for name in names {
            let id = uuid::Uuid::new_v4().to_string();
            self.conn.execute(
                "UPDATE _tables SET table_id = ?1 WHERE table_name = ?2",
                params![id, name],
            )?;
        }

        self.conn.execute(
            "INSERT OR REPLACE INTO _config (key, value) VALUES ('schema_version', '8')",
            [],
        )?;

        Ok(())
    }

    /// Migrate from schema v8 to v9: add a `vector_index_definitions TEXT`
    /// column to `_tables`.
    ///
    /// Only the duplicate-column error (a re-run over a half-applied
    /// migration whose version stamp did not land) is tolerated; any other
    /// failure propagates rather than stamping a version whose column is
    /// silently absent.
    fn migrate_v8_to_v9(&self) -> Result<()> {
        if let Err(e) = self
            .conn
            .execute(sql_builders::ADD_VECTOR_INDEX_DEFINITIONS_COLUMN, [])
            && !sql_builders::is_duplicate_column_error(&e.to_string())
        {
            return Err(e.into());
        }

        self.conn.execute(
            "INSERT OR REPLACE INTO _config (key, value) VALUES ('schema_version', '9')",
            [],
        )?;

        Ok(())
    }

    /// Get a reference to the underlying connection (for transactions, etc.).
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Get a mutable reference to the underlying connection.
    pub fn conn_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }

    // -----------------------------------------------------------------------
    // Table metadata
    // -----------------------------------------------------------------------

    /// Insert a row into the `_tables` metadata table.
    pub fn insert_table_metadata(&self, m: &CreateTableMetadata) -> Result<()> {
        let table_name = m.table_name;
        let (sql, params) = sql_builders::insert_table_metadata(m);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Get metadata for a table. Returns None if the table doesn't exist.
    ///
    /// Results are cached in memory. The cache is invalidated when metadata
    /// is modified via `insert_table_metadata`, `delete_table_metadata`,
    /// `enable_stream`, or `update_ttl_config`.
    pub fn get_table_metadata(&self, table_name: &str) -> Result<Option<TableMetadata>> {
        // Check cache first
        if let Some(cached) = self.metadata_cache.borrow().get(table_name) {
            return Ok(Some(cached.clone()));
        }

        let (sql, params) = sql_builders::get_table_metadata(table_name);
        let mut stmt = self.conn.prepare(&sql)?;

        let result = stmt.query_row(rusqlite::params_from_iter(params.iter()), row_to_metadata);

        match result {
            Ok(meta) => {
                self.metadata_cache
                    .borrow_mut()
                    .insert(table_name.to_string(), meta.clone());
                Ok(Some(meta))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(DynoxideError::from(e)),
        }
    }

    /// Delete metadata for a table.
    pub fn delete_table_metadata(&self, table_name: &str) -> Result<bool> {
        let (sql, params) = sql_builders::delete_table_metadata(table_name);
        let affected = self
            .conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(affected > 0)
    }

    /// Update attribute definitions and GSI definitions for a table.
    pub fn update_table_metadata(
        &self,
        table_name: &str,
        attribute_definitions: &str,
        gsi_definitions: Option<&str>,
    ) -> Result<()> {
        let (sql, params) =
            sql_builders::update_table_metadata(table_name, attribute_definitions, gsi_definitions);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Update vector index definitions for a table; `None` clears the column.
    pub fn update_vector_index_definitions(
        &self,
        table_name: &str,
        vector_index_definitions: Option<&str>,
    ) -> Result<()> {
        let (sql, params) =
            sql_builders::update_vector_index_definitions(table_name, vector_index_definitions);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Update provisioned throughput for a table.
    pub fn update_provisioned_throughput(
        &self,
        table_name: &str,
        provisioned_throughput: &str,
    ) -> Result<()> {
        let (sql, params) =
            sql_builders::update_provisioned_throughput(table_name, provisioned_throughput);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Clear provisioned throughput for a table (sets to SQL NULL).
    pub fn clear_provisioned_throughput(&self, table_name: &str) -> Result<()> {
        let (sql, params) = sql_builders::clear_provisioned_throughput(table_name);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Clear on-demand throughput ceilings for a table (sets to SQL NULL).
    pub fn clear_on_demand_throughput(&self, table_name: &str) -> Result<()> {
        let (sql, params) = sql_builders::clear_on_demand_throughput(table_name);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Update billing mode for a table.
    pub fn update_billing_mode(&self, table_name: &str, billing_mode: &str) -> Result<()> {
        let (sql, params) = sql_builders::update_billing_mode(table_name, billing_mode);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Update the table class for a table.
    pub fn update_table_class(&self, table_name: &str, table_class: &str) -> Result<()> {
        let (sql, params) = sql_builders::update_table_class(table_name, table_class);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Update the on-demand throughput (stored as JSON) for a table.
    pub fn update_on_demand_throughput(
        &self,
        table_name: &str,
        on_demand_throughput: &str,
    ) -> Result<()> {
        let (sql, params) =
            sql_builders::update_on_demand_throughput(table_name, on_demand_throughput);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tag operations
    // -----------------------------------------------------------------------

    /// Get tags for a table.
    pub fn get_tags(&self, table_name: &str) -> Result<Vec<crate::types::Tag>> {
        let tags_json: Option<String> = self.conn.query_row(
            "SELECT tags FROM _tables WHERE table_name = ?1",
            params![table_name],
            |row| row.get(0),
        )?;

        match tags_json {
            Some(json) => serde_json::from_str(&json)
                .map_err(|e| DynoxideError::InternalServerError(format!("Bad tags JSON: {e}"))),
            None => Ok(Vec::new()),
        }
    }

    /// Set (merge) tags on a table. New keys overwrite existing keys.
    pub fn set_tags(&self, table_name: &str, new_tags: &[crate::types::Tag]) -> Result<()> {
        use std::collections::BTreeMap;

        let existing = self.get_tags(table_name)?;
        let mut tag_map: BTreeMap<String, String> =
            existing.into_iter().map(|t| (t.key, t.value)).collect();

        for tag in new_tags {
            tag_map.insert(tag.key.clone(), tag.value.clone());
        }

        if tag_map.len() > 50 {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: \
                 Too many tags: tag limit is 50"
                    .to_string(),
            ));
        }

        let merged: Vec<crate::types::Tag> = tag_map
            .into_iter()
            .map(|(k, v)| crate::types::Tag { key: k, value: v })
            .collect();

        let json = serde_json::to_string(&merged)
            .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

        self.conn.execute(
            "UPDATE _tables SET tags = ?1 WHERE table_name = ?2",
            params![json, table_name],
        )?;
        Ok(())
    }

    /// Update the deletion protection setting for a table.
    pub fn update_deletion_protection(&self, table_name: &str, enabled: bool) -> Result<()> {
        let (sql, params) = sql_builders::update_deletion_protection(table_name, enabled);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Remove tags by key from a table.
    pub fn remove_tags(&self, table_name: &str, keys: &[String]) -> Result<()> {
        let mut tags = self.get_tags(table_name)?;
        tags.retain(|t| !keys.contains(&t.key));

        let json = if tags.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&tags)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?,
            )
        };

        self.conn.execute(
            "UPDATE _tables SET tags = ?1 WHERE table_name = ?2",
            params![json, table_name],
        )?;
        Ok(())
    }

    /// List all table names.
    pub fn list_table_names(&self) -> Result<Vec<String>> {
        let (sql, params) = sql_builders::list_table_names();
        let mut stmt = self.conn.prepare(&sql)?;
        let names = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(names)
    }

    /// Check if a table exists in metadata.
    pub fn table_exists(&self, table_name: &str) -> Result<bool> {
        let (sql, params) = sql_builders::table_exists(table_name);
        let count: i32 =
            self.conn
                .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                    row.get(0)
                })?;
        Ok(count > 0)
    }

    /// Invalidate the cached metadata for a specific table.
    #[allow(dead_code)]
    pub(crate) fn invalidate_metadata_cache(&self, table_name: &str) {
        self.metadata_cache.borrow_mut().remove(table_name);
    }

    // -----------------------------------------------------------------------
    // Dynamic data tables
    // -----------------------------------------------------------------------

    /// Create a data table for a DynamoDB table.
    pub fn create_data_table(&self, table_name: &str) -> Result<()> {
        let (sql, params) = sql_builders::create_data_table(table_name);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    /// Drop a data table.
    pub fn drop_data_table(&self, table_name: &str) -> Result<()> {
        let (sql, params) = sql_builders::drop_data_table(table_name);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    /// Create a GSI table.
    pub fn create_gsi_table(&self, table_name: &str, index_name: &str) -> Result<()> {
        let (sql, _) = sql_builders::create_gsi_table(table_name, index_name);
        self.conn.execute_batch(&sql)?;
        Ok(())
    }

    /// Drop a GSI table.
    pub fn drop_gsi_table(&self, table_name: &str, index_name: &str) -> Result<()> {
        let (sql, params) = sql_builders::drop_gsi_table(table_name, index_name);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // GSI item operations
    // -----------------------------------------------------------------------

    /// Insert an item into a GSI table.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_gsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        gsi_pk: &str,
        gsi_sk: &str,
        table_pk: &str,
        table_sk: &str,
        item_json: &str,
    ) -> Result<()> {
        let sql = sql_builders::gsi_insert_sql(table_name, index_name);
        let params = sql_builders::gsi_insert_params(gsi_pk, gsi_sk, table_pk, table_sk, item_json);
        self.conn
            .prepare_cached(&sql)?
            .execute(rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    /// Bulk-insert many rows into one GSI table using a single cached
    /// prepared statement. Equivalent to calling [`Self::insert_gsi_item`]
    /// once per row, but reuses the statement across the batch.
    pub fn insert_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        rows: &[crate::storage_backend::GsiItemRow],
    ) -> Result<()> {
        let sql = sql_builders::gsi_insert_sql(table_name, index_name);
        let mut stmt = self.conn.prepare_cached(&sql)?;
        for row in rows {
            let params = sql_builders::gsi_insert_params(
                &row.gsi_pk,
                &row.gsi_sk,
                &row.table_pk,
                &row.table_sk,
                &row.item_json,
            );
            stmt.execute(rusqlite::params_from_iter(params.iter()))?;
        }
        Ok(())
    }

    /// Delete an item from a GSI table by base table primary key.
    pub fn delete_gsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        table_pk: &str,
        table_sk: &str,
    ) -> Result<()> {
        let (sql, params) =
            sql_builders::delete_gsi_item(table_name, index_name, table_pk, table_sk);
        self.conn
            .prepare_cached(&sql)?
            .execute(rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    /// Query items from a GSI table.
    pub fn query_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        gsi_pk: &str,
        params: &QueryParams,
    ) -> Result<Vec<(String, String, String)>> {
        let (sql, params_vec) =
            sql_builders::query_gsi_items(table_name, index_name, gsi_pk, params);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    /// Scan all items from a GSI table.
    pub fn scan_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        params: &ScanParams,
    ) -> Result<Vec<(String, String, String)>> {
        let (sql, params_vec) = sql_builders::scan_gsi_items(table_name, index_name, params);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // LSI table operations
    // -----------------------------------------------------------------------

    /// Create an LSI table for a given base table and index name.
    pub fn create_lsi_table(&self, table_name: &str, index_name: &str) -> Result<()> {
        let (sql, _) = sql_builders::create_lsi_table(table_name, index_name);
        self.conn.execute_batch(&sql)?;
        Ok(())
    }

    /// Create a vector index shadow table.
    pub fn create_vector_table(&self, table_name: &str, index_name: &str) -> Result<()> {
        let (sql, _) = sql_builders::create_vector_table(table_name, index_name);
        self.conn.execute_batch(&sql)?;
        Ok(())
    }

    /// Bulk-insert many rows into one vector shadow table using a single
    /// cached prepared statement, mirroring [`Self::insert_gsi_items`].
    pub fn insert_vector_items(
        &self,
        table_name: &str,
        index_name: &str,
        rows: &[crate::storage_backend::VectorItemRow],
    ) -> Result<()> {
        let sql = sql_builders::vector_insert_sql(table_name, index_name);
        let mut stmt = self.conn.prepare_cached(&sql)?;
        for row in rows {
            let params = sql_builders::vector_insert_params(
                &row.table_pk,
                &row.table_sk,
                &row.hash_value,
                &row.vector_json,
                &row.filter_json,
                &row.item_json,
                row.entry_bytes,
            );
            stmt.execute(rusqlite::params_from_iter(params.iter()))?;
        }
        Ok(())
    }

    /// Delete a vector shadow-table row by base table primary key, mirroring
    /// [`Self::delete_gsi_item`].
    pub fn delete_vector_item(
        &self,
        table_name: &str,
        index_name: &str,
        table_pk: &str,
        table_sk: &str,
    ) -> Result<()> {
        let (sql, params) =
            sql_builders::delete_vector_item(table_name, index_name, table_pk, table_sk);
        self.conn
            .prepare_cached(&sql)?
            .execute(rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    /// Load vector shadow-table candidate rows for a search, optionally
    /// scoped to one SearchSchema HASH value. Ordered by base-table primary
    /// key (see the builder) so equal scores tie-break deterministically.
    pub fn query_vector_candidates(
        &self,
        table_name: &str,
        index_name: &str,
        hash_value: Option<&str>,
    ) -> Result<Vec<crate::storage_backend::VectorCandidateRow>> {
        let (sql, params) =
            sql_builders::query_vector_candidates(table_name, index_name, hash_value);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                Ok(crate::storage_backend::VectorCandidateRow {
                    table_pk: row.get(0)?,
                    table_sk: row.get(1)?,
                    vector_json: row.get(2)?,
                    filter_json: row.get(3)?,
                    entry_bytes: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Load the projected entries for the keys a search returns.
    pub fn vector_items_for_keys(
        &self,
        table_name: &str,
        index_name: &str,
        keys: &[(String, String)],
    ) -> Result<Vec<(String, String, String)>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let (sql, params) = sql_builders::vector_items_for_keys(table_name, index_name, keys);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Drop a vector index shadow table.
    pub fn drop_vector_table(&self, table_name: &str, index_name: &str) -> Result<()> {
        let (sql, params) = sql_builders::drop_vector_table(table_name, index_name);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    /// Drop an LSI table.
    pub fn drop_lsi_table(&self, table_name: &str, index_name: &str) -> Result<()> {
        let (sql, params) = sql_builders::drop_lsi_table(table_name, index_name);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // LSI item operations
    // -----------------------------------------------------------------------

    /// Insert an item into an LSI table.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_lsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
        sk: &str,
        base_pk: &str,
        base_sk: &str,
        item_json: &str,
    ) -> Result<()> {
        let sql = sql_builders::lsi_insert_sql(table_name, index_name);
        let params = sql_builders::lsi_insert_params(pk, sk, base_pk, base_sk, item_json);
        self.conn
            .prepare_cached(&sql)?
            .execute(rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    /// Delete an item from an LSI table by base table primary key.
    pub fn delete_lsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        base_pk: &str,
        base_sk: &str,
    ) -> Result<()> {
        let (sql, params) = sql_builders::delete_lsi_item(table_name, index_name, base_pk, base_sk);
        self.conn
            .prepare_cached(&sql)?
            .execute(rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    /// Query items from an LSI table.
    pub fn query_lsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
        params: &QueryParams,
    ) -> Result<Vec<(String, String, String)>> {
        let (sql, params_vec) = sql_builders::query_lsi_items(table_name, index_name, pk, params);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    /// Scan all items from an LSI table.
    pub fn scan_lsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        params: &ScanParams,
    ) -> Result<Vec<(String, String, String)>> {
        let (sql, params_vec) = sql_builders::scan_lsi_items(table_name, index_name, params);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Transaction support
    // -----------------------------------------------------------------------

    /// Begin an immediate SQLite transaction.
    pub fn begin_transaction(&self) -> Result<()> {
        self.conn.execute_batch(sql_builders::BEGIN)?;
        Ok(())
    }

    /// Commit the current transaction.
    pub fn commit(&self) -> Result<()> {
        self.conn.execute_batch(sql_builders::COMMIT)?;
        Ok(())
    }

    /// Rollback the current transaction.
    pub fn rollback(&self) -> Result<()> {
        self.conn.execute_batch(sql_builders::ROLLBACK)?;
        Ok(())
    }

    /// The schema version the database records, or `None` when it records
    /// none, or one that isn't a number.
    pub(crate) fn recorded_schema_version(&self) -> Result<Option<u32>> {
        use rusqlite::OptionalExtension;
        let value: Option<String> = self
            .conn
            .query_row(sql_builders::GET_SCHEMA_VERSION, [], |r| r.get(0))
            .optional()?;
        Ok(crate::storage_backend::schema::parse_recorded(
            value.as_deref(),
        ))
    }

    /// Record `version` as the database's schema version, replacing any
    /// recorded one.
    pub(crate) fn record_schema_version(&self, version: u32) -> Result<()> {
        let (sql, params) = sql_builders::set_schema_version(version);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;
        Ok(())
    }

    /// Set aggressive PRAGMAs for bulk loading.
    ///
    /// Disables fsync, increases cache, and enables memory-mapped I/O.
    /// Only safe when data loss on crash is acceptable (e.g., fresh import
    /// that can be re-run).
    pub fn enable_bulk_loading(&self) -> Result<()> {
        self.conn.execute_batch(
            "PRAGMA synchronous = OFF;
             PRAGMA cache_size = -64000;
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = 268435456;",
        )?;
        Ok(())
    }

    /// Restore normal PRAGMAs after bulk loading.
    pub fn disable_bulk_loading(&self) -> Result<()> {
        self.conn.execute_batch(
            "PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -2000;
             PRAGMA temp_store = DEFAULT;
             PRAGMA mmap_size = 0;",
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Item CRUD
    // -----------------------------------------------------------------------

    /// Insert or replace an item.
    pub fn put_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        item_json: &str,
        item_size: usize,
    ) -> Result<Option<String>> {
        self.put_item_with_hash(table_name, pk, sk, item_json, item_size, "")
    }

    /// Put an item with an explicit hash prefix for parallel scan ordering.
    pub fn put_item_with_hash(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        item_json: &str,
        item_size: usize,
        hash_prefix: &str,
    ) -> Result<Option<String>> {
        // First, try to get the old item for return value
        let old_item = self.get_item(table_name, pk, sk)?;

        let (sql, params) =
            sql_builders::put_item_with_hash(table_name, pk, sk, item_json, item_size, hash_prefix);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;

        Ok(old_item)
    }

    /// Bulk-insert many base-table rows using a single cached prepared
    /// statement (`INSERT OR REPLACE`). Unlike [`Self::put_item_with_hash`],
    /// which preserves any existing `cached_at`, this writes `cached_at`
    /// verbatim from each row, matching the import flow's semantics.
    pub fn put_base_items(
        &self,
        table_name: &str,
        rows: &[crate::storage_backend::BaseItemRow],
    ) -> Result<()> {
        let escaped = escape_table_name(table_name);
        let sql = format!(
            "INSERT OR REPLACE INTO \"{escaped}\" (pk, sk, item_json, item_size, cached_at, hash_prefix) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        );
        let mut stmt = self.conn.prepare_cached(&sql)?;
        for row in rows {
            stmt.execute(params![
                row.pk,
                row.sk,
                row.item_json,
                row.item_size as i64,
                row.cached_at,
                row.hash_prefix
            ])?;
        }
        Ok(())
    }

    /// Get a single item by primary key.
    pub fn get_item(&self, table_name: &str, pk: &str, sk: &str) -> Result<Option<String>> {
        let (sql, params) = sql_builders::get_item(table_name, pk, sk);
        let result = self
            .conn
            .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                row.get(0)
            });

        match result {
            Ok(json) => Ok(Some(json)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(DynoxideError::from(e)),
        }
    }

    /// Return the total item_size for all items sharing the given partition key.
    pub fn get_partition_size(&self, table_name: &str, pk: &str) -> Result<i64> {
        let (sql, params) = sql_builders::get_partition_size(table_name, pk);
        let size: i64 =
            self.conn
                .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                    row.get(0)
                })?;
        Ok(size)
    }

    /// Return the total size of LSI items for a given partition key.
    /// LSI items are stored as JSON text, so we use length(item_json).
    pub fn get_lsi_partition_size(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
    ) -> Result<i64> {
        let (sql, params) = sql_builders::get_lsi_partition_size(table_name, index_name, pk);
        let size: i64 =
            self.conn
                .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                    row.get(0)
                })?;
        Ok(size)
    }

    /// Delete an item by primary key. Returns the old item_json if it existed.
    pub fn delete_item(&self, table_name: &str, pk: &str, sk: &str) -> Result<Option<String>> {
        let old_item = self.get_item(table_name, pk, sk)?;

        let (sql, params) = sql_builders::delete_item(table_name, pk, sk);
        self.conn
            .execute(&sql, rusqlite::params_from_iter(params.iter()))?;

        Ok(old_item)
    }

    /// Query items by partition key with optional sort key condition.
    ///
    /// `sk_condition` is a SQL fragment like `AND sk > ?` with `sk_params` providing values.
    /// Returns `(items, has_more)` where items is a vec of `(pk, sk, item_json)`.
    pub fn query_items(
        &self,
        table_name: &str,
        pk: &str,
        params: &QueryParams,
    ) -> Result<Vec<(String, String, String)>> {
        let (sql, params_vec) = sql_builders::query_items(table_name, pk, params);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    /// Scan items from a table with pagination.
    ///
    /// Returns `(pk, sk, item_json)` tuples ordered by hash_prefix for
    /// dynalite-compatible parallel scan behaviour.
    pub fn scan_items(
        &self,
        table_name: &str,
        params: &ScanParams,
    ) -> Result<Vec<(String, String, String)>> {
        let (sql, params_vec) = sql_builders::scan_items(table_name, params);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    /// Count items in a table.
    pub fn count_items(&self, table_name: &str) -> Result<i64> {
        let (sql, params) = sql_builders::count_items(table_name);
        let count: i64 =
            self.conn
                .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                    row.get(0)
                })?;
        Ok(count)
    }

    /// Count items in a table and total their stored sizes, in one query.
    pub fn item_count_and_size(&self, table_name: &str) -> Result<(i64, i64)> {
        let (sql, params) = sql_builders::item_count_and_size(table_name);
        let stats: (i64, i64) =
            self.conn
                .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;
        Ok(stats)
    }

    /// Count a GSI's entries and total their projected sizes. See
    /// [`StorageBackend::gsi_item_count_and_size`](crate::storage_backend::StorageBackend::gsi_item_count_and_size).
    pub fn gsi_item_count_and_size(
        &self,
        table_name: &str,
        index_name: &str,
        whole_items: bool,
    ) -> Result<(i64, i64)> {
        if whole_items {
            self.count_and_size(sql_builders::gsi_count_and_size_from_base(
                table_name, index_name,
            ))
        } else {
            self.count_and_size_entries(sql_builders::gsi_entries_item_json(table_name, index_name))
        }
    }

    /// Count an LSI's entries and total their projected sizes. See
    /// [`StorageBackend::lsi_item_count_and_size`](crate::storage_backend::StorageBackend::lsi_item_count_and_size).
    pub fn lsi_item_count_and_size(
        &self,
        table_name: &str,
        index_name: &str,
        whole_items: bool,
    ) -> Result<(i64, i64)> {
        if whole_items {
            self.count_and_size(sql_builders::lsi_count_and_size_from_base(
                table_name, index_name,
            ))
        } else {
            self.count_and_size_entries(sql_builders::lsi_entries_item_json(table_name, index_name))
        }
    }

    /// Count a vector index's entries and total their stored billable sizes.
    /// See [`StorageBackend::vector_item_count_and_size`](crate::storage_backend::StorageBackend::vector_item_count_and_size).
    pub fn vector_item_count_and_size(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<(i64, i64)> {
        self.count_and_size(sql_builders::vector_count_and_size(table_name, index_name))
    }

    /// Rewrite the stored `item_size` of the given rows. See
    /// [`StorageBackend::set_item_sizes`](crate::storage_backend::StorageBackend::set_item_sizes).
    pub fn set_item_sizes(
        &self,
        table_name: &str,
        sizes: &[(String, String, usize)],
    ) -> Result<()> {
        for (pk, sk, size) in sizes {
            #[cfg(test)]
            Self::count_down(&self.item_size_fault, "injected item size write failure")?;
            let (sql, params) = sql_builders::set_item_size(table_name, pk, sk, *size);
            self.conn
                .prepare_cached(&sql)?
                .execute(rusqlite::params_from_iter(params.iter()))?;
        }
        Ok(())
    }

    fn count_and_size(
        &self,
        (sql, params): (String, Vec<sql_builders::SqlParam<'static>>),
    ) -> Result<(i64, i64)> {
        Ok(self
            .conn
            .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?)
    }

    fn count_and_size_entries(
        &self,
        (sql, params): (String, Vec<sql_builders::SqlParam<'static>>),
    ) -> Result<(i64, i64)> {
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params.iter()))?;
        let (mut count, mut bytes) = (0i64, 0i64);
        while let Some(row) = rows.next()? {
            let json: String = row.get(0)?;
            count += 1;
            bytes += crate::storage_backend::projected_entry_size(&json)
                .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
        }
        Ok((count, bytes))
    }

    // -----------------------------------------------------------------------
    // Introspection
    // -----------------------------------------------------------------------

    /// Get the database file path, or `None` for in-memory databases.
    pub fn db_path(&self) -> Option<String> {
        self.conn
            .path()
            .filter(|p| !p.is_empty())
            .map(|p| p.to_owned())
    }

    /// Get the total database size in bytes.
    pub fn db_size_bytes(&self) -> Result<u64> {
        let size: i64 = self.conn.query_row(
            "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()",
            [],
            |row| row.get(0),
        )?;
        Ok(size as u64)
    }

    /// Count the number of DynamoDB tables.
    pub fn table_count(&self) -> Result<usize> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM _tables", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    /// Get per-table statistics: name, item count, and approximate size in bytes.
    ///
    /// Uses a single query per table (COUNT + SUM combined) instead of separate queries.
    pub fn table_stats(&self) -> Result<Vec<TableStats>> {
        let table_names = self.list_table_names()?;
        let mut stats = Vec::with_capacity(table_names.len());
        for name in table_names {
            let sql = format!(
                "SELECT COUNT(*), COALESCE(SUM(item_size), 0) FROM \"{}\"",
                escape_table_name(&name)
            );
            let (item_count, size_bytes): (i64, i64) = self
                .conn
                .query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?)))?;
            stats.push(TableStats {
                table_name: name,
                item_count,
                size_bytes: size_bytes as u64,
            });
        }
        Ok(stats)
    }

    /// Get combined database info in a single call for atomic consistency.
    ///
    /// Returns all introspection data that `get_database_info` tools need
    /// without releasing the lock between queries.
    pub fn database_info(&self) -> Result<DatabaseInfo> {
        let path = self.db_path();
        let size_bytes = self.db_size_bytes()?;
        let table_count = self.table_count()?;
        let stats = self.table_stats()?;

        let mut table_details = Vec::with_capacity(stats.len());
        for s in stats {
            let metadata = self.get_table_metadata(&s.table_name)?;
            table_details.push(TableInfoEntry { stats: s, metadata });
        }

        Ok(DatabaseInfo {
            path,
            size_bytes,
            table_count,
            tables: table_details,
        })
    }

    // -----------------------------------------------------------------------
    // Snapshot operations
    // -----------------------------------------------------------------------

    /// Create a snapshot of the database by copying it to the given path.
    /// Uses `VACUUM INTO` which works for both in-memory and file-backed databases.
    pub fn vacuum_into(&self, path: &str) -> Result<()> {
        if path.contains('\0') {
            return Err(DynoxideError::ValidationException(
                "path contains null byte".to_string(),
            ));
        }
        self.conn
            .execute_batch(&format!("VACUUM INTO '{}'", path.replace('\'', "''")))?;
        Ok(())
    }

    /// Run VACUUM to compact the database file in place.
    pub fn vacuum(&self) -> Result<()> {
        self.conn.execute_batch("VACUUM")?;
        Ok(())
    }

    /// Restore the database from a snapshot file using SQLite's backup API.
    /// This replaces the current database contents with the snapshot contents.
    /// Works for both in-memory and file-backed databases.
    ///
    /// The snapshot is opened read-only, so it is never written to. A snapshot
    /// in WAL mode (every file Dynoxide creates is) may be left with its
    /// `-wal` and `-shm` files beside it afterwards, as reading a WAL database
    /// creates them and a read-only connection can't remove them on close.
    pub fn restore_from(&mut self, path: &str) -> Result<()> {
        // Read-only and without CREATE: a mistyped path is an error, rather
        // than a new, empty file backed up over this database, and the
        // snapshot itself is never written to.
        let source = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_URI
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        self.restore_from_connection(&source)
    }

    /// Backup the current database to a new in-memory SQLite connection.
    ///
    /// Used for in-memory snapshot storage: the returned connection holds
    /// a complete copy of the database without touching the filesystem.
    pub fn backup_to_memory(&self) -> Result<Connection> {
        let mut dest = Connection::open_in_memory()?;
        {
            let backup = rusqlite::backup::Backup::new(&self.conn, &mut dest)?;
            run_backup(&backup)?;
        }
        Ok(dest)
    }

    /// Restore the database from another SQLite connection using the backup API.
    ///
    /// A source with no Dynoxide metadata (an empty or zero-byte file,
    /// `:memory:`, or another program's database) is refused as not a
    /// snapshot. Otherwise the source is copied into a private temporary
    /// database and brought up to date there, as an open would bring a file
    /// an earlier build wrote; only then does the copy replace this database's
    /// contents and the metadata cache get cleared. A copy or migration that
    /// fails leaves this database as it was. SQLite keeps the temporary copy
    /// in its page cache and spills it to a temporary file as it grows, so a
    /// large snapshot doesn't need its own size in memory. Builds with
    /// encryption are the exception: SQLCipher is compiled to keep temporary
    /// databases in memory (`SQLITE_TEMP_STORE=2`), so there the copy does take
    /// its own size in memory, and never reaches the disk unencrypted.
    pub fn restore_from_connection(&mut self, source: &Connection) -> Result<()> {
        ensure_snapshot(source)?;
        refuse_newer_schema(source)?;
        let staged = self.staged_copy(source)?;
        {
            let backup = rusqlite::backup::Backup::new(&staged.conn, &mut self.conn)?;
            run_backup(&backup)?;
        }
        self.metadata_cache.borrow_mut().clear();
        Ok(())
    }

    /// Copy `source` into a fresh private temporary database and run every
    /// migration its recorded schema version calls for, leaving this database
    /// untouched.
    fn staged_copy(&self, source: &Connection) -> Result<Storage> {
        let mut staged = Storage {
            // An empty path opens a private temporary database: on disk once
            // it outgrows the page cache (in memory throughout on encryption
            // builds, whose SQLCipher keeps temporary databases there), and
            // deleted when it is closed.
            conn: Connection::open("")?,
            metadata_cache: RefCell::new(HashMap::new()),
            // A countdown armed on this database fires in the migration that
            // stands in for it.
            #[cfg(test)]
            gsi_insert_fault: RefCell::new(self.gsi_insert_fault.take()),
            #[cfg(test)]
            item_size_fault: RefCell::new(self.item_size_fault.take()),
            clock: Arc::clone(&self.clock),
        };
        let copied = match rusqlite::backup::Backup::new(source, &mut staged.conn) {
            Ok(backup) => run_backup(&backup),
            Err(e) => Err(e.into()),
        };
        let migrated = copied
            .and_then(|()| Self::register_functions(&staged.conn))
            .and_then(|()| staged.migrate_to_current());
        #[cfg(test)]
        {
            *self.gsi_insert_fault.borrow_mut() = staged.gsi_insert_fault.take();
            *self.item_size_fault.borrow_mut() = staged.item_size_fault.take();
        }
        migrated.map(|()| staged)
    }

    /// Get the database size in bytes for an arbitrary connection.
    pub fn connection_size_bytes(conn: &Connection) -> Result<u64> {
        let size: i64 = conn.query_row(
            "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()",
            [],
            |row| row.get(0),
        )?;
        Ok(size as u64)
    }

    // -----------------------------------------------------------------------
    // Stream operations
    // -----------------------------------------------------------------------

    /// Enable streams on a table.
    pub fn enable_stream(&self, table_name: &str, view_type: &str, label: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE _tables SET stream_enabled = 1, stream_view_type = ?1, stream_label = ?2 WHERE table_name = ?3",
            params![view_type, label, table_name],
        )?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Disable streams on a table.
    pub fn disable_stream(&self, table_name: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE _tables SET stream_enabled = 0 WHERE table_name = ?1",
            params![table_name],
        )?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Insert a stream record.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_stream_record(
        &self,
        table_name: &str,
        event_name: &str,
        keys_json: &str,
        new_image: Option<&str>,
        old_image: Option<&str>,
        sequence_number: &str,
        shard_id: &str,
        created_at: i64,
    ) -> Result<()> {
        self.insert_stream_record_with_identity(
            table_name,
            event_name,
            keys_json,
            new_image,
            old_image,
            sequence_number,
            shard_id,
            created_at,
            None,
        )
    }

    /// Insert a stream record with optional user identity (for TTL deletions).
    #[allow(clippy::too_many_arguments)]
    pub fn insert_stream_record_with_identity(
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
    ) -> Result<()> {
        // Globally unique, as DynamoDB's eventID is: 32 lowercase hex digits.
        let event_id = uuid::Uuid::new_v4().simple().to_string();
        self.conn.execute(
            "INSERT INTO _stream_records (table_name, event_name, keys_json, new_image, old_image, sequence_number, shard_id, created_at, user_identity, event_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![table_name, event_name, keys_json, new_image, old_image, sequence_number, shard_id, created_at, user_identity, event_id],
        )?;
        Ok(())
    }

    /// Get the next sequence number for a table's stream: one past the last,
    /// and never below the clock's floor.
    pub fn next_stream_sequence_number(&self, table_name: &str) -> Result<i64> {
        let result: std::result::Result<i64, _> = self.conn.query_row(
            "SELECT COALESCE(MAX(CAST(sequence_number AS INTEGER)), 0) + 1 FROM _stream_records WHERE table_name = ?1",
            params![table_name],
            |row| row.get(0),
        );
        let next = result.unwrap_or(1);
        Ok(next.max(crate::streams::sequence_number_floor(self.clock())))
    }

    /// Get stream records for a shard starting after a given sequence number.
    pub fn get_stream_records(
        &self,
        table_name: &str,
        shard_id: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<StreamRecord>> {
        Ok(self
            .stream_records_with_event_ids(table_name, shard_id, after_sequence, limit)?
            .into_iter()
            .map(|(record, _)| record)
            .collect())
    }

    /// As [`Self::get_stream_records`], with each record's stored event id.
    /// The id is `None` only on a row an older build wrote to the file since
    /// this one opened it.
    pub(crate) fn stream_records_with_event_ids(
        &self,
        table_name: &str,
        shard_id: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<(StreamRecord, Option<String>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT event_name, keys_json, new_image, old_image, sequence_number, created_at, user_identity, event_id
             FROM _stream_records
             WHERE table_name = ?1 AND shard_id = ?2 AND CAST(sequence_number AS INTEGER) > ?3
             ORDER BY CAST(sequence_number AS INTEGER) ASC
             LIMIT ?4",
        )?;
        let rows = stmt
            .query_map(
                params![table_name, shard_id, after_sequence, limit as i64],
                |row| {
                    let record = StreamRecord {
                        event_name: row.get(0)?,
                        keys_json: row.get(1)?,
                        new_image: row.get(2)?,
                        old_image: row.get(3)?,
                        sequence_number: row.get(4)?,
                        created_at: row.get(5)?,
                        user_identity: row.get(6)?,
                    };
                    Ok((record, row.get(7)?))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// List tables that have streams enabled.
    pub fn list_stream_enabled_tables(&self) -> Result<Vec<TableMetadata>> {
        let sql = format!(
            "SELECT {} FROM _tables WHERE stream_enabled = 1 ORDER BY table_name",
            sql_builders::TABLE_METADATA_COLUMNS
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], row_to_metadata)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // TTL operations
    // -----------------------------------------------------------------------

    /// Update TTL configuration for a table.
    pub fn update_ttl_config(
        &self,
        table_name: &str,
        attribute_name: Option<&str>,
        enabled: bool,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE _tables SET ttl_attribute = ?1, ttl_enabled = ?2 WHERE table_name = ?3",
            params![attribute_name, enabled as i32, table_name],
        )?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// List tables that have TTL enabled.
    pub fn list_ttl_enabled_tables(&self) -> Result<Vec<TableMetadata>> {
        let sql = format!(
            "SELECT {} FROM _tables WHERE ttl_enabled = 1 ORDER BY table_name",
            sql_builders::TABLE_METADATA_COLUMNS
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], row_to_metadata)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Get the min and max sequence numbers for a shard.
    pub fn get_shard_sequence_range(
        &self,
        table_name: &str,
        shard_id: &str,
    ) -> Result<(Option<String>, Option<String>)> {
        // Compared as numbers: as text, "10" sorts before "9".
        let result: std::result::Result<(Option<String>, Option<String>), _> = self.conn.query_row(
            "SELECT MIN(CAST(sequence_number AS INTEGER)), MAX(CAST(sequence_number AS INTEGER)) FROM _stream_records WHERE table_name = ?1 AND shard_id = ?2",
            params![table_name, shard_id],
            |row| {
                let min: Option<i64> = row.get(0)?;
                let max: Option<i64> = row.get(1)?;
                Ok((min.map(|n| n.to_string()), max.map(|n| n.to_string())))
            },
        );
        match result {
            Ok(range) => Ok(range),
            Err(_) => Ok((None, None)),
        }
    }

    // -----------------------------------------------------------------------
    // Cache tracking (cached_at)
    // -----------------------------------------------------------------------

    /// Update the `cached_at` timestamp for a single item.
    pub fn touch_cached_at(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        timestamp: f64,
    ) -> Result<()> {
        let sql = format!(
            "UPDATE \"{}\" SET cached_at = ?1 WHERE pk = ?2 AND sk = ?3",
            escape_table_name(table_name)
        );
        self.conn.execute(&sql, params![timestamp, pk, sk])?;
        Ok(())
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
        let sql = format!(
            "SELECT pk, sk, item_size FROM \"{}\" WHERE cached_at IS NOT NULL ORDER BY cached_at ASC LIMIT ?1",
            escape_table_name(table_name)
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(params![limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

/// A stream record from the `_stream_records` table.
#[derive(Debug, Clone)]
pub struct StreamRecord {
    pub event_name: String,
    pub keys_json: String,
    pub new_image: Option<String>,
    pub old_image: Option<String>,
    pub sequence_number: String,
    pub created_at: i64,
    pub user_identity: Option<String>,
}

/// Per-table statistics returned by `Storage::table_stats()`.
#[derive(Debug, Clone)]
pub struct TableStats {
    pub table_name: String,
    pub item_count: i64,
    pub size_bytes: u64,
}

/// Combined database introspection info returned by `Storage::database_info()`.
#[derive(Debug, Clone)]
pub struct DatabaseInfo {
    pub path: Option<String>,
    pub size_bytes: u64,
    pub table_count: usize,
    pub tables: Vec<TableInfoEntry>,
}

/// Per-table stats + metadata for `DatabaseInfo`.
#[derive(Debug, Clone)]
pub struct TableInfoEntry {
    pub stats: TableStats,
    pub metadata: Option<TableMetadata>,
}

/// Metadata row from the `_tables` table.
///
/// Note: The `tags` column is intentionally excluded. Tags are not on the hot
/// path for item operations and are accessed via separate `get_tags`/`set_tags`
/// methods to keep the metadata cache lean.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub table_name: String,
    pub key_schema: String,
    pub attribute_definitions: String,
    pub gsi_definitions: Option<String>,
    pub lsi_definitions: Option<String>,
    pub stream_enabled: bool,
    pub stream_view_type: Option<String>,
    pub stream_label: Option<String>,
    pub ttl_attribute: Option<String>,
    pub ttl_enabled: bool,
    pub created_at: i64,
    pub table_status: String,
    pub billing_mode: Option<String>,
    pub provisioned_throughput: Option<String>,
    pub sse_specification: Option<String>,
    pub table_class: Option<String>,
    pub deletion_protection_enabled: bool,
    pub on_demand_throughput: Option<String>,
    /// Stable per-table identifier assigned at create time (#55). `None` only
    /// for rows that predate the v8 migration's backfill.
    pub table_id: Option<String>,
    /// Vector index definitions as request-shaped JSON, following the
    /// `gsi_definitions` convention. `None` when the table has none.
    pub vector_index_definitions: Option<String>,
}

/// Map a row from the _tables SELECT to a TableMetadata struct.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
fn row_to_metadata(row: &rusqlite::Row) -> rusqlite::Result<TableMetadata> {
    Ok(TableMetadata {
        table_name: row.get(0)?,
        key_schema: row.get(1)?,
        attribute_definitions: row.get(2)?,
        gsi_definitions: row.get(3)?,
        lsi_definitions: row.get(4)?,
        stream_enabled: row.get::<_, i32>(5)? != 0,
        stream_view_type: row.get(6)?,
        stream_label: row.get(7)?,
        ttl_attribute: row.get(8)?,
        ttl_enabled: row.get::<_, i32>(9)? != 0,
        created_at: row.get(10)?,
        table_status: row.get(11)?,
        billing_mode: row.get(12)?,
        provisioned_throughput: row.get(13)?,
        sse_specification: row.get(14)?,
        table_class: row.get(15)?,
        deletion_protection_enabled: row.get::<_, i32>(16).unwrap_or(0) != 0,
        on_demand_throughput: row.get(17)?,
        table_id: row.get(18)?,
        vector_index_definitions: row.get(19)?,
    })
}

/// How long a backup waits between attempts on a database another
/// connection has busy or locked, and after the copy starts over.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
const BACKUP_LOCK_PAUSE: std::time::Duration = std::time::Duration::from_millis(10);

/// How long a backup keeps retrying a busy or locked database before giving
/// up, by the clock: the same patience as an open connection's busy timeout,
/// [`REQUEST_BUSY_TIMEOUT`]. Tests use it unchanged, so a test that waits out
/// a brief lock has a wide margin on a slow machine.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
const BACKUP_LOCK_PATIENCE: std::time::Duration = REQUEST_BUSY_TIMEOUT;

/// How many times a backup starts over, because another connection changed
/// the database being copied, before it gives up.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
const BACKUP_RESTART_LIMIT: u32 = 50;

/// What one backup step reported, as far as [`BackupWatch`] cares.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
#[derive(Clone, Copy, Debug, PartialEq)]
enum BackupStep {
    Done,
    /// Pages copied and more to go; `remaining` is what's left to copy.
    More {
        remaining: i32,
    },
    /// The database was busy or locked.
    Busy,
}

/// What [`run_backup`] does next.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
#[derive(Clone, Copy, Debug, PartialEq)]
enum BackupNext {
    Finish,
    Step,
    Pause,
    GiveUpLocked,
    GiveUpRestarting,
}

/// The bookkeeping behind [`run_backup`]: how long the database has been
/// busy, by the clock, and how often the copy has started over.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
#[derive(Default)]
struct BackupWatch {
    busy_since: Option<std::time::Instant>,
    last_remaining: Option<i32>,
    restarts: u32,
}

#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
impl BackupWatch {
    /// Decide what follows a step that started at `started` and ended at `now`.
    ///
    /// A busy spell is timed from the start of the first step that found the
    /// database busy, so the time SQLite's own busy handler spends inside that
    /// step counts towards the patience. Progress ends the spell. A copy whose
    /// remaining page count doesn't fall has started over, which SQLite does
    /// when another connection changes the database being copied: the pages it
    /// copies again leave the same count remaining as the step before, or more
    /// if the database grew. A restart pauses before the next step, so a short
    /// burst of writes to the source doesn't use the limit up at once.
    fn observe(
        &mut self,
        step: BackupStep,
        started: std::time::Instant,
        now: std::time::Instant,
        patience: std::time::Duration,
    ) -> BackupNext {
        match step {
            BackupStep::Done => BackupNext::Finish,
            BackupStep::More { remaining } => {
                self.busy_since = None;
                let restarted = self.last_remaining.is_some_and(|last| remaining >= last);
                self.last_remaining = Some(remaining);
                if !restarted {
                    return BackupNext::Step;
                }
                self.restarts += 1;
                if self.restarts > BACKUP_RESTART_LIMIT {
                    BackupNext::GiveUpRestarting
                } else {
                    BackupNext::Pause
                }
            }
            BackupStep::Busy => {
                let since = *self.busy_since.get_or_insert(started);
                if now.saturating_duration_since(since) >= patience {
                    BackupNext::GiveUpLocked
                } else {
                    BackupNext::Pause
                }
            }
        }
    }
}

/// Copy every remaining page through `backup`.
///
/// Steps run back to back while they make progress. A busy or locked database
/// is retried every [`BACKUP_LOCK_PAUSE`] until it has been busy for
/// [`BACKUP_LOCK_PATIENCE`] by the clock, then reported as locked. A step can
/// overrun that by a busy-timeout wait of its own inside SQLite, on each
/// database it found locked. A copy that keeps starting over because the
/// database being copied keeps changing waits the same pause after each
/// restart and gives up after [`BACKUP_RESTART_LIMIT`] of them.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
fn run_backup(backup: &rusqlite::backup::Backup<'_, '_>) -> Result<()> {
    let mut watch = BackupWatch::default();
    loop {
        let started = std::time::Instant::now();
        let step = backup_step(backup)?;
        match watch.observe(
            step,
            started,
            std::time::Instant::now(),
            BACKUP_LOCK_PATIENCE,
        ) {
            BackupNext::Finish => return Ok(()),
            BackupNext::Step => {}
            BackupNext::Pause => std::thread::sleep(BACKUP_LOCK_PAUSE),
            BackupNext::GiveUpLocked => {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                    Some("database is locked".to_string()),
                )
                .into());
            }
            BackupNext::GiveUpRestarting => {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                    Some(format!(
                        "the database being copied kept changing; gave up after {BACKUP_RESTART_LIMIT} restarts"
                    )),
                )
                .into());
            }
        }
    }
}

/// Copy the next hundred pages through `backup` and say what happened.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
fn backup_step(backup: &rusqlite::backup::Backup<'_, '_>) -> Result<BackupStep> {
    use rusqlite::backup::StepResult;
    Ok(match backup.step(100)? {
        StepResult::Done => BackupStep::Done,
        StepResult::More => BackupStep::More {
            remaining: backup.progress().remaining,
        },
        // Busy, Locked and any later variant: wait and retry, within limits.
        _ => BackupStep::Busy,
    })
}

/// Refuse a restore source that holds no Dynoxide metadata.
///
/// An empty or zero-byte file, `:memory:` or another program's database
/// opens without complaint, and restoring one would replace this database
/// with an empty one.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
fn ensure_snapshot(source: &Connection) -> Result<()> {
    if table_exists(source, "_tables")? {
        Ok(())
    } else {
        Err(DynoxideError::ValidationException(
            "not a Dynoxide snapshot: it has no table metadata".to_string(),
        ))
    }
}

/// Whether the database at `conn` holds a table called `name`.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(sql_builders::TABLE_EXISTS, [name], |row| row.get(0))?)
}

/// Refuse a database a newer build wrote, reading it and nothing more: a
/// version it records must be one this build knows. An open and a restore
/// check this before they write anything, so a refused file, or a refused
/// snapshot and the database it would have replaced, is left as it was.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
fn refuse_newer_schema(conn: &Connection) -> Result<()> {
    use rusqlite::OptionalExtension;
    if !table_exists(conn, "_config")? {
        return Ok(());
    }
    let value: Option<String> = conn
        .query_row(sql_builders::GET_SCHEMA_VERSION, [], |row| row.get(0))
        .optional()?;
    crate::storage_backend::schema::refuse_newer(crate::storage_backend::schema::parse_recorded(
        value.as_deref(),
    ))
    .map_err(DynoxideError::InternalServerError)
}

#[cfg(all(test, any(feature = "native-sqlite", feature = "_has-encryption")))]
mod tests {
    use super::*;

    fn test_storage() -> Storage {
        Storage::memory().expect("Failed to create in-memory storage")
    }

    #[test]
    fn test_initialize_creates_metadata_tables() {
        let storage = test_storage();
        // _config and _tables should exist
        let version: String = storage
            .conn()
            .query_row(
                "SELECT value FROM _config WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION.to_string());
    }

    #[test]
    fn test_migrate_v6_to_v7_adds_on_demand_throughput_column() {
        // Issue #44: the on_demand_throughput column must be added to existing
        // on-disk databases through the versioned migration chain, not just the
        // fresh CREATE: otherwise DescribeTable on a pre-existing table errors.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        // Build a v6-shape database by hand: _tables without on_demand_throughput,
        // schema_version pinned at 6, and one pre-existing table row.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE _config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 CREATE TABLE _tables (
                    table_name TEXT PRIMARY KEY,
                    key_schema TEXT NOT NULL,
                    attribute_definitions TEXT NOT NULL,
                    gsi_definitions TEXT,
                    lsi_definitions TEXT,
                    stream_enabled INTEGER DEFAULT 0,
                    stream_view_type TEXT,
                    stream_label TEXT,
                    ttl_attribute TEXT,
                    ttl_enabled INTEGER DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    table_status TEXT NOT NULL DEFAULT 'ACTIVE',
                    billing_mode TEXT DEFAULT 'PAY_PER_REQUEST',
                    provisioned_throughput TEXT,
                    tags TEXT,
                    sse_specification TEXT,
                    table_class TEXT,
                    deletion_protection_enabled INTEGER DEFAULT 0
                 );",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO _config (key, value) VALUES ('schema_version', '6')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO _tables (table_name, key_schema, attribute_definitions, created_at) \
                 VALUES ('LegacyTable', ?1, ?2, 0)",
                params![
                    r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#,
                    r#"[{"AttributeName":"pk","AttributeType":"S"}]"#,
                ],
            )
            .unwrap();
        }

        // Reopening runs the migration chain; v6 -> v7 adds the column, and the
        // chain continues to the current SCHEMA_VERSION.
        let storage = Storage::new(&path).unwrap();
        let version: String = storage
            .conn()
            .query_row(
                "SELECT value FROM _config WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION.to_string());

        // The pre-existing row survives and reads back through row_to_metadata,
        // with the new column present and NULL.
        let meta = storage.get_table_metadata("LegacyTable").unwrap().unwrap();
        assert_eq!(meta.table_name, "LegacyTable");
        assert!(meta.on_demand_throughput.is_none());

        // A bare SELECT of the new column would error if the ALTER had not run.
        let col: Option<String> = storage
            .conn()
            .query_row(
                "SELECT on_demand_throughput FROM _tables WHERE table_name = 'LegacyTable'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(col.is_none());
    }

    /// #55: the v7 -> v8 migration adds the `table_id` column and backfills any
    /// pre-existing table with a one-time random id, so a legacy table reports a
    /// stable TableId from then on.
    #[test]
    fn test_migrate_v7_to_v8_backfills_table_id() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        // Build a v7-shape database by hand: _tables with on_demand_throughput
        // but no table_id, schema_version pinned at 7, one pre-existing row.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE _config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 CREATE TABLE _tables (
                    table_name TEXT PRIMARY KEY,
                    key_schema TEXT NOT NULL,
                    attribute_definitions TEXT NOT NULL,
                    gsi_definitions TEXT,
                    lsi_definitions TEXT,
                    stream_enabled INTEGER DEFAULT 0,
                    stream_view_type TEXT,
                    stream_label TEXT,
                    ttl_attribute TEXT,
                    ttl_enabled INTEGER DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    table_status TEXT NOT NULL DEFAULT 'ACTIVE',
                    billing_mode TEXT DEFAULT 'PAY_PER_REQUEST',
                    provisioned_throughput TEXT,
                    tags TEXT,
                    sse_specification TEXT,
                    table_class TEXT,
                    deletion_protection_enabled INTEGER DEFAULT 0,
                    on_demand_throughput TEXT
                 );",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO _config (key, value) VALUES ('schema_version', '7')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO _tables (table_name, key_schema, attribute_definitions, created_at) \
                 VALUES ('LegacyTable', ?1, ?2, 0)",
                params![
                    r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#,
                    r#"[{"AttributeName":"pk","AttributeType":"S"}]"#,
                ],
            )
            .unwrap();
        }

        // Reopening runs the migration chain through v8.
        let storage = Storage::new(&path).unwrap();
        let version: String = storage
            .conn()
            .query_row(
                "SELECT value FROM _config WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION.to_string());

        // The legacy row was backfilled with a non-empty table_id, and it reads
        // back through row_to_metadata.
        let meta = storage.get_table_metadata("LegacyTable").unwrap().unwrap();
        let id = meta.table_id.expect("legacy table should be backfilled");
        assert!(!id.is_empty());

        // Reopening again keeps the same id (stable, not regenerated).
        drop(storage);
        let storage2 = Storage::new(&path).unwrap();
        let meta2 = storage2.get_table_metadata("LegacyTable").unwrap().unwrap();
        assert_eq!(meta2.table_id.as_deref(), Some(id.as_str()));
    }

    /// Write a database at `path` whose stream record table predates the
    /// `event_id` column, holding records at sequence numbers 1 and 2.
    fn write_pre_event_id_database(path: &str) {
        let storage = Storage::new(path).unwrap();
        for seq in ["1", "2"] {
            storage
                .insert_stream_record("T", "INSERT", "{}", None, None, seq, "s", 0)
                .unwrap();
        }
        storage
            .conn()
            .execute_batch("ALTER TABLE _stream_records DROP COLUMN event_id")
            .unwrap();
    }

    fn stored_event_ids(storage: &Storage) -> Vec<Option<String>> {
        storage
            .stream_records_with_event_ids("T", "s", 0, 10)
            .unwrap()
            .into_iter()
            .map(|(_, event_id)| event_id)
            .collect()
    }

    /// Opening a database from before the `event_id` column adds it, and each
    /// old record keeps the id it always reported: its sequence number.
    #[test]
    fn stream_records_without_an_event_id_keep_their_sequence_number_on_open() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        write_pre_event_id_database(&path);

        let expected = vec![Some("1".to_string()), Some("2".to_string())];
        let storage = Storage::new(&path).unwrap();
        assert_eq!(stored_event_ids(&storage), expected);

        // Once the column exists the backfill does not run again: a row an
        // older build writes without an id stays as written.
        storage
            .conn()
            .execute_batch("UPDATE _stream_records SET event_id = NULL WHERE sequence_number = '2'")
            .unwrap();
        drop(storage);
        let storage = Storage::new(&path).unwrap();
        assert_eq!(
            stored_event_ids(&storage),
            vec![Some("1".to_string()), None]
        );
    }

    /// A restored snapshot from before the `event_id` column is backfilled
    /// the same way.
    #[test]
    fn a_restored_snapshot_without_an_event_id_column_is_backfilled() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        write_pre_event_id_database(&path);

        let mut storage = Storage::memory().unwrap();
        storage.restore_from(&path).unwrap();
        assert_eq!(
            stored_event_ids(&storage),
            vec![Some("1".to_string()), Some("2".to_string())]
        );
    }

    /// The v8 -> v9 migration adds the `vector_index_definitions` column, so a
    /// pre-upgrade database keeps working and its rows read back through
    /// row_to_metadata with the new column present and NULL.
    #[test]
    fn test_migrate_v8_to_v9_adds_vector_index_definitions_column() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        // Build a v8-shape database by hand: _tables with table_id but no
        // vector_index_definitions, schema_version pinned at 8, one row.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE _config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 CREATE TABLE _tables (
                    table_name TEXT PRIMARY KEY,
                    key_schema TEXT NOT NULL,
                    attribute_definitions TEXT NOT NULL,
                    gsi_definitions TEXT,
                    lsi_definitions TEXT,
                    stream_enabled INTEGER DEFAULT 0,
                    stream_view_type TEXT,
                    stream_label TEXT,
                    ttl_attribute TEXT,
                    ttl_enabled INTEGER DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    table_status TEXT NOT NULL DEFAULT 'ACTIVE',
                    billing_mode TEXT DEFAULT 'PAY_PER_REQUEST',
                    provisioned_throughput TEXT,
                    tags TEXT,
                    sse_specification TEXT,
                    table_class TEXT,
                    deletion_protection_enabled INTEGER DEFAULT 0,
                    on_demand_throughput TEXT,
                    table_id TEXT
                 );",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO _config (key, value) VALUES ('schema_version', '8')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO _tables (table_name, key_schema, attribute_definitions, created_at, table_id) \
                 VALUES ('LegacyTable', ?1, ?2, 0, 'fixed-id')",
                params![
                    r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#,
                    r#"[{"AttributeName":"pk","AttributeType":"S"}]"#,
                ],
            )
            .unwrap();
        }

        // Reopening runs the migration chain; v8 -> v9 adds the column.
        let storage = Storage::new(&path).unwrap();
        let version: String = storage
            .conn()
            .query_row(
                "SELECT value FROM _config WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION.to_string());

        // The pre-existing row survives and reads back with the new column
        // present and NULL.
        let meta = storage.get_table_metadata("LegacyTable").unwrap().unwrap();
        assert_eq!(meta.table_name, "LegacyTable");
        assert!(meta.vector_index_definitions.is_none());

        // A bare SELECT of the new column would error if the ALTER had not run.
        let col: Option<String> = storage
            .conn()
            .query_row(
                "SELECT vector_index_definitions FROM _tables WHERE table_name = 'LegacyTable'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(col.is_none());
    }

    #[test]
    fn test_migrate_v9_to_v10_rebuilds_a_multi_attribute_gsi() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        // A table with a one-plus-two-attribute GSI key, written by this build.
        {
            let db = crate::Database::new(&path).unwrap();
            db.create_table(
                serde_json::from_value(serde_json::json!({
                    "TableName": "Legacy",
                    "BillingMode": "PAY_PER_REQUEST",
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "h", "AttributeType": "S"},
                        {"AttributeName": "a", "AttributeType": "S"},
                        {"AttributeName": "b", "AttributeType": "S"}
                    ],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "GlobalSecondaryIndexes": [{
                        "IndexName": "byKey",
                        "KeySchema": [
                            {"AttributeName": "h", "KeyType": "HASH"},
                            {"AttributeName": "a", "KeyType": "RANGE"},
                            {"AttributeName": "b", "KeyType": "RANGE"}
                        ],
                        "Projection": {"ProjectionType": "ALL"}
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            for item in [
                serde_json::json!({"pk": {"S": "1"}, "h": {"S": "x"}, "a": {"S": "m"}, "b": {"S": "2"}}),
                serde_json::json!({"pk": {"S": "2"}, "h": {"S": "x"}, "a": {"S": "m"}, "b": {"S": "1"}}),
                serde_json::json!({"pk": {"S": "3"}, "h": {"S": "x"}, "a": {"S": "m"}}),
            ] {
                db.put_item(
                    serde_json::from_value(
                        serde_json::json!({"TableName": "Legacy", "Item": item}),
                    )
                    .unwrap(),
                )
                .unwrap();
            }
        }

        // Put the index back the way a v9 build left it: keyed on the first
        // sort key attribute alone, and holding the item that lacks `b`.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "DELETE FROM \"Legacy::gsi::byKey\";
                 INSERT INTO \"Legacy::gsi::byKey\" (gsi_pk, gsi_sk, table_pk, table_sk, item_json)
                    SELECT 'S:x', 'S:m', pk, sk, item_json FROM \"Legacy\";
                 UPDATE _config SET value = '9' WHERE key = 'schema_version';",
            )
            .unwrap();
        }

        let storage = Storage::new(&path).unwrap();
        let version: String = storage
            .conn()
            .query_row(
                "SELECT value FROM _config WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION.to_string());
        let rows: Vec<(String, String)> = {
            let mut stmt = storage
                .conn()
                .prepare(
                    "SELECT table_pk, gsi_sk FROM \"Legacy::gsi::byKey\" ORDER BY gsi_sk, table_pk",
                )
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let expected_key = |b: &str| {
            crate::actions::gsi::composite_key_string(&[
                &AttributeValue::S("m".into()),
                &AttributeValue::S(b.into()),
            ])
            .unwrap()
        };
        assert_eq!(
            rows,
            vec![
                ("S:2".to_string(), expected_key("1")),
                ("S:1".to_string(), expected_key("2")),
            ]
        );
    }

    /// A path for a test database inside a fresh temporary directory, so the
    /// `-wal` and `-shm` files a WAL database (or a read-only restore from
    /// one) leaves beside it are removed with the directory.
    fn v9_snapshot_path() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db").to_str().unwrap().to_string();
        (dir, path)
    }

    /// Write a database the way a v9 build left it at `path`. Table `Legacy`
    /// has three GSIs:
    ///
    /// - `byKey`, one HASH and two RANGE attributes, keyed on the first of
    ///   each kind and holding the item that lacks its second RANGE attribute;
    /// - `byTwoHash`, two HASH and one RANGE attribute, keyed on the first
    ///   HASH attribute alone;
    /// - `bySingle`, one attribute of each kind, stored as every version
    ///   stores it.
    ///
    /// Three named items come first, then `extra` more carrying every key
    /// attribute. Table `Vec` has a vector index and one item whose stored size
    /// is deliberately stale, the billed size plus an arbitrary amount, so the
    /// size recompute has a row to rewrite.
    fn write_v9_database(path: &str, extra: usize) {
        {
            let db = crate::Database::new(path).unwrap();
            db.create_table(
                serde_json::from_value(serde_json::json!({
                    "TableName": "Legacy",
                    "BillingMode": "PAY_PER_REQUEST",
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "h", "AttributeType": "S"},
                        {"AttributeName": "h2", "AttributeType": "S"},
                        {"AttributeName": "a", "AttributeType": "S"},
                        {"AttributeName": "b", "AttributeType": "S"}
                    ],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "GlobalSecondaryIndexes": [
                        {
                            "IndexName": "byKey",
                            "KeySchema": [
                                {"AttributeName": "h", "KeyType": "HASH"},
                                {"AttributeName": "a", "KeyType": "RANGE"},
                                {"AttributeName": "b", "KeyType": "RANGE"}
                            ],
                            "Projection": {"ProjectionType": "ALL"}
                        },
                        {
                            "IndexName": "byTwoHash",
                            "KeySchema": [
                                {"AttributeName": "h", "KeyType": "HASH"},
                                {"AttributeName": "h2", "KeyType": "HASH"},
                                {"AttributeName": "a", "KeyType": "RANGE"}
                            ],
                            "Projection": {"ProjectionType": "ALL"}
                        },
                        {
                            "IndexName": "bySingle",
                            "KeySchema": [
                                {"AttributeName": "h", "KeyType": "HASH"},
                                {"AttributeName": "a", "KeyType": "RANGE"}
                            ],
                            "Projection": {"ProjectionType": "KEYS_ONLY"}
                        }
                    ]
                }))
                .unwrap(),
            )
            .unwrap();
            let named = [
                serde_json::json!({"pk": {"S": "1"}, "h": {"S": "x"}, "h2": {"S": "y"}, "a": {"S": "m"}, "b": {"S": "2"}}),
                serde_json::json!({"pk": {"S": "2"}, "h": {"S": "x"}, "h2": {"S": "y"}, "a": {"S": "m"}, "b": {"S": "1"}}),
                serde_json::json!({"pk": {"S": "3"}, "h": {"S": "x"}, "h2": {"S": "y"}, "a": {"S": "m"}}),
            ];
            let extras = (0..extra).map(|i| {
                serde_json::json!({
                    "pk": {"S": format!("e{i:05}")},
                    "h": {"S": "x"}, "h2": {"S": "y"}, "a": {"S": "m"},
                    "b": {"S": format!("{i:05}")}
                })
            });
            for item in named.into_iter().chain(extras) {
                db.put_item(
                    serde_json::from_value(
                        serde_json::json!({"TableName": "Legacy", "Item": item}),
                    )
                    .unwrap(),
                )
                .unwrap();
            }
            db.create_table(
                serde_json::from_value(serde_json::json!({
                    "TableName": "Vec",
                    "BillingMode": "PAY_PER_REQUEST",
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "VectorIndexes": [{
                        "IndexName": "vix",
                        "VectorAttribute": {"AttributeName": "e"},
                        "Dimensions": 3,
                        "DistanceFunction": "COSINE",
                        "Projection": {"ProjectionType": "KEYS_ONLY"}
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            db.put_item(
                serde_json::from_value(serde_json::json!({
                    "TableName": "Vec",
                    "Item": {
                        "pk": {"S": "v"},
                        "e": {"L": [{"N": "0.125"}, {"N": "0.125"}, {"N": "0.125"}]}
                    }
                }))
                .unwrap(),
            )
            .unwrap();
        }
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "UPDATE \"Vec\" SET item_size = item_size + 7;
             DELETE FROM \"Legacy::gsi::byKey\";
             INSERT INTO \"Legacy::gsi::byKey\" (gsi_pk, gsi_sk, table_pk, table_sk, item_json)
                SELECT 'S:x', 'S:m', pk, sk, item_json FROM \"Legacy\";
             DELETE FROM \"Legacy::gsi::byTwoHash\";
             INSERT INTO \"Legacy::gsi::byTwoHash\" (gsi_pk, gsi_sk, table_pk, table_sk, item_json)
                SELECT 'S:x', 'S:m', pk, sk, item_json FROM \"Legacy\";
             UPDATE _config SET value = '9' WHERE key = 'schema_version';",
        )
        .unwrap();
    }

    /// Every column of every row of a GSI table, in base key order.
    fn gsi_rows(conn: &Connection, index: &str) -> Vec<[String; 5]> {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT gsi_pk, gsi_sk, table_pk, table_sk, item_json \
                 FROM \"Legacy::gsi::{index}\" ORDER BY table_pk, table_sk"
            ))
            .unwrap();
        stmt.query_map([], |r| {
            Ok([r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?])
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    }

    fn recorded_version(conn: &Connection) -> String {
        conn.query_row(
            "SELECT value FROM _config WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Open `path` as [`Storage::new`] does, with the GSI insert and item
    /// size countdowns armed before the open's migrations run.
    fn open_with_faults(
        path: &str,
        gsi_insert: Option<u32>,
        item_size: Option<u32>,
    ) -> Result<Storage> {
        let mut storage = Storage {
            conn: Connection::open(path)?,
            metadata_cache: RefCell::new(HashMap::new()),
            gsi_insert_fault: RefCell::new(gsi_insert),
            item_size_fault: RefCell::new(item_size),
            clock: Arc::new(SystemClock),
        };
        storage.initialize()?;
        Ok(storage)
    }

    #[test]
    fn an_interrupted_gsi_rebuild_leaves_the_database_as_it_was() {
        let (_dir, path) = v9_snapshot_path();
        write_v9_database(&path, 0);
        let before = {
            let conn = Connection::open(&path).unwrap();
            (gsi_rows(&conn, "byKey"), gsi_rows(&conn, "byTwoHash"))
        };

        // The first index insert of the rebuild fails, after the rebuild has
        // already dropped and recreated the index it was filling.
        assert!(open_with_faults(&path, Some(0), None).is_err());

        let conn = Connection::open(&path).unwrap();
        assert_eq!(recorded_version(&conn), "9");
        assert_eq!(
            (gsi_rows(&conn, "byKey"), gsi_rows(&conn, "byTwoHash")),
            before,
            "an interrupted rebuild must leave every index as it found it"
        );
        drop(conn);

        // The next open runs the rebuild from the start.
        let storage = Storage::new(&path).unwrap();
        assert_eq!(recorded_version(storage.conn()), SCHEMA_VERSION.to_string());
    }

    #[test]
    fn an_interrupted_size_recompute_leaves_the_database_as_it_was() {
        let (_dir, path) = v9_snapshot_path();
        {
            let db = crate::Database::new(&path).unwrap();
            db.create_table(
                serde_json::from_value(serde_json::json!({
                    "TableName": "Vec",
                    "BillingMode": "PAY_PER_REQUEST",
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "VectorIndexes": [{
                        "IndexName": "vix",
                        "VectorAttribute": {"AttributeName": "e"},
                        "Dimensions": 3,
                        "DistanceFunction": "COSINE",
                        "Projection": {"ProjectionType": "KEYS_ONLY"}
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            for pk in ["a", "b", "c"] {
                db.put_item(
                    serde_json::from_value(serde_json::json!({
                        "TableName": "Vec",
                        "Item": {
                            "pk": {"S": pk},
                            "e": {"L": [{"N": "0.125"}, {"N": "0.125"}, {"N": "0.125"}]}
                        }
                    }))
                    .unwrap(),
                )
                .unwrap();
            }
        }
        // Back to how a v10 build stored them: a different size per row.
        let sizes = |conn: &Connection| -> Vec<(String, i64)> {
            let mut stmt = conn
                .prepare("SELECT pk, item_size FROM \"Vec\" ORDER BY pk")
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let before = {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "UPDATE \"Vec\" SET item_size = item_size + 7;
                 UPDATE _config SET value = '10' WHERE key = 'schema_version';",
            )
            .unwrap();
            sizes(&conn)
        };

        // One row is rewritten, then the second rewrite fails.
        assert!(open_with_faults(&path, None, Some(1)).is_err());

        let conn = Connection::open(&path).unwrap();
        assert_eq!(recorded_version(&conn), "10");
        assert_eq!(
            sizes(&conn),
            before,
            "an interrupted recompute must leave every stored size as it found it"
        );
    }

    fn vec_size(conn: &Connection) -> i64 {
        conn.query_row("SELECT item_size FROM \"Vec\" WHERE pk = 'S:v'", [], |r| {
            r.get(0)
        })
        .unwrap()
    }

    #[test]
    fn a_second_open_runs_no_migration() {
        let (_dir, path) = v9_snapshot_path();
        write_v9_database(&path, 0);
        drop(Storage::new(&path).unwrap());
        // The fixture gives each migration a row to write, and either would
        // fail at its first write, so an open that ran one fails.
        let storage = open_with_faults(&path, Some(0), Some(0)).unwrap();
        assert_eq!(recorded_version(storage.conn()), SCHEMA_VERSION.to_string());
    }

    #[test]
    fn a_recompute_interrupted_after_the_rebuild_leaves_version_10() {
        let (_dir, path) = v9_snapshot_path();
        write_v9_database(&path, 0);
        let stale_size = vec_size(&Connection::open(&path).unwrap());

        // The GSI rebuild runs and commits; the size recompute then fails at
        // its first row.
        assert!(open_with_faults(&path, None, Some(0)).is_err());

        let conn = Connection::open(&path).unwrap();
        assert_eq!(
            recorded_version(&conn),
            "10",
            "the rebuild's step is kept and the recompute's is not"
        );
        assert_eq!(vec_size(&conn), stale_size, "the recompute is rolled back");
        let composite = crate::actions::gsi::composite_key_string(&[
            &AttributeValue::S("m".into()),
            &AttributeValue::S("2".into()),
        ])
        .unwrap();
        assert!(
            gsi_rows(&conn, "byKey")
                .iter()
                .any(|[_, gsi_sk, _, _, _]| *gsi_sk == composite),
            "the rebuilt index is kept"
        );
        drop(conn);

        // The next open runs only the recompute.
        let storage = open_with_faults(&path, Some(0), None).unwrap();
        assert_eq!(recorded_version(storage.conn()), SCHEMA_VERSION.to_string());
        assert_eq!(vec_size(storage.conn()), stale_size - 7);
    }

    #[test]
    fn the_rebuild_keeps_single_attribute_indexes_and_keys_every_hash_attribute() {
        let (_dir, path) = v9_snapshot_path();
        write_v9_database(&path, 0);
        let single_before = gsi_rows(&Connection::open(&path).unwrap(), "bySingle");

        let storage = Storage::new(&path).unwrap();
        assert_eq!(
            gsi_rows(storage.conn(), "bySingle"),
            single_before,
            "a single-attribute index is stored the same way and is left alone"
        );
        let two_hash = crate::actions::gsi::composite_key_string(&[
            &AttributeValue::S("x".into()),
            &AttributeValue::S("y".into()),
        ])
        .unwrap();
        let keys: Vec<(String, String)> = gsi_rows(storage.conn(), "byTwoHash")
            .into_iter()
            .map(|[gsi_pk, _, table_pk, _, _]| (gsi_pk, table_pk))
            .collect();
        assert_eq!(
            keys,
            ["S:1", "S:2", "S:3"]
                .map(|pk| (two_hash.clone(), pk.to_string()))
                .to_vec()
        );
    }

    #[test]
    fn the_rebuild_runs_past_the_first_backfill_batch() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        // The backfill reads the base table 1,000 rows at a time.
        write_v9_database(&path, 1_005);

        let storage = Storage::new(&path).unwrap();
        // Every item but the one lacking `b` is in `byKey`; every item is in
        // `byTwoHash`.
        assert_eq!(gsi_rows(storage.conn(), "byKey").len(), 2 + 1_005);
        assert_eq!(gsi_rows(storage.conn(), "byTwoHash").len(), 3 + 1_005);
    }

    #[test]
    fn a_restored_snapshot_from_an_earlier_version_is_brought_up_to_date() {
        let (_dir, path) = v9_snapshot_path();
        write_v9_database(&path, 0);

        let mut storage = Storage::memory().unwrap();
        storage.restore_from(&path).unwrap();

        assert_eq!(recorded_version(storage.conn()), SCHEMA_VERSION.to_string());
        let sort_keys: Vec<String> = gsi_rows(storage.conn(), "byKey")
            .into_iter()
            .map(|[_, gsi_sk, _, _, _]| gsi_sk)
            .collect();
        let composite = |b: &str| {
            crate::actions::gsi::composite_key_string(&[
                &AttributeValue::S("m".into()),
                &AttributeValue::S(b.into()),
            ])
            .unwrap()
        };
        assert_eq!(sort_keys, vec![composite("2"), composite("1")]);
    }

    /// A table of this database's own, so a restore that should have left the
    /// database alone can be seen to have done so.
    fn mark_live(storage: &Storage) {
        storage
            .conn()
            .execute_batch(
                "CREATE TABLE live_marker (v TEXT); INSERT INTO live_marker VALUES ('live');",
            )
            .unwrap();
    }

    fn still_live(storage: &Storage) -> bool {
        storage
            .conn()
            .query_row("SELECT v FROM live_marker", [], |r| r.get::<_, String>(0))
            .map(|v| v == "live")
            .unwrap_or(false)
    }

    #[test]
    fn restoring_from_a_missing_file_errors_and_leaves_the_database_alone() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such-snapshot.db");
        let missing = missing.to_str().unwrap();
        let mut storage = Storage::memory().unwrap();
        mark_live(&storage);

        assert!(storage.restore_from(missing).is_err());
        assert!(
            !std::path::Path::new(missing).exists(),
            "a mistyped path must not be created"
        );
        assert!(still_live(&storage), "the database must be left as it was");
    }

    #[test]
    fn a_restore_whose_migration_fails_leaves_the_database_alone() {
        let (_dir, path) = v9_snapshot_path();
        write_v9_database(&path, 0);

        let mut storage = Storage::memory().unwrap();
        mark_live(&storage);
        // The restored snapshot's GSI rebuild fails at its first insert.
        storage.fail_gsi_insert_after(0);
        let err = storage.restore_from(&path).unwrap_err().to_string();
        assert!(
            err.contains("injected GSI insert failure"),
            "the restore must fail in the migration, not elsewhere: {err}"
        );

        assert!(still_live(&storage), "the database must be left as it was");
        let legacy: i64 = storage
            .conn()
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'Legacy'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(legacy, 0, "none of the snapshot may be left behind");
        assert_eq!(recorded_version(storage.conn()), SCHEMA_VERSION.to_string());
    }

    #[test]
    fn restoring_from_something_that_is_not_a_snapshot_errors_and_leaves_the_database_alone() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("zero-bytes.db");
        std::fs::write(&empty, b"").unwrap();
        let foreign = dir.path().join("foreign.db");
        Connection::open(&foreign)
            .unwrap()
            .execute_batch("CREATE TABLE notes (body TEXT); INSERT INTO notes VALUES ('x');")
            .unwrap();

        for source in [
            empty.to_str().unwrap(),
            foreign.to_str().unwrap(),
            ":memory:",
        ] {
            let mut storage = Storage::memory().unwrap();
            mark_live(&storage);
            let err = storage.restore_from(source).unwrap_err().to_string();
            assert!(
                err.contains("not a Dynoxide snapshot"),
                "{source}: refused for the wrong reason: {err}"
            );
            assert!(
                still_live(&storage),
                "{source}: the database must be left as it was"
            );
        }
    }

    #[test]
    fn a_backup_gives_up_on_a_source_that_stays_locked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("locked.db");
        let path = path.to_str().unwrap().to_string();
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode = DELETE; CREATE TABLE t (v); INSERT INTO t VALUES (1);",
            )
            .unwrap();
        }
        // Another connection holds the file exclusively for the whole copy.
        let holder = Connection::open(&path).unwrap();
        holder
            .execute_batch("BEGIN EXCLUSIVE; INSERT INTO t VALUES (2);")
            .unwrap();

        // The copier keeps the connection's default busy timeout, so a busy
        // step waits inside SQLite too, and the patience counts that time.
        let (done, outcome) = std::sync::mpsc::channel();
        let copier_path = path.clone();
        std::thread::spawn(move || {
            let source = Connection::open(&copier_path).unwrap();
            let mut dest = Connection::open_in_memory().unwrap();
            let result = rusqlite::backup::Backup::new(&source, &mut dest)
                .map_err(DynoxideError::from)
                .and_then(|backup| run_backup(&backup));
            let _ = done.send(result.map_err(|e| e.to_string()));
        });

        // The first busy step waits out SQLite's busy timeout (5 s), which
        // uses up the whole patience, so the copy gives up after about five
        // seconds.
        let result = outcome
            .recv_timeout(std::time::Duration::from_secs(15))
            .expect("a backup of a locked source must give up within its patience");
        let err = result.expect_err("the source stayed locked, so the copy must fail");
        assert!(err.contains("locked") || err.contains("busy"), "{err}");
        drop(holder);
    }

    /// A source locked briefly by another connection, released after `hold`.
    fn briefly_locked_source(hold: std::time::Duration) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("briefly-locked.db");
        let path = path.to_str().unwrap().to_string();
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode = DELETE; CREATE TABLE t (v); INSERT INTO t VALUES (1);",
            )
            .unwrap();
        }
        let (locked, is_locked) = std::sync::mpsc::channel();
        let holder_path = path.clone();
        std::thread::spawn(move || {
            let holder = Connection::open(&holder_path).unwrap();
            holder
                .execute_batch("BEGIN EXCLUSIVE; INSERT INTO t VALUES (2);")
                .unwrap();
            let _ = locked.send(());
            std::thread::sleep(hold);
            holder.execute_batch("COMMIT").unwrap();
        });
        is_locked.recv().unwrap();
        (dir, path)
    }

    fn copy_rows(source: &Connection) -> Result<i64> {
        let mut dest = Connection::open_in_memory().unwrap();
        {
            let backup = rusqlite::backup::Backup::new(source, &mut dest)?;
            run_backup(&backup)?;
        }
        Ok(dest.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))?)
    }

    #[test]
    fn a_backup_waits_out_a_short_lock() {
        let (_dir, path) = briefly_locked_source(std::time::Duration::from_millis(50));
        // SQLite's own busy wait covers a short lock.
        let source = Connection::open(&path).unwrap();
        assert_eq!(
            copy_rows(&source).unwrap(),
            2,
            "the copy sees the committed row"
        );
    }

    #[test]
    fn a_backup_retries_a_lock_that_clears() {
        let (_dir, path) = briefly_locked_source(std::time::Duration::from_millis(50));
        // With no busy wait inside SQLite, the retry loop is what waits.
        let source = Connection::open(&path).unwrap();
        source.busy_timeout(std::time::Duration::ZERO).unwrap();
        assert_eq!(
            copy_rows(&source).unwrap(),
            2,
            "the copy sees the committed row"
        );
    }

    #[test]
    fn a_backup_watch_times_a_busy_spell_from_the_step_that_found_it() {
        let patience = std::time::Duration::from_secs(5);
        let t0 = std::time::Instant::now();
        let at = |ms: u64| t0 + std::time::Duration::from_millis(ms);
        let mut watch = BackupWatch::default();
        // The first busy step spent 4.9 s waiting inside SQLite: still patient.
        assert_eq!(
            watch.observe(BackupStep::Busy, at(0), at(4_900), patience),
            BackupNext::Pause
        );
        // The next busy step ends past five seconds from the first one's start.
        assert_eq!(
            watch.observe(BackupStep::Busy, at(4_910), at(5_000), patience),
            BackupNext::GiveUpLocked
        );
        // Progress ends a spell, so the next busy spell is timed afresh.
        let mut watch = BackupWatch::default();
        assert_eq!(
            watch.observe(BackupStep::Busy, at(0), at(4_000), patience),
            BackupNext::Pause
        );
        assert_eq!(
            watch.observe(
                BackupStep::More { remaining: 10 },
                at(4_010),
                at(4_020),
                patience
            ),
            BackupNext::Step
        );
        assert_eq!(
            watch.observe(BackupStep::Busy, at(4_030), at(8_000), patience),
            BackupNext::Pause
        );
        assert_eq!(
            watch.observe(BackupStep::Done, at(8_010), at(8_020), patience),
            BackupNext::Finish
        );
    }

    #[test]
    fn a_backup_watch_gives_up_on_a_copy_that_keeps_starting_over() {
        let patience = std::time::Duration::from_secs(5);
        let t = std::time::Instant::now();
        let mut watch = BackupWatch::default();
        assert_eq!(
            watch.observe(BackupStep::More { remaining: 100 }, t, t, patience),
            BackupNext::Step
        );
        // Steady progress isn't a restart.
        assert_eq!(
            watch.observe(BackupStep::More { remaining: 50 }, t, t, patience),
            BackupNext::Step
        );
        // Each jump back up is a restart; the limit's worth are tolerated,
        // each followed by a pause.
        for _ in 0..BACKUP_RESTART_LIMIT {
            assert_eq!(
                watch.observe(BackupStep::More { remaining: 100 }, t, t, patience),
                BackupNext::Pause
            );
            assert_eq!(
                watch.observe(BackupStep::More { remaining: 50 }, t, t, patience),
                BackupNext::Step
            );
        }
        assert_eq!(
            watch.observe(BackupStep::More { remaining: 100 }, t, t, patience),
            BackupNext::GiveUpRestarting
        );
    }

    #[test]
    fn a_backup_watch_counts_a_copy_that_starts_over_at_the_same_place() {
        // A source changed between every step starts over each time, and the
        // first hundred pages copied again leave the same count remaining as
        // the step before: SQLite reports 302, 302, 302... for a 402-page
        // source, so a restart is a count that doesn't fall.
        let patience = std::time::Duration::from_secs(5);
        let t = std::time::Instant::now();
        let mut watch = BackupWatch::default();
        assert_eq!(
            watch.observe(BackupStep::More { remaining: 302 }, t, t, patience),
            BackupNext::Step
        );
        // Each restart within the limit pauses before the next step, so a
        // short burst of writes to the source doesn't use the limit up at once.
        for restart in 1..=BACKUP_RESTART_LIMIT {
            assert_eq!(
                watch.observe(BackupStep::More { remaining: 302 }, t, t, patience),
                BackupNext::Pause,
                "restart {restart}"
            );
        }
        assert_eq!(
            watch.observe(BackupStep::More { remaining: 302 }, t, t, patience),
            BackupNext::GiveUpRestarting
        );
    }

    #[test]
    fn a_backup_of_a_source_changed_between_steps_gives_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("changing.db");
        let path = path.to_str().unwrap().to_string();
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode = DELETE; CREATE TABLE t (v); \
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 400) \
                 INSERT INTO t SELECT zeroblob(3000) FROM n;",
            )
            .unwrap();
        let source = Connection::open(&path).unwrap();
        let mut dest = Connection::open_in_memory().unwrap();
        let backup = rusqlite::backup::Backup::new(&source, &mut dest).unwrap();
        let patience = std::time::Duration::from_secs(5);
        let mut watch = BackupWatch::default();
        let mut outcome = None;
        // Another connection changes the source between every pair of steps,
        // which is what makes SQLite start the copy over. The new value must
        // differ from the old: SQLite leaves a page alone when a write would
        // put back the bytes already there, and the copy then carries on.
        for step in 0..(2 * BACKUP_RESTART_LIMIT + 10) {
            let t = std::time::Instant::now();
            match watch.observe(backup_step(&backup).unwrap(), t, t, patience) {
                BackupNext::Step | BackupNext::Pause => {}
                other => {
                    outcome = Some((step, other));
                    break;
                }
            }
            writer
                .execute("UPDATE t SET v = randomblob(3000) WHERE rowid = 1", [])
                .unwrap();
        }
        let (step, next) = outcome.expect("a copy that keeps starting over must give up");
        assert_eq!(next, BackupNext::GiveUpRestarting);
        assert!(
            step <= BACKUP_RESTART_LIMIT + 1,
            "gave up after {step} steps, not at the restart limit"
        );
    }

    #[test]
    fn a_restore_leaves_its_source_file_unchanged() {
        let (_dir, path) = v9_snapshot_path();
        write_v9_database(&path, 0);
        let before = std::fs::read(&path).unwrap();

        let mut storage = Storage::memory().unwrap();
        storage.restore_from(&path).unwrap();
        drop(storage);

        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(recorded_version(&Connection::open(&path).unwrap()), "9");
    }

    fn busy_timeout_ms(conn: &Connection) -> i64 {
        conn.query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap()
    }

    /// The busy timeout the last open on this thread ran its migrations
    /// under, and the one the connection it handed out has.
    fn open_and_after(storage: &Storage) -> (Option<i64>, i64) {
        (
            OPENED_WITH_BUSY_TIMEOUT_MS.take(),
            busy_timeout_ms(storage.conn()),
        )
    }

    /// A database file waits 30 seconds for a lock another process holds
    /// while it opens and brings the file up to date, and five once it is
    /// open, so a request held up by another process gives up after five
    /// seconds rather than holding every other request up behind it.
    #[test]
    fn a_database_file_waits_thirty_seconds_for_a_lock_while_opening_and_five_after() {
        let (_dir, path) = v9_snapshot_path();
        let storage = Storage::new(&path).unwrap();
        assert_eq!(open_and_after(&storage), (Some(30_000), 5_000));
        // An in-memory database has no other process to wait for, and keeps
        // the connection's default throughout.
        let memory = Storage::memory().unwrap();
        assert_eq!(open_and_after(&memory), (Some(5_000), 5_000));
    }

    #[cfg(feature = "_has-encryption")]
    #[test]
    fn an_encrypted_database_file_waits_thirty_seconds_for_a_lock_while_opening_and_five_after() {
        const KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let (_dir, path) = v9_snapshot_path();
        let storage = Storage::new_encrypted(&path, KEY).unwrap();
        assert_eq!(open_and_after(&storage), (Some(30_000), 5_000));
    }

    /// A second process opening a file while the first holds its write lock
    /// part way through a migration waits for the lock, then finds the
    /// migration done and leaves it alone. The pause before the first commits
    /// only makes it likely the second is already waiting; the outcome is the
    /// same either way, and how long the second will wait is the test above.
    #[test]
    fn a_second_open_waits_for_a_migration_another_holds_then_finds_it_done() {
        let (_dir, path) = v9_snapshot_path();
        write_v9_database(&path, 0);
        let stale_index = gsi_rows(&Connection::open(&path).unwrap(), "byKey");

        // The first open, part way through: it holds the write lock and has
        // finished the work and recorded the version, but not yet committed.
        // Its work leaves a size no recompute would give, so a second
        // recompute would show.
        let first = Connection::open(&path).unwrap();
        first.execute_batch("BEGIN IMMEDIATE").unwrap();
        first
            .execute_batch(&format!(
                "UPDATE \"Vec\" SET item_size = 1;
                 UPDATE _config SET value = '{SCHEMA_VERSION}' WHERE key = 'schema_version';"
            ))
            .unwrap();

        let (started, starting) = std::sync::mpsc::channel();
        let second_path = path.clone();
        let second = std::thread::spawn(move || {
            started.send(()).unwrap();
            let storage = Storage::new(&second_path)?;
            Ok::<_, DynoxideError>((
                recorded_version(storage.conn()),
                vec_size(storage.conn()),
                gsi_rows(storage.conn(), "byKey"),
            ))
        });
        starting.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.execute_batch("COMMIT").unwrap();

        let (version, size, index) = second.join().unwrap().unwrap();
        assert_eq!(version, SCHEMA_VERSION.to_string());
        assert_eq!(
            size, 1,
            "the second open must not recompute the sizes again"
        );
        assert_eq!(
            index, stale_index,
            "the second open must not rebuild the index the first left"
        );
    }

    /// A database that records no version, as the browser build left one
    /// from Dynoxide 0.10 to 0.13, is taken through every step on open rather
    /// than taken to be current. It already has every column and index the
    /// earlier steps add, so this also shows each of them leaves those alone.
    #[test]
    fn a_database_that_records_no_version_is_brought_up_to_date() {
        let (_dir, path) = v9_snapshot_path();
        write_v9_database(&path, 0);
        let table_id = |conn: &Connection| -> String {
            conn.query_row(
                "SELECT table_id FROM _tables WHERE table_name = 'Legacy'",
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        let (stale_size, id_before) = {
            let conn = Connection::open(&path).unwrap();
            conn.execute("DELETE FROM _config WHERE key = 'schema_version'", [])
                .unwrap();
            (vec_size(&conn), table_id(&conn))
        };

        let storage = Storage::new(&path).unwrap();
        assert_eq!(recorded_version(storage.conn()), SCHEMA_VERSION.to_string());
        assert_eq!(
            vec_size(storage.conn()),
            stale_size - 7,
            "vector table sizes are recomputed"
        );
        let composite = crate::actions::gsi::composite_key_string(&[
            &AttributeValue::S("m".into()),
            &AttributeValue::S("2".into()),
        ])
        .unwrap();
        assert!(
            gsi_rows(storage.conn(), "byKey")
                .iter()
                .any(|[_, gsi_sk, _, _, _]| *gsi_sk == composite),
            "multi-attribute indexes are rebuilt"
        );
        assert_eq!(table_id(storage.conn()), id_before, "a table keeps its id");
        drop(storage);

        let db = crate::Database::new(&path).unwrap();
        let item = db
            .get_item(
                serde_json::from_value(serde_json::json!({
                    "TableName": "Legacy",
                    "Key": {"pk": {"S": "1"}}
                }))
                .unwrap(),
            )
            .unwrap()
            .item;
        assert!(item.is_some(), "the items read back");
    }

    /// Record a version one past this build's in the database at `conn`, as a
    /// newer build would have, and fold the WAL into the file, so the file's
    /// bytes are the whole database.
    fn stamp_newer(conn: &Connection) {
        conn.execute(
            "UPDATE _config SET value = ?1 WHERE key = 'schema_version'",
            [(SCHEMA_VERSION + 1).to_string()],
        )
        .unwrap();
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
    }

    /// A database at `path` as a newer build would have left it.
    fn write_newer_database(path: &str) {
        drop(crate::Database::new(path).unwrap());
        stamp_newer(&Connection::open(path).unwrap());
    }

    fn assert_refused_as_newer(err: DynoxideError) {
        let message = err.to_string();
        assert_eq!(
            message,
            crate::storage_backend::schema::newer_than_this_build(SCHEMA_VERSION + 1)
        );
        assert!(
            message.contains(&format!("schema version {}", SCHEMA_VERSION + 1))
                && message.contains(&format!("up to schema version {SCHEMA_VERSION}")),
            "{message}"
        );
    }

    #[test]
    fn a_database_a_newer_build_wrote_is_refused_and_left_as_it_was() {
        let (_dir, path) = v9_snapshot_path();
        write_newer_database(&path);
        let before = std::fs::read(&path).unwrap();

        assert_refused_as_newer(Storage::new(&path).map(drop).unwrap_err());
        assert_refused_as_newer(crate::Database::new(&path).map(drop).unwrap_err());

        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(
            recorded_version(&Connection::open(&path).unwrap()),
            (SCHEMA_VERSION + 1).to_string()
        );
    }

    /// The newer build's stamp may still be in the `-wal` file, not folded
    /// into the main file, when another connection holds the database open.
    /// It is read from there, and the refusal leaves both files as they were.
    #[test]
    fn a_newer_stamp_still_in_the_wal_is_refused_and_both_files_are_left_as_they_were() {
        let (_dir, path) = v9_snapshot_path();
        drop(crate::Database::new(&path).unwrap());
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch("PRAGMA wal_autocheckpoint = 0")
            .unwrap();
        writer
            .execute(
                "UPDATE _config SET value = ?1 WHERE key = 'schema_version'",
                [(SCHEMA_VERSION + 1).to_string()],
            )
            .unwrap();
        let wal = format!("{path}-wal");
        let main_before = std::fs::read(&path).unwrap();
        let wal_before = std::fs::read(&wal).unwrap();
        assert!(!wal_before.is_empty(), "the stamp is in the WAL");

        assert_refused_as_newer(Storage::new(&path).map(drop).unwrap_err());
        assert_refused_as_newer(crate::Database::new(&path).map(drop).unwrap_err());

        assert_eq!(std::fs::read(&path).unwrap(), main_before);
        assert_eq!(std::fs::read(&wal).unwrap(), wal_before);
        assert_eq!(recorded_version(&writer), (SCHEMA_VERSION + 1).to_string());
    }

    #[cfg(feature = "_has-encryption")]
    #[test]
    fn an_encrypted_database_a_newer_build_wrote_is_refused_and_left_as_it_was() {
        const KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let (_dir, path) = v9_snapshot_path();
        drop(crate::Database::new_encrypted(&path, KEY).unwrap());
        {
            let conn = Connection::open(&path).unwrap();
            conn.pragma_update(None, "key", format!("x'{KEY}'"))
                .unwrap();
            stamp_newer(&conn);
        }
        let before = std::fs::read(&path).unwrap();

        assert_refused_as_newer(
            crate::Database::new_encrypted(&path, KEY)
                .map(drop)
                .unwrap_err(),
        );

        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn a_restore_refuses_a_snapshot_a_newer_build_wrote() {
        let (_dir, path) = v9_snapshot_path();
        write_newer_database(&path);
        let before = std::fs::read(&path).unwrap();

        let mut storage = Storage::memory().unwrap();
        mark_live(&storage);
        assert_refused_as_newer(storage.restore_from(&path).unwrap_err());
        assert!(still_live(&storage), "the database must be left as it was");
        assert_eq!(recorded_version(storage.conn()), SCHEMA_VERSION.to_string());
        drop(storage);

        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn test_wal_mode_enabled() {
        let storage = test_storage();
        let mode: String = storage
            .conn()
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        // In-memory databases may report "memory" instead of "wal"
        assert!(mode == "wal" || mode == "memory", "Got mode: {mode}");
    }

    // FNV-1a parity contract. The wasm bridge hashes through js/fnv1a.js, which
    // parallel-scan segment assignment relies on agreeing with this native
    // scalar byte-for-byte. These vectors are the shared truth: the JS unit test
    // js/fnv1a.test.js asserts the same inputs hash to the same values, pinning
    // the two implementations together.
    #[test]
    fn fnv1a_hash_matches_known_vectors() {
        let storage = test_storage();
        let cases: [(&str, i64); 6] = [
            ("", 2166136261),
            ("a", 3826002220),
            ("u#1", 2199603432),
            ("artist#42", 2385694177),
            ("café", 2821410889),
            ("tenant#9007199254740993", 2022216178),
        ];
        for (input, expected) in cases {
            let got: i64 = storage
                .conn()
                .query_row("SELECT fnv1a_hash(?1)", [input], |row| row.get(0))
                .unwrap();
            assert_eq!(got, expected, "fnv1a_hash({input:?})");
        }
    }

    #[test]
    fn test_table_metadata_crud() {
        let storage = test_storage();

        // Initially no tables
        assert!(!storage.table_exists("TestTable").unwrap());
        assert!(storage.list_table_names().unwrap().is_empty());

        // Insert metadata
        storage
            .insert_table_metadata(&CreateTableMetadata {
                table_name: "TestTable",
                key_schema: r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#,
                attribute_definitions: r#"[{"AttributeName":"pk","AttributeType":"S"}]"#,
                created_at: 1000000,
                ..Default::default()
            })
            .unwrap();

        assert!(storage.table_exists("TestTable").unwrap());
        assert_eq!(storage.list_table_names().unwrap(), vec!["TestTable"]);

        // Get metadata
        let meta = storage.get_table_metadata("TestTable").unwrap().unwrap();
        assert_eq!(meta.table_name, "TestTable");
        assert_eq!(meta.table_status, "ACTIVE");
        assert_eq!(meta.created_at, 1000000);

        // Delete metadata
        assert!(storage.delete_table_metadata("TestTable").unwrap());
        assert!(!storage.table_exists("TestTable").unwrap());
    }

    #[test]
    fn test_create_and_drop_data_table() {
        let storage = test_storage();
        storage.create_data_table("MyTable").unwrap();

        // Should be able to insert into it
        storage
            .put_item("MyTable", "pk1", "", r#"{"pk":{"S":"pk1"}}"#, 10)
            .unwrap();

        let item = storage.get_item("MyTable", "pk1", "").unwrap();
        assert!(item.is_some());

        storage.drop_data_table("MyTable").unwrap();
    }

    #[test]
    fn test_item_crud() {
        let storage = test_storage();
        storage.create_data_table("Items").unwrap();

        // Put item
        let old = storage
            .put_item(
                "Items",
                "user#1",
                "profile",
                r#"{"name":{"S":"Alice"}}"#,
                20,
            )
            .unwrap();
        assert!(old.is_none()); // No previous item

        // Get item
        let item = storage.get_item("Items", "user#1", "profile").unwrap();
        assert_eq!(item.unwrap(), r#"{"name":{"S":"Alice"}}"#);

        // Replace item (returns old)
        let old = storage
            .put_item("Items", "user#1", "profile", r#"{"name":{"S":"Bob"}}"#, 18)
            .unwrap();
        assert_eq!(old.unwrap(), r#"{"name":{"S":"Alice"}}"#);

        // Delete item
        let deleted = storage.delete_item("Items", "user#1", "profile").unwrap();
        assert_eq!(deleted.unwrap(), r#"{"name":{"S":"Bob"}}"#);

        // Item should be gone
        assert!(
            storage
                .get_item("Items", "user#1", "profile")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_query_items() {
        let storage = test_storage();
        storage.create_data_table("Orders").unwrap();

        // Insert several items for the same partition key
        for i in 1..=5 {
            let sk = format!("order#{i:03}");
            let json = format!(r#"{{"id":{{"N":"{i}"}}}}"#);
            storage
                .put_item("Orders", "user#1", &sk, &json, 10)
                .unwrap();
        }

        // Query all for partition key
        let results = storage
            .query_items(
                "Orders",
                "user#1",
                &QueryParams {
                    forward: true,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].1, "order#001"); // Sorted ascending

        // Query with limit
        let results = storage
            .query_items(
                "Orders",
                "user#1",
                &QueryParams {
                    forward: true,
                    limit: Some(2),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(results.len(), 2);

        // Query reverse
        let results = storage
            .query_items(
                "Orders",
                "user#1",
                &QueryParams {
                    forward: false,
                    limit: Some(2),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, "order#005"); // Sorted descending
    }

    #[test]
    fn test_scan_items() {
        let storage = test_storage();
        storage.create_data_table("ScanTest").unwrap();

        storage.put_item("ScanTest", "a", "1", r#"{}"#, 2).unwrap();
        storage.put_item("ScanTest", "b", "2", r#"{}"#, 2).unwrap();
        storage.put_item("ScanTest", "c", "3", r#"{}"#, 2).unwrap();

        let results = storage.scan_items("ScanTest", &Default::default()).unwrap();
        assert_eq!(results.len(), 3);

        // Scan with limit
        let results = storage
            .scan_items(
                "ScanTest",
                &ScanParams {
                    limit: Some(2),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(results.len(), 2);

        // Scan with pagination
        let results = storage
            .scan_items(
                "ScanTest",
                &ScanParams {
                    limit: Some(2),
                    exclusive_start_pk: Some("a"),
                    exclusive_start_sk: Some("1"),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "b"); // Skipped "a"
    }

    #[test]
    fn test_count_items() {
        let storage = test_storage();
        storage.create_data_table("CountTest").unwrap();

        assert_eq!(storage.count_items("CountTest").unwrap(), 0);

        storage.put_item("CountTest", "a", "", r#"{}"#, 2).unwrap();
        storage.put_item("CountTest", "b", "", r#"{}"#, 2).unwrap();

        assert_eq!(storage.count_items("CountTest").unwrap(), 2);
    }

    #[test]
    fn test_gsi_table_lifecycle() {
        let storage = test_storage();
        storage.create_gsi_table("Orders", "ByDate").unwrap();

        // Should be able to write to the GSI table via raw SQL
        let gsi_name = "Orders::gsi::ByDate";
        let sql = format!(
            "INSERT INTO \"{}\" (gsi_pk, gsi_sk, table_pk, table_sk, item_json) VALUES (?1, ?2, ?3, ?4, ?5)",
            gsi_name.replace('"', "\"\"")
        );
        storage
            .conn()
            .execute(
                &sql,
                params!["2024-01-01", "001", "user#1", "order#001", r#"{}"#],
            )
            .unwrap();

        storage.drop_gsi_table("Orders", "ByDate").unwrap();
    }

    #[test]
    fn test_nonexistent_table_metadata() {
        let storage = test_storage();
        assert!(storage.get_table_metadata("Nonexistent").unwrap().is_none());
        assert!(!storage.delete_table_metadata("Nonexistent").unwrap());
    }

    #[test]
    fn test_metadata_cache_hit() {
        let storage = test_storage();
        storage
            .insert_table_metadata(&CreateTableMetadata {
                table_name: "CacheTest",
                key_schema: r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#,
                attribute_definitions: r#"[{"AttributeName":"pk","AttributeType":"S"}]"#,
                created_at: 1000000,
                ..Default::default()
            })
            .unwrap();

        // First call populates cache
        let meta1 = storage.get_table_metadata("CacheTest").unwrap().unwrap();
        assert_eq!(meta1.table_name, "CacheTest");

        // Second call should hit cache (same result)
        let meta2 = storage.get_table_metadata("CacheTest").unwrap().unwrap();
        assert_eq!(meta2.table_name, "CacheTest");
        assert_eq!(meta1.created_at, meta2.created_at);

        // Cache should have the entry
        assert!(storage.metadata_cache.borrow().contains_key("CacheTest"));
    }

    #[test]
    fn test_metadata_cache_invalidated_on_delete() {
        let storage = test_storage();
        storage
            .insert_table_metadata(&CreateTableMetadata {
                table_name: "DelCache",
                key_schema: r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#,
                attribute_definitions: r#"[{"AttributeName":"pk","AttributeType":"S"}]"#,
                created_at: 1000000,
                ..Default::default()
            })
            .unwrap();

        // Populate cache
        storage.get_table_metadata("DelCache").unwrap();
        assert!(storage.metadata_cache.borrow().contains_key("DelCache"));

        // Delete should invalidate cache
        storage.delete_table_metadata("DelCache").unwrap();
        assert!(!storage.metadata_cache.borrow().contains_key("DelCache"));
    }

    #[test]
    fn test_metadata_cache_invalidated_on_stream_enable() {
        let storage = test_storage();
        storage
            .insert_table_metadata(&CreateTableMetadata {
                table_name: "StreamCache",
                key_schema: r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#,
                attribute_definitions: r#"[{"AttributeName":"pk","AttributeType":"S"}]"#,
                created_at: 1000000,
                ..Default::default()
            })
            .unwrap();

        // Populate cache
        let meta = storage.get_table_metadata("StreamCache").unwrap().unwrap();
        assert!(!meta.stream_enabled);

        // Enable stream should invalidate cache
        storage
            .enable_stream("StreamCache", "NEW_AND_OLD_IMAGES", "2024-01-01T00:00:00")
            .unwrap();
        assert!(!storage.metadata_cache.borrow().contains_key("StreamCache"));

        // Next get should reflect the change
        let meta = storage.get_table_metadata("StreamCache").unwrap().unwrap();
        assert!(meta.stream_enabled);
    }

    #[test]
    fn test_metadata_cache_invalidated_on_ttl_update() {
        let storage = test_storage();
        storage
            .insert_table_metadata(&CreateTableMetadata {
                table_name: "TtlCache",
                key_schema: r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#,
                attribute_definitions: r#"[{"AttributeName":"pk","AttributeType":"S"}]"#,
                created_at: 1000000,
                ..Default::default()
            })
            .unwrap();

        // Populate cache
        let meta = storage.get_table_metadata("TtlCache").unwrap().unwrap();
        assert!(!meta.ttl_enabled);

        // Update TTL should invalidate cache
        storage
            .update_ttl_config("TtlCache", Some("expires_at"), true)
            .unwrap();
        assert!(!storage.metadata_cache.borrow().contains_key("TtlCache"));

        // Next get should reflect the change
        let meta = storage.get_table_metadata("TtlCache").unwrap().unwrap();
        assert!(meta.ttl_enabled);
        assert_eq!(meta.ttl_attribute, Some("expires_at".to_string()));
    }

    #[test]
    fn test_num_to_buffer_zero() {
        // numToBuffer("0") → [0x80]
        assert_eq!(num_to_buffer("0"), vec![0x80]);
        assert_eq!(num_to_buffer("-0"), vec![0x80]);
    }

    #[test]
    fn test_hash_prefix_string_keys() {
        // Verify known hash prefixes for string keys used in scan tests.
        // These specific values determine which segment items land in.
        let h1 = compute_hash_prefix(&AttributeValue::S("3635".into()));
        let h2 = compute_hash_prefix(&AttributeValue::S("228".into()));
        let h3 = compute_hash_prefix(&AttributeValue::S("1668".into()));
        let h4 = compute_hash_prefix(&AttributeValue::S("3435".into()));

        // With TotalSegments=4096, segment 0 owns bucket 0 only.
        // Items "3635" and "228" must be in segment 0 (bucket 0).
        assert_eq!(
            hash_bucket(&h1),
            0,
            "3635 should be bucket 0, got hash {h1}"
        );
        assert_eq!(hash_bucket(&h2), 0, "228 should be bucket 0, got hash {h2}");

        // "1668" must be in segment 1 (bucket 1)
        assert_eq!(
            hash_bucket(&h3),
            1,
            "1668 should be bucket 1, got hash {h3}"
        );

        // "3435" must be in segment 4 (bucket 4)
        assert_eq!(
            hash_bucket(&h4),
            4,
            "3435 should be bucket 4, got hash {h4}"
        );
    }

    #[test]
    fn a_key_string_read_back_hashes_as_the_key_did() {
        let mut keys: Vec<AttributeValue> = [
            "0",
            "-0",
            "1",
            "-1",
            "12300",
            "1.23e4",
            "0.000123",
            "-0.5",
            "123.4560",
            "99999999999999999999999999999999999999",
            "-1E-130",
            "9.9E125",
        ]
        .iter()
        .map(|n| AttributeValue::N((*n).to_string()))
        .collect();
        keys.extend(["", "a", "item-0042", "ü:é"].map(|s| AttributeValue::S(s.into())));
        keys.extend([vec![], vec![0u8], vec![0xff, 0x00, 0x10]].map(AttributeValue::B));
        for key in keys {
            let stored = key.to_key_string().unwrap();
            let read_back = crate::types::key_value_from_key_string(&stored)
                .unwrap_or_else(|| panic!("{stored} did not read back"));
            assert_eq!(
                compute_hash_prefix(&read_back),
                compute_hash_prefix(&key),
                "{key:?} via {stored}"
            );
        }
        assert!(crate::types::key_value_from_key_string("X:1").is_none());
        assert!(crate::types::key_value_from_key_string("N:12").is_none());
    }

    #[test]
    fn test_hash_prefix_number_keys() {
        // Verify number key hash prefixes from scan tests.
        // "251" must be in segment 1 (bucket 1) with TotalSegments=4096
        let h1 = compute_hash_prefix(&AttributeValue::N("251".into()));
        assert_eq!(hash_bucket(&h1), 1, "251 should be bucket 1, got hash {h1}");

        // "2388" must be in segment 4095 (bucket 4095)
        let h2 = compute_hash_prefix(&AttributeValue::N("2388".into()));
        assert_eq!(
            hash_bucket(&h2),
            4095,
            "2388 should be bucket 4095, got hash {h2}"
        );
    }

    #[test]
    fn test_hash_in_segment() {
        // bucket 0 should be in segment 0 of 4096
        assert!(hash_in_segment("000000", 0, 4096));
        assert!(!hash_in_segment("000000", 1, 4096));

        // bucket 1 should be in segment 1 of 4096
        assert!(hash_in_segment("001000", 1, 4096));
        assert!(!hash_in_segment("001000", 0, 4096));

        // bucket 4095 should be in segment 4095 of 4096
        assert!(hash_in_segment("fff000", 4095, 4096));
        assert!(!hash_in_segment("fff000", 0, 4096));

        // With 2 segments: buckets 0-2047 in segment 0, 2048-4095 in segment 1
        assert!(hash_in_segment("000000", 0, 2));
        assert!(hash_in_segment("7ff000", 0, 2));
        assert!(hash_in_segment("800000", 1, 2));
        assert!(hash_in_segment("fff000", 1, 2));
    }
}
