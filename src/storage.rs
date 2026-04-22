use crate::errors::{DynoxideError, Result};
use crate::types::AttributeValue;
use rusqlite::{Connection, params};
use std::cell::RefCell;
use std::collections::HashMap;

/// Current schema version. Stored in the `_config` table for future migrations.
const SCHEMA_VERSION: &str = "6";

/// Number of hash buckets used for parallel scan segment assignment.
/// Matches dynalite's implementation.
const HASH_BUCKETS: u32 = 4096;

// ---------------------------------------------------------------------------
// MD5-based hash prefix for parallel scan (dynalite-compatible)
// ---------------------------------------------------------------------------

/// Compute the 6-character hex hash prefix for a partition key value.
///
/// Uses `MD5("Outliers" + key_bytes)` where `key_bytes` depends on the
/// attribute type:
/// - `S`: UTF-8 bytes of the string
/// - `N`: Oracle packed BCD encoding via [`num_to_buffer`]
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
        // byteArray[0] ^= 0xffffffff — JS bitwise XOR on a number
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

fn ceiling_div(a: u32, b: u32) -> u32 {
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
/// data tables. All SQL lives here — higher layers work with Rust types.
pub struct Storage {
    conn: Connection,
    /// In-memory cache of table metadata to avoid repeated SQLite reads.
    /// Safe to use `RefCell` because `Storage` is always behind `Arc<Mutex<>>`.
    metadata_cache: RefCell<HashMap<String, TableMetadata>>,
}

impl Storage {
    /// Open a persistent database at the given path.
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        let mut storage = Self {
            conn,
            metadata_cache: RefCell::new(HashMap::new()),
        };
        storage.initialize().map_err(Self::maybe_encrypted_error)?;
        Ok(storage)
    }

    /// If a SQLite error is SQLITE_NOTADB, return a clearer error message
    /// suggesting the database may be encrypted.
    fn maybe_encrypted_error(err: DynoxideError) -> DynoxideError {
        if let DynoxideError::SqliteError(ref sqlite_err) = err {
            if let Some(rusqlite::ErrorCode::NotADatabase) = sqlite_err.sqlite_error_code() {
                return DynoxideError::InternalServerError(
                    "Database file is encrypted or not a valid SQLite database. \
                     If encrypted, enable the `encryption` or `encryption-cc` feature \
                     and use Database::new_encrypted() with the correct key."
                        .to_string(),
                );
            }
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
        // Safety: key is validated to be exactly 64 hex characters [0-9a-fA-F]
        // by Database::new_encrypted(), so no injection is possible in the
        // x'...' hex literal format. Note: pragma_update does NOT use parameter
        // binding for the PRAGMA value — hex validation is the sole injection defense.
        let mut pragma_val = format!("x'{key}'");
        conn.pragma_update(None, "key", &pragma_val)?;
        pragma_val.zeroize();
        // Verify the key works by reading from the database
        conn.execute_batch("SELECT count(*) FROM sqlite_master;")?;
        let mut storage = Self {
            conn,
            metadata_cache: RefCell::new(HashMap::new()),
        };
        storage.initialize()?;
        Ok(storage)
    }

    /// Open an in-memory database (for tests and ephemeral use).
    pub fn memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let mut storage = Self {
            conn,
            metadata_cache: RefCell::new(HashMap::new()),
        };
        storage.initialize()?;
        Ok(storage)
    }

    /// Initialize the database: WAL mode, metadata tables, config.
    fn initialize(&mut self) -> Result<()> {
        // Enable WAL mode for better concurrency
        self.conn.pragma_update(None, "journal_mode", "WAL")?;

        // Register FNV-1a hash function for parallel scan segment assignment.
        // Uses get_raw() to borrow directly from SQLite's buffer (zero-copy).
        self.conn.create_scalar_function(
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

        // Create metadata tables
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS _tables (
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
            );

            CREATE TABLE IF NOT EXISTS _stream_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                event_name TEXT NOT NULL,
                keys_json TEXT NOT NULL,
                new_image TEXT,
                old_image TEXT,
                sequence_number TEXT NOT NULL,
                shard_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                user_identity TEXT
            );",
        )?;

        // Migrate: add user_identity column if it doesn't exist (for databases created before Phase 11)
        let _ = self
            .conn
            .execute_batch("ALTER TABLE _stream_records ADD COLUMN user_identity TEXT");

        // Set schema version if not present
        self.conn.execute(
            "INSERT OR IGNORE INTO _config (key, value) VALUES ('schema_version', ?1)",
            params![SCHEMA_VERSION],
        )?;

        // Run schema migrations
        let version: i32 = self
            .conn
            .query_row(
                "SELECT value FROM _config WHERE key = 'schema_version'",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1);

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

        Ok(())
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
            if let Some(json) = gsi_json {
                if let Ok(gsis) = serde_json::from_str::<Vec<serde_json::Value>>(json) {
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
            }
            if let Some(json) = lsi_json {
                if let Ok(lsis) = serde_json::from_str::<Vec<serde_json::Value>>(json) {
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
        self.conn.execute(
            "INSERT INTO _tables (table_name, key_schema, attribute_definitions, gsi_definitions, \
             lsi_definitions, provisioned_throughput, created_at, sse_specification, table_class, \
             deletion_protection_enabled, billing_mode)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                m.table_name,
                m.key_schema,
                m.attribute_definitions,
                m.gsi_definitions,
                m.lsi_definitions,
                m.provisioned_throughput,
                m.created_at,
                m.sse_specification,
                m.table_class,
                m.deletion_protection_enabled as i32,
                m.billing_mode,
            ],
        )?;
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

        let sql = format!("SELECT {TABLE_METADATA_COLUMNS} FROM _tables WHERE table_name = ?1");
        let mut stmt = self.conn.prepare(&sql)?;

        let result = stmt.query_row(params![table_name], row_to_metadata);

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
        let affected = self.conn.execute(
            "DELETE FROM _tables WHERE table_name = ?1",
            params![table_name],
        )?;
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
        self.conn.execute(
            "UPDATE _tables SET attribute_definitions = ?1, gsi_definitions = ?2 WHERE table_name = ?3",
            params![attribute_definitions, gsi_definitions, table_name],
        )?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Update provisioned throughput for a table.
    pub fn update_provisioned_throughput(
        &self,
        table_name: &str,
        provisioned_throughput: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE _tables SET provisioned_throughput = ?1 WHERE table_name = ?2",
            params![provisioned_throughput, table_name],
        )?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Clear provisioned throughput for a table (sets to SQL NULL).
    pub fn clear_provisioned_throughput(&self, table_name: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE _tables SET provisioned_throughput = NULL WHERE table_name = ?1",
            params![table_name],
        )?;
        self.metadata_cache.borrow_mut().remove(table_name);
        Ok(())
    }

    /// Update billing mode for a table.
    pub fn update_billing_mode(&self, table_name: &str, billing_mode: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE _tables SET billing_mode = ?1 WHERE table_name = ?2",
            params![billing_mode, table_name],
        )?;
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
        self.conn.execute(
            "UPDATE _tables SET deletion_protection_enabled = ?1 WHERE table_name = ?2",
            params![enabled as i32, table_name],
        )?;
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
        let mut stmt = self
            .conn
            .prepare("SELECT table_name FROM _tables ORDER BY table_name")?;
        let names = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(names)
    }

    /// Check if a table exists in metadata.
    pub fn table_exists(&self, table_name: &str) -> Result<bool> {
        let count: i32 = self.conn.query_row(
            "SELECT COUNT(*) FROM _tables WHERE table_name = ?1",
            params![table_name],
            |row| row.get(0),
        )?;
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
        let sql = format!(
            "CREATE TABLE \"{}\" (
                pk TEXT NOT NULL,
                sk TEXT NOT NULL DEFAULT '',
                item_json TEXT NOT NULL,
                item_size INTEGER NOT NULL,
                cached_at REAL,
                hash_prefix TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (pk, sk)
            )",
            escape_table_name(table_name)
        );
        self.conn.execute(&sql, [])?;
        Ok(())
    }

    /// Drop a data table.
    pub fn drop_data_table(&self, table_name: &str) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS \"{}\"", escape_table_name(table_name));
        self.conn.execute(&sql, [])?;
        Ok(())
    }

    /// Create a GSI table.
    pub fn create_gsi_table(&self, table_name: &str, index_name: &str) -> Result<()> {
        let gsi_table_name = format!("{table_name}::gsi::{index_name}");
        let escaped = escape_table_name(&gsi_table_name);
        let sql = format!(
            "CREATE TABLE \"{escaped}\" (
                gsi_pk TEXT NOT NULL,
                gsi_sk TEXT NOT NULL DEFAULT '',
                table_pk TEXT NOT NULL,
                table_sk TEXT NOT NULL DEFAULT '',
                item_json TEXT NOT NULL,
                PRIMARY KEY (gsi_pk, gsi_sk, table_pk, table_sk)
            )"
        );
        self.conn.execute(&sql, [])?;

        let idx_name = escape_table_name(&format!("{gsi_table_name}::base_key"));
        self.conn.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS \"{idx_name}\" ON \"{escaped}\" (table_pk, table_sk)"
        ))?;
        Ok(())
    }

    /// Drop a GSI table.
    pub fn drop_gsi_table(&self, table_name: &str, index_name: &str) -> Result<()> {
        let gsi_table_name = format!("{table_name}::gsi::{index_name}");
        let sql = format!(
            "DROP TABLE IF EXISTS \"{}\"",
            escape_table_name(&gsi_table_name)
        );
        self.conn.execute(&sql, [])?;
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
        let gsi_table_name = format!("{table_name}::gsi::{index_name}");
        let sql = format!(
            "INSERT OR REPLACE INTO \"{}\" (gsi_pk, gsi_sk, table_pk, table_sk, item_json) VALUES (?1, ?2, ?3, ?4, ?5)",
            escape_table_name(&gsi_table_name)
        );
        self.conn
            .prepare_cached(&sql)?
            .execute(params![gsi_pk, gsi_sk, table_pk, table_sk, item_json])?;
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
        let gsi_table_name = format!("{table_name}::gsi::{index_name}");
        let sql = format!(
            "DELETE FROM \"{}\" WHERE table_pk = ?1 AND table_sk = ?2",
            escape_table_name(&gsi_table_name)
        );
        self.conn
            .prepare_cached(&sql)?
            .execute(params![table_pk, table_sk])?;
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
        let gsi_table_name = format!("{table_name}::gsi::{index_name}");
        let mut sql = format!(
            "SELECT gsi_pk, gsi_sk, item_json FROM \"{}\" WHERE gsi_pk = ?1",
            escape_table_name(&gsi_table_name)
        );

        let mut param_idx = 2;
        let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(gsi_pk.to_string())];

        if let Some(cond) = params.sk_condition {
            // Replace "sk" with "gsi_sk" in the condition.
            // N.B. This string replacement works because all SQL fragments generated
            // by key_condition use the form ` sk ` or ` sk>` — they never embed
            // `sk` without surrounding whitespace/operator. A more robust solution
            // would thread the column name through the key condition builder, but
            // that is a larger refactor deferred for now.
            let gsi_cond = cond.replace(" sk ", " gsi_sk ").replace(" sk>", " gsi_sk>");
            sql.push(' ');
            sql.push_str(&gsi_cond);
            for &p in params.sk_params {
                all_params.push(Box::new(p.to_string()));
                param_idx += 1;
            }
        }

        // For GSI pagination, use a composite cursor (gsi_sk, table_pk, table_sk)
        // so that hash-only GSIs (where gsi_sk is always '') paginate correctly.
        if let (Some(start_sk), Some(start_base_pk), Some(start_base_sk)) = (
            params.exclusive_start_sk,
            params.exclusive_start_base_pk,
            params.exclusive_start_base_sk,
        ) {
            let op = if params.forward { ">" } else { "<" };
            sql.push_str(&format!(
                " AND (gsi_sk, table_pk, table_sk) {op} (?{}, ?{}, ?{})",
                param_idx,
                param_idx + 1,
                param_idx + 2
            ));
            all_params.push(Box::new(start_sk.to_string()));
            all_params.push(Box::new(start_base_pk.to_string()));
            all_params.push(Box::new(start_base_sk.to_string()));
        } else if let Some(start_sk) = params.exclusive_start_sk {
            if params.forward {
                sql.push_str(&format!(" AND gsi_sk > ?{param_idx}"));
            } else {
                sql.push_str(&format!(" AND gsi_sk < ?{param_idx}"));
            }
            all_params.push(Box::new(start_sk.to_string()));
        }

        sql.push_str(if params.forward {
            " ORDER BY gsi_sk ASC, table_pk ASC, table_sk ASC"
        } else {
            " ORDER BY gsi_sk DESC, table_pk DESC, table_sk DESC"
        });

        if let Some(lim) = params.limit {
            sql.push_str(&format!(" LIMIT {lim}"));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            all_params.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
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
        let gsi_table_name = format!("{table_name}::gsi::{index_name}");
        let mut sql = format!(
            "SELECT gsi_pk, gsi_sk, item_json FROM \"{}\"",
            escape_table_name(&gsi_table_name)
        );

        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut where_clauses = Vec::new();
        let mut param_idx = 1;

        if let (Some(start_pk), Some(start_sk)) =
            (params.exclusive_start_pk, params.exclusive_start_sk)
        {
            // The GSI table's primary key is (gsi_pk, gsi_sk, table_pk, table_sk).
            // Using only (gsi_pk, gsi_sk) for the cursor skips rows that share the
            // same GSI key but have different base table keys.
            if let (Some(base_pk), Some(base_sk)) = (
                params.exclusive_start_base_pk,
                params.exclusive_start_base_sk,
            ) {
                where_clauses.push(format!(
                    "(gsi_pk, gsi_sk, table_pk, table_sk) > (?{}, ?{}, ?{}, ?{})",
                    param_idx,
                    param_idx + 1,
                    param_idx + 2,
                    param_idx + 3
                ));
                params_vec.push(Box::new(start_pk.to_string()));
                params_vec.push(Box::new(start_sk.to_string()));
                params_vec.push(Box::new(base_pk.to_string()));
                params_vec.push(Box::new(base_sk.to_string()));
                param_idx += 4;
            } else {
                where_clauses.push(format!(
                    "(gsi_pk, gsi_sk) > (?{}, ?{})",
                    param_idx,
                    param_idx + 1
                ));
                params_vec.push(Box::new(start_pk.to_string()));
                params_vec.push(Box::new(start_sk.to_string()));
                param_idx += 2;
            }
        }

        // For GSI parallel scan, hash on the base table pk (table_pk column)
        if let (Some(seg), Some(total)) = (params.segment, params.total_segments) {
            where_clauses.push(format!(
                "(fnv1a_hash(table_pk) % ?{}) = ?{}",
                param_idx,
                param_idx + 1
            ));
            params_vec.push(Box::new(total as i64));
            params_vec.push(Box::new(seg as i64));
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        sql.push_str(" ORDER BY gsi_pk ASC, gsi_sk ASC, table_pk ASC, table_sk ASC");

        if let Some(lim) = params.limit {
            sql.push_str(&format!(" LIMIT {lim}"));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
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
        let lsi_table_name = format!("{table_name}::lsi::{index_name}");
        let escaped = escape_table_name(&lsi_table_name);
        let sql = format!(
            "CREATE TABLE \"{escaped}\" (
                pk TEXT NOT NULL,
                sk TEXT NOT NULL DEFAULT '',
                base_pk TEXT NOT NULL,
                base_sk TEXT NOT NULL DEFAULT '',
                item_json TEXT NOT NULL,
                PRIMARY KEY (pk, sk, base_pk, base_sk)
            )"
        );
        self.conn.execute(&sql, [])?;

        let idx_name = escape_table_name(&format!("{lsi_table_name}::base_key"));
        self.conn.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS \"{idx_name}\" ON \"{escaped}\" (base_pk, base_sk)"
        ))?;
        Ok(())
    }

    /// Drop an LSI table.
    pub fn drop_lsi_table(&self, table_name: &str, index_name: &str) -> Result<()> {
        let lsi_table_name = format!("{table_name}::lsi::{index_name}");
        let sql = format!(
            "DROP TABLE IF EXISTS \"{}\"",
            escape_table_name(&lsi_table_name)
        );
        self.conn.execute(&sql, [])?;
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
        let lsi_table_name = format!("{table_name}::lsi::{index_name}");
        let sql = format!(
            "INSERT OR REPLACE INTO \"{}\" (pk, sk, base_pk, base_sk, item_json) VALUES (?1, ?2, ?3, ?4, ?5)",
            escape_table_name(&lsi_table_name)
        );
        self.conn
            .prepare_cached(&sql)?
            .execute(params![pk, sk, base_pk, base_sk, item_json])?;
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
        let lsi_table_name = format!("{table_name}::lsi::{index_name}");
        let sql = format!(
            "DELETE FROM \"{}\" WHERE base_pk = ?1 AND base_sk = ?2",
            escape_table_name(&lsi_table_name)
        );
        self.conn
            .prepare_cached(&sql)?
            .execute(params![base_pk, base_sk])?;
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
        let lsi_table_name = format!("{table_name}::lsi::{index_name}");
        let mut sql = format!(
            "SELECT pk, sk, item_json FROM \"{}\" WHERE pk = ?1",
            escape_table_name(&lsi_table_name)
        );

        let mut param_idx = 2;
        let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(pk.to_string())];

        if let Some(cond) = params.sk_condition {
            sql.push(' ');
            sql.push_str(cond);
            for &p in params.sk_params {
                all_params.push(Box::new(p.to_string()));
                param_idx += 1;
            }
        }

        // Use composite cursor when all three LSI pagination values are present,
        // otherwise fall back to simple sk comparison.
        if let (Some(start_sk), Some(start_base_pk), Some(start_base_sk)) = (
            params.exclusive_start_sk,
            params.exclusive_start_base_pk,
            params.exclusive_start_base_sk,
        ) {
            let op = if params.forward { ">" } else { "<" };
            sql.push_str(&format!(
                " AND (sk, base_pk, base_sk) {op} (?{}, ?{}, ?{})",
                param_idx,
                param_idx + 1,
                param_idx + 2
            ));
            all_params.push(Box::new(start_sk.to_string()));
            all_params.push(Box::new(start_base_pk.to_string()));
            all_params.push(Box::new(start_base_sk.to_string()));
        } else if let Some(start_sk) = params.exclusive_start_sk {
            if params.forward {
                sql.push_str(&format!(" AND sk > ?{param_idx}"));
            } else {
                sql.push_str(&format!(" AND sk < ?{param_idx}"));
            }
            all_params.push(Box::new(start_sk.to_string()));
        }

        sql.push_str(if params.forward {
            " ORDER BY sk ASC, base_pk ASC, base_sk ASC"
        } else {
            " ORDER BY sk DESC, base_pk DESC, base_sk DESC"
        });

        if let Some(lim) = params.limit {
            sql.push_str(&format!(" LIMIT {lim}"));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            all_params.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
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
        let lsi_table_name = format!("{table_name}::lsi::{index_name}");
        let mut sql = format!(
            "SELECT pk, sk, item_json FROM \"{}\"",
            escape_table_name(&lsi_table_name)
        );

        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut where_clauses = Vec::new();
        let mut param_idx = 1;

        // Use composite cursor when base key values are present for correct LSI pagination.
        if let (Some(start_pk), Some(start_sk), Some(start_base_pk), Some(start_base_sk)) = (
            params.exclusive_start_pk,
            params.exclusive_start_sk,
            params.exclusive_start_base_pk,
            params.exclusive_start_base_sk,
        ) {
            where_clauses.push(format!(
                "(pk, sk, base_pk, base_sk) > (?{}, ?{}, ?{}, ?{})",
                param_idx,
                param_idx + 1,
                param_idx + 2,
                param_idx + 3
            ));
            params_vec.push(Box::new(start_pk.to_string()));
            params_vec.push(Box::new(start_sk.to_string()));
            params_vec.push(Box::new(start_base_pk.to_string()));
            params_vec.push(Box::new(start_base_sk.to_string()));
            param_idx += 4;
        } else if let (Some(start_pk), Some(start_sk)) =
            (params.exclusive_start_pk, params.exclusive_start_sk)
        {
            where_clauses.push(format!("(pk, sk) > (?{}, ?{})", param_idx, param_idx + 1));
            params_vec.push(Box::new(start_pk.to_string()));
            params_vec.push(Box::new(start_sk.to_string()));
            param_idx += 2;
        }

        // For LSI parallel scan, hash on the base table pk (base_pk column)
        if let (Some(seg), Some(total)) = (params.segment, params.total_segments) {
            where_clauses.push(format!(
                "(fnv1a_hash(base_pk) % ?{}) = ?{}",
                param_idx,
                param_idx + 1
            ));
            params_vec.push(Box::new(total as i64));
            params_vec.push(Box::new(seg as i64));
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        sql.push_str(" ORDER BY pk ASC, sk ASC");

        if let Some(lim) = params.limit {
            sql.push_str(&format!(" LIMIT {lim}"));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
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
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        Ok(())
    }

    /// Commit the current transaction.
    pub fn commit(&self) -> Result<()> {
        self.conn.execute_batch("COMMIT")?;
        Ok(())
    }

    /// Rollback the current transaction.
    pub fn rollback(&self) -> Result<()> {
        self.conn.execute_batch("ROLLBACK")?;
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

        let escaped = escape_table_name(table_name);
        let sql = format!(
            "INSERT OR REPLACE INTO \"{escaped}\" (pk, sk, item_json, item_size, cached_at, hash_prefix) \
             VALUES (?1, ?2, ?3, ?4, \
             (SELECT cached_at FROM \"{escaped}\" WHERE pk = ?1 AND sk = ?2), ?5)"
        );
        self.conn.execute(
            &sql,
            params![pk, sk, item_json, item_size as i64, hash_prefix],
        )?;

        Ok(old_item)
    }

    /// Get a single item by primary key.
    pub fn get_item(&self, table_name: &str, pk: &str, sk: &str) -> Result<Option<String>> {
        let sql = format!(
            "SELECT item_json FROM \"{}\" WHERE pk = ?1 AND sk = ?2",
            escape_table_name(table_name)
        );
        let result = self.conn.query_row(&sql, params![pk, sk], |row| row.get(0));

        match result {
            Ok(json) => Ok(Some(json)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(DynoxideError::from(e)),
        }
    }

    /// Return the total item_size for all items sharing the given partition key.
    pub fn get_partition_size(&self, table_name: &str, pk: &str) -> Result<i64> {
        let sql = format!(
            "SELECT COALESCE(SUM(item_size), 0) FROM \"{}\" WHERE pk = ?1",
            escape_table_name(table_name)
        );
        let size: i64 = self.conn.query_row(&sql, params![pk], |row| row.get(0))?;
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
        let lsi_table_name = format!("{table_name}::lsi::{index_name}");
        let sql = format!(
            "SELECT COALESCE(SUM(length(item_json)), 0) FROM \"{}\" WHERE pk = ?1",
            escape_table_name(&lsi_table_name)
        );
        let size: i64 = self.conn.query_row(&sql, params![pk], |row| row.get(0))?;
        Ok(size)
    }

    /// Delete an item by primary key. Returns the old item_json if it existed.
    pub fn delete_item(&self, table_name: &str, pk: &str, sk: &str) -> Result<Option<String>> {
        let old_item = self.get_item(table_name, pk, sk)?;

        let sql = format!(
            "DELETE FROM \"{}\" WHERE pk = ?1 AND sk = ?2",
            escape_table_name(table_name)
        );
        self.conn.execute(&sql, params![pk, sk])?;

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
        let mut sql = format!(
            "SELECT pk, sk, item_json FROM \"{}\" WHERE pk = ?1",
            escape_table_name(table_name)
        );

        let mut param_idx = 2;
        let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(pk.to_string())];

        if let Some(cond) = params.sk_condition {
            sql.push(' ');
            sql.push_str(cond);
            for &p in params.sk_params {
                all_params.push(Box::new(p.to_string()));
                param_idx += 1;
            }
        }

        if let Some(start_sk) = params.exclusive_start_sk {
            if params.forward {
                sql.push_str(&format!(" AND sk > ?{param_idx}"));
            } else {
                sql.push_str(&format!(" AND sk < ?{param_idx}"));
            }
            all_params.push(Box::new(start_sk.to_string()));
        }

        sql.push_str(if params.forward {
            " ORDER BY sk ASC"
        } else {
            " ORDER BY sk DESC"
        });

        if let Some(lim) = params.limit {
            sql.push_str(&format!(" LIMIT {lim}"));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            all_params.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
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
        let mut sql = format!(
            "SELECT pk, sk, item_json FROM \"{}\"",
            escape_table_name(table_name)
        );

        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut where_clauses = Vec::new();
        let mut param_idx = 1;

        // For parallel scan with hash_prefix-based segment filtering
        let is_parallel = params.segment.is_some() && params.total_segments.is_some();

        if let (Some(start_pk), Some(start_sk)) =
            (params.exclusive_start_pk, params.exclusive_start_sk)
        {
            if is_parallel {
                // For parallel scans, pagination must respect hash_prefix ordering
                where_clauses.push(format!(
                    "(hash_prefix, pk, sk) > ((SELECT hash_prefix FROM \"{}\" WHERE pk = ?{} AND sk = ?{} LIMIT 1), ?{}, ?{})",
                    escape_table_name(table_name),
                    param_idx, param_idx + 1,
                    param_idx, param_idx + 1
                ));
            } else {
                where_clauses.push(format!("(pk, sk) > (?{}, ?{})", param_idx, param_idx + 1));
            }
            params_vec.push(Box::new(start_pk.to_string()));
            params_vec.push(Box::new(start_sk.to_string()));
            param_idx += 2;
        }

        if let (Some(seg), Some(total)) = (params.segment, params.total_segments) {
            // Use hash_prefix column for segment assignment.
            // Bucket = parseInt(hash_prefix[0..3], 16).
            // Segment owns buckets from ceil(4096*seg/total) to ceil(4096*(seg+1)/total)-1.
            let start_bucket = ceiling_div(HASH_BUCKETS * seg, total);
            let end_bucket = ceiling_div(HASH_BUCKETS * (seg + 1), total) - 1;
            let start_hex = format!("{:03x}", start_bucket);
            let end_hex = format!("{:03x}", end_bucket);
            // hash_prefix is a 6-char hex string; compare the first 3 chars
            where_clauses.push(format!(
                "substr(hash_prefix, 1, 3) >= ?{} AND substr(hash_prefix, 1, 3) <= ?{}",
                param_idx,
                param_idx + 1
            ));
            params_vec.push(Box::new(start_hex));
            params_vec.push(Box::new(end_hex));
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        // For parallel scans, order by hash_prefix for dynalite-compatible behaviour.
        // For regular scans, use pk/sk ordering.
        if is_parallel {
            sql.push_str(" ORDER BY hash_prefix ASC, pk ASC, sk ASC");
        } else {
            sql.push_str(" ORDER BY pk ASC, sk ASC");
        }

        if let Some(lim) = params.limit {
            sql.push_str(&format!(" LIMIT {lim}"));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    /// Count items in a table.
    pub fn count_items(&self, table_name: &str) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", escape_table_name(table_name));
        let count: i64 = self.conn.query_row(&sql, [], |row| row.get(0))?;
        Ok(count)
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
    pub fn restore_from(&mut self, path: &str) -> Result<()> {
        let source = Connection::open(path)?;
        self.restore_from_connection(&source)
    }

    /// Backup the current database to a new in-memory SQLite connection.
    ///
    /// Used for in-memory snapshot storage — the returned connection holds
    /// a complete copy of the database without touching the filesystem.
    pub fn backup_to_memory(&self) -> Result<Connection> {
        let mut dest = Connection::open_in_memory()?;
        {
            let backup = rusqlite::backup::Backup::new(&self.conn, &mut dest)?;
            backup.run_to_completion(100, std::time::Duration::from_millis(0), None)?;
        }
        Ok(dest)
    }

    /// Restore the database from another SQLite connection using the backup API.
    ///
    /// Replaces the current database contents with the source connection's
    /// contents. Invalidates the metadata cache.
    pub fn restore_from_connection(&mut self, source: &Connection) -> Result<()> {
        let backup = rusqlite::backup::Backup::new(source, &mut self.conn)?;
        backup.run_to_completion(100, std::time::Duration::from_millis(0), None)?;
        self.metadata_cache.borrow_mut().clear();
        Ok(())
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
        self.conn.execute(
            "INSERT INTO _stream_records (table_name, event_name, keys_json, new_image, old_image, sequence_number, shard_id, created_at, user_identity)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![table_name, event_name, keys_json, new_image, old_image, sequence_number, shard_id, created_at, user_identity],
        )?;
        Ok(())
    }

    /// Get the next sequence number for a table's stream.
    pub fn next_stream_sequence_number(&self, table_name: &str) -> Result<i64> {
        let result: std::result::Result<i64, _> = self.conn.query_row(
            "SELECT COALESCE(MAX(CAST(sequence_number AS INTEGER)), 0) + 1 FROM _stream_records WHERE table_name = ?1",
            params![table_name],
            |row| row.get(0),
        );
        match result {
            Ok(n) => Ok(n),
            Err(_) => Ok(1),
        }
    }

    /// Get stream records for a shard starting after a given sequence number.
    pub fn get_stream_records(
        &self,
        table_name: &str,
        shard_id: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<StreamRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT event_name, keys_json, new_image, old_image, sequence_number, created_at, user_identity
             FROM _stream_records
             WHERE table_name = ?1 AND shard_id = ?2 AND CAST(sequence_number AS INTEGER) > ?3
             ORDER BY CAST(sequence_number AS INTEGER) ASC
             LIMIT ?4",
        )?;
        let rows = stmt
            .query_map(
                params![table_name, shard_id, after_sequence, limit as i64],
                |row| {
                    Ok(StreamRecord {
                        event_name: row.get(0)?,
                        keys_json: row.get(1)?,
                        new_image: row.get(2)?,
                        old_image: row.get(3)?,
                        sequence_number: row.get(4)?,
                        created_at: row.get(5)?,
                        user_identity: row.get(6)?,
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// List tables that have streams enabled.
    pub fn list_stream_enabled_tables(&self) -> Result<Vec<TableMetadata>> {
        let sql = format!(
            "SELECT {TABLE_METADATA_COLUMNS} FROM _tables WHERE stream_enabled = 1 ORDER BY table_name"
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
            "SELECT {TABLE_METADATA_COLUMNS} FROM _tables WHERE ttl_enabled = 1 ORDER BY table_name"
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
        let result: std::result::Result<(Option<String>, Option<String>), _> = self.conn.query_row(
            "SELECT MIN(sequence_number), MAX(sequence_number) FROM _stream_records WHERE table_name = ?1 AND shard_id = ?2",
            params![table_name, shard_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
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

/// Escape double quotes in table names for safe SQL identifier use.
pub(crate) fn escape_table_name(name: &str) -> String {
    name.replace('"', "\"\"")
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
}

/// The standard SELECT column list for _tables queries.
const TABLE_METADATA_COLUMNS: &str = "table_name, key_schema, attribute_definitions, gsi_definitions, \
     lsi_definitions, stream_enabled, stream_view_type, stream_label, ttl_attribute, ttl_enabled, \
     created_at, table_status, billing_mode, provisioned_throughput, \
     sse_specification, table_class, deletion_protection_enabled";

/// Map a row from the _tables SELECT to a TableMetadata struct.
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
    })
}

#[cfg(test)]
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
        assert_eq!(version, SCHEMA_VERSION);
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
