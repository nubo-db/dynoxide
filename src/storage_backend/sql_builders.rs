//! Backend-neutral SQL builders: the single source of truth for the SQL that
//! every storage backend issues.
//!
//! Each builder returns the SQL string plus a `Vec<SqlParam>` of bound
//! parameters. The native rusqlite backend binds `SqlParam` through its
//! [`rusqlite::ToSql`] impl; the wasm SQLite backend converts each
//! `SqlParam` to a JS value for the bridge. Keeping the SQL here, rather than
//! inside a backend, guarantees both backends issue identical statements: a
//! query fixed on one is fixed for both, so the two backends cannot silently
//! drift apart.
//!
//! Builders interpolate table names directly (SQLite cannot bind an
//! identifier) via [`escape_table_name`] and bind values positionally with
//! `?N` placeholders.
//!
//! Internal API. This module is `pub` only so both backends can share it; it is
//! `#[doc(hidden)]` and carries no stability guarantee - treat it as a private
//! contract between the rusqlite and wasm backends, not a public surface.

use crate::storage::{CreateTableMetadata, HASH_BUCKETS, QueryParams, ScanParams, ceiling_div};
use std::borrow::Cow;

/// One bound SQL parameter, covering the SQLite value universe and nothing
/// more.
///
/// Text and blob values are held as [`Cow`], so the common case (binding a
/// `&str` that already lives in the caller, like an item's JSON) borrows with
/// no allocation, while builders that compute an owned value (a formatted hash
/// bucket, say) still store it inline. The native backend binds straight
/// through [`rusqlite::ToSql`]; the wasm backend copies into a JS value, which
/// it must do regardless.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParam<'a> {
    /// A UTF-8 text value, bound as SQLite `TEXT`.
    Text(Cow<'a, str>),
    /// A 64-bit signed integer, bound as SQLite `INTEGER`.
    Integer(i64),
    /// A double, bound as SQLite `REAL`.
    Real(f64),
    /// A byte string, bound as SQLite `BLOB`.
    Blob(Cow<'a, [u8]>),
    /// SQL `NULL`.
    Null,
}

impl<'a> SqlParam<'a> {
    /// Text parameter. A `&str` borrows; an owned `String` is taken as-is.
    pub fn text(s: impl Into<Cow<'a, str>>) -> Self {
        SqlParam::Text(s.into())
    }

    /// `TEXT` (borrowed) when `Some`, SQL `NULL` when `None`.
    pub fn opt_text(s: Option<&'a str>) -> Self {
        match s {
            Some(v) => SqlParam::Text(Cow::Borrowed(v)),
            None => SqlParam::Null,
        }
    }

    /// `REAL` when `Some`, SQL `NULL` when `None`.
    pub fn opt_real(v: Option<f64>) -> Self {
        match v {
            Some(v) => SqlParam::Real(v),
            None => SqlParam::Null,
        }
    }
}

/// Bind `SqlParam` directly to a rusqlite statement, so the native backend can
/// pass `rusqlite::params_from_iter(params.iter())` for any builder.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
impl rusqlite::ToSql for SqlParam<'_> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value, ValueRef};
        Ok(match self {
            SqlParam::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
            SqlParam::Integer(i) => ToSqlOutput::Owned(Value::Integer(*i)),
            SqlParam::Real(f) => ToSqlOutput::Owned(Value::Real(*f)),
            SqlParam::Blob(b) => ToSqlOutput::Borrowed(ValueRef::Blob(b)),
            SqlParam::Null => ToSqlOutput::Owned(Value::Null),
        })
    }
}

/// Escape embedded double quotes so a table name is safe to interpolate as a
/// SQLite quoted identifier.
pub fn escape_table_name(name: &str) -> String {
    name.replace('"', "\"\"")
}

/// The standard SELECT column list for `_tables` queries.
///
/// Both metadata row-mappers decode this positionally - native
/// `Storage::row_to_metadata` and the wasm `WasmBridgeBackend::row_to_metadata` -
/// so the column order is load-bearing and the two mappers must stay in lockstep
/// with it. Append new columns at the end only: an older binary then reads a
/// database written by a newer one by simply not selecting the trailing column.
pub(crate) const TABLE_METADATA_COLUMNS: &str = "table_name, key_schema, attribute_definitions, gsi_definitions, \
     lsi_definitions, stream_enabled, stream_view_type, stream_label, ttl_attribute, ttl_enabled, \
     created_at, table_status, billing_mode, provisioned_throughput, \
     sse_specification, table_class, deletion_protection_enabled, on_demand_throughput, table_id";

/// Idempotent schema bootstrap shared by both backends: the metadata, config,
/// and stream-record tables at the current schema version. Native
/// `Storage::initialize` runs this (then layers WAL, custom functions, and
/// migrations on top); the wasm backend runs it on open. Keeping it here means
/// the `_tables` column set cannot drift between backends.
pub const INIT_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS _config (
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
                deletion_protection_enabled INTEGER DEFAULT 0,
                on_demand_throughput TEXT,
                table_id TEXT
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
            );";

// --- Transaction control (constant SQL) ---------------------------------

/// Begin an immediate write transaction.
pub const BEGIN: &str = "BEGIN IMMEDIATE";
/// Commit the current transaction.
pub const COMMIT: &str = "COMMIT";
/// Roll back the current transaction.
pub const ROLLBACK: &str = "ROLLBACK";

// --- Table metadata (`_tables`) -----------------------------------------

/// Insert a metadata row into `_tables`.
///
/// The TableId is assigned once, here, at create time and never changes for
/// this incarnation of the table. AWS uses a random v4 UUID; a recreated table
/// gets a fresh one, so a drop + recreate yields a different id even within the
/// same second, matching AWS. Generating it in the shared builder keeps both
/// backends in step. See #55.
pub fn insert_table_metadata<'a>(m: &CreateTableMetadata<'a>) -> (String, Vec<SqlParam<'a>>) {
    let table_id = uuid::Uuid::new_v4().to_string();
    let sql =
        "INSERT INTO _tables (table_name, key_schema, attribute_definitions, gsi_definitions, \
         lsi_definitions, provisioned_throughput, created_at, sse_specification, table_class, \
         deletion_protection_enabled, billing_mode, on_demand_throughput, table_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
            .to_string();
    let params = vec![
        SqlParam::text(m.table_name),
        SqlParam::text(m.key_schema),
        SqlParam::text(m.attribute_definitions),
        SqlParam::opt_text(m.gsi_definitions),
        SqlParam::opt_text(m.lsi_definitions),
        SqlParam::opt_text(m.provisioned_throughput),
        SqlParam::Integer(m.created_at),
        SqlParam::opt_text(m.sse_specification),
        SqlParam::opt_text(m.table_class),
        SqlParam::Integer(m.deletion_protection_enabled as i64),
        SqlParam::opt_text(m.billing_mode),
        SqlParam::opt_text(m.on_demand_throughput),
        SqlParam::text(table_id),
    ];
    (sql, params)
}

/// Select all metadata columns for one table.
pub fn get_table_metadata(table_name: &str) -> (String, Vec<SqlParam<'_>>) {
    (
        format!("SELECT {TABLE_METADATA_COLUMNS} FROM _tables WHERE table_name = ?1"),
        vec![SqlParam::text(table_name)],
    )
}

/// Delete a table's metadata row.
pub fn delete_table_metadata(table_name: &str) -> (String, Vec<SqlParam<'_>>) {
    (
        "DELETE FROM _tables WHERE table_name = ?1".to_string(),
        vec![SqlParam::text(table_name)],
    )
}

/// List every table name, ordered.
pub fn list_table_names() -> (String, Vec<SqlParam<'static>>) {
    (
        "SELECT table_name FROM _tables ORDER BY table_name".to_string(),
        Vec::new(),
    )
}

/// Count the metadata rows matching a table name (existence check).
pub fn table_exists(table_name: &str) -> (String, Vec<SqlParam<'_>>) {
    (
        "SELECT COUNT(*) FROM _tables WHERE table_name = ?1".to_string(),
        vec![SqlParam::text(table_name)],
    )
}

// --- Table-setting updates (`_tables`) ----------------------------------
//
// The `UpdateTable` mutations: each is a single `UPDATE _tables SET <col> = ?`.
// Shared so both backends issue identical SQL; the strings here are the single
// source of truth and are pinned byte-for-byte (with their params) by the tests
// below, so a native re-point or a wasm port cannot drift them apart.

/// Update a table's attribute and GSI definitions. The write the add/delete-GSI
/// path of `UpdateTable` issues; `gsi_definitions` is `None` (SQL `NULL`) when no
/// GSIs remain.
pub fn update_table_metadata<'a>(
    table_name: &'a str,
    attribute_definitions: &'a str,
    gsi_definitions: Option<&'a str>,
) -> (String, Vec<SqlParam<'a>>) {
    (
        "UPDATE _tables SET attribute_definitions = ?1, gsi_definitions = ?2 WHERE table_name = ?3"
            .to_string(),
        vec![
            SqlParam::text(attribute_definitions),
            SqlParam::opt_text(gsi_definitions),
            SqlParam::text(table_name),
        ],
    )
}

/// Set a table's provisioned-throughput JSON.
pub fn update_provisioned_throughput<'a>(
    table_name: &'a str,
    provisioned_throughput: &'a str,
) -> (String, Vec<SqlParam<'a>>) {
    (
        "UPDATE _tables SET provisioned_throughput = ?1 WHERE table_name = ?2".to_string(),
        vec![
            SqlParam::text(provisioned_throughput),
            SqlParam::text(table_name),
        ],
    )
}

/// Clear a table's provisioned throughput (switching to on-demand billing).
pub fn clear_provisioned_throughput(table_name: &str) -> (String, Vec<SqlParam<'_>>) {
    (
        "UPDATE _tables SET provisioned_throughput = NULL WHERE table_name = ?1".to_string(),
        vec![SqlParam::text(table_name)],
    )
}

/// Set a table's billing mode.
pub fn update_billing_mode<'a>(
    table_name: &'a str,
    billing_mode: &'a str,
) -> (String, Vec<SqlParam<'a>>) {
    (
        "UPDATE _tables SET billing_mode = ?1 WHERE table_name = ?2".to_string(),
        vec![SqlParam::text(billing_mode), SqlParam::text(table_name)],
    )
}

/// Set a table's storage class.
pub fn update_table_class<'a>(
    table_name: &'a str,
    table_class: &'a str,
) -> (String, Vec<SqlParam<'a>>) {
    (
        "UPDATE _tables SET table_class = ?1 WHERE table_name = ?2".to_string(),
        vec![SqlParam::text(table_class), SqlParam::text(table_name)],
    )
}

/// Set a table's on-demand-throughput JSON.
pub fn update_on_demand_throughput<'a>(
    table_name: &'a str,
    on_demand_throughput: &'a str,
) -> (String, Vec<SqlParam<'a>>) {
    (
        "UPDATE _tables SET on_demand_throughput = ?1 WHERE table_name = ?2".to_string(),
        vec![
            SqlParam::text(on_demand_throughput),
            SqlParam::text(table_name),
        ],
    )
}

/// Set a table's deletion-protection flag (stored as INTEGER 0/1).
pub fn update_deletion_protection(table_name: &str, enabled: bool) -> (String, Vec<SqlParam<'_>>) {
    (
        "UPDATE _tables SET deletion_protection_enabled = ?1 WHERE table_name = ?2".to_string(),
        vec![
            SqlParam::Integer(enabled as i64),
            SqlParam::text(table_name),
        ],
    )
}

// --- Data tables ---------------------------------------------------------

/// Create the per-table data table.
pub fn create_data_table(table_name: &str) -> (String, Vec<SqlParam<'static>>) {
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
    (sql, Vec::new())
}

/// Drop the per-table data table if it exists.
pub fn drop_data_table(table_name: &str) -> (String, Vec<SqlParam<'static>>) {
    (
        format!("DROP TABLE IF EXISTS \"{}\"", escape_table_name(table_name)),
        Vec::new(),
    )
}

// --- Item CRUD -----------------------------------------------------------

/// Insert-or-replace one item, preserving any existing `cached_at`.
pub fn put_item_with_hash<'a>(
    table_name: &str,
    pk: &'a str,
    sk: &'a str,
    item_json: &'a str,
    item_size: usize,
    hash_prefix: &'a str,
) -> (String, Vec<SqlParam<'a>>) {
    let escaped = escape_table_name(table_name);
    let sql = format!(
        "INSERT OR REPLACE INTO \"{escaped}\" (pk, sk, item_json, item_size, cached_at, hash_prefix) \
         VALUES (?1, ?2, ?3, ?4, \
         (SELECT cached_at FROM \"{escaped}\" WHERE pk = ?1 AND sk = ?2), ?5)"
    );
    let params = vec![
        SqlParam::text(pk),
        SqlParam::text(sk),
        SqlParam::text(item_json),
        SqlParam::Integer(item_size as i64),
        SqlParam::text(hash_prefix),
    ];
    (sql, params)
}

/// Select one item's JSON by primary key.
pub fn get_item<'a>(table_name: &str, pk: &'a str, sk: &'a str) -> (String, Vec<SqlParam<'a>>) {
    (
        format!(
            "SELECT item_json FROM \"{}\" WHERE pk = ?1 AND sk = ?2",
            escape_table_name(table_name)
        ),
        vec![SqlParam::text(pk), SqlParam::text(sk)],
    )
}

/// Delete one item by primary key.
pub fn delete_item<'a>(table_name: &str, pk: &'a str, sk: &'a str) -> (String, Vec<SqlParam<'a>>) {
    (
        format!(
            "DELETE FROM \"{}\" WHERE pk = ?1 AND sk = ?2",
            escape_table_name(table_name)
        ),
        vec![SqlParam::text(pk), SqlParam::text(sk)],
    )
}

/// Sum `item_size` across one partition key.
pub fn get_partition_size<'a>(table_name: &str, pk: &'a str) -> (String, Vec<SqlParam<'a>>) {
    (
        format!(
            "SELECT COALESCE(SUM(item_size), 0) FROM \"{}\" WHERE pk = ?1",
            escape_table_name(table_name)
        ),
        vec![SqlParam::text(pk)],
    )
}

/// Count all items in a data table.
pub fn count_items(table_name: &str) -> (String, Vec<SqlParam<'static>>) {
    (
        format!("SELECT COUNT(*) FROM \"{}\"", escape_table_name(table_name)),
        Vec::new(),
    )
}

// --- Secondary index tables (GSI/LSI) -----------------------------------

/// Create a GSI table plus its base-key index (a two-statement batch).
pub fn create_gsi_table(table_name: &str, index_name: &str) -> (String, Vec<SqlParam<'static>>) {
    let gsi = format!("{table_name}::gsi::{index_name}");
    let escaped = escape_table_name(&gsi);
    let idx = escape_table_name(&format!("{gsi}::base_key"));
    let sql = format!(
        "CREATE TABLE \"{escaped}\" (
                gsi_pk TEXT NOT NULL,
                gsi_sk TEXT NOT NULL DEFAULT '',
                table_pk TEXT NOT NULL,
                table_sk TEXT NOT NULL DEFAULT '',
                item_json TEXT NOT NULL,
                PRIMARY KEY (gsi_pk, gsi_sk, table_pk, table_sk)
            );
            CREATE INDEX IF NOT EXISTS \"{idx}\" ON \"{escaped}\" (table_pk, table_sk);"
    );
    (sql, Vec::new())
}

/// Drop a GSI table if it exists.
pub fn drop_gsi_table(table_name: &str, index_name: &str) -> (String, Vec<SqlParam<'static>>) {
    let gsi = format!("{table_name}::gsi::{index_name}");
    (
        format!("DROP TABLE IF EXISTS \"{}\"", escape_table_name(&gsi)),
        Vec::new(),
    )
}

/// Insert-or-replace statement for a GSI row, shared by single and bulk insert.
pub fn gsi_insert_sql(table_name: &str, index_name: &str) -> String {
    let gsi = format!("{table_name}::gsi::{index_name}");
    format!(
        "INSERT OR REPLACE INTO \"{}\" (gsi_pk, gsi_sk, table_pk, table_sk, item_json) VALUES (?1, ?2, ?3, ?4, ?5)",
        escape_table_name(&gsi)
    )
}

/// Bound parameters for one GSI row, matching [`gsi_insert_sql`].
pub fn gsi_insert_params<'a>(
    gsi_pk: &'a str,
    gsi_sk: &'a str,
    table_pk: &'a str,
    table_sk: &'a str,
    item_json: &'a str,
) -> Vec<SqlParam<'a>> {
    vec![
        SqlParam::text(gsi_pk),
        SqlParam::text(gsi_sk),
        SqlParam::text(table_pk),
        SqlParam::text(table_sk),
        SqlParam::text(item_json),
    ]
}

/// Delete a GSI row by base-table primary key.
pub fn delete_gsi_item<'a>(
    table_name: &str,
    index_name: &str,
    table_pk: &'a str,
    table_sk: &'a str,
) -> (String, Vec<SqlParam<'a>>) {
    let gsi = format!("{table_name}::gsi::{index_name}");
    (
        format!(
            "DELETE FROM \"{}\" WHERE table_pk = ?1 AND table_sk = ?2",
            escape_table_name(&gsi)
        ),
        vec![SqlParam::text(table_pk), SqlParam::text(table_sk)],
    )
}

/// Create an LSI table plus its base-key index (a two-statement batch).
pub fn create_lsi_table(table_name: &str, index_name: &str) -> (String, Vec<SqlParam<'static>>) {
    let lsi = format!("{table_name}::lsi::{index_name}");
    let escaped = escape_table_name(&lsi);
    let idx = escape_table_name(&format!("{lsi}::base_key"));
    let sql = format!(
        "CREATE TABLE \"{escaped}\" (
                pk TEXT NOT NULL,
                sk TEXT NOT NULL DEFAULT '',
                base_pk TEXT NOT NULL,
                base_sk TEXT NOT NULL DEFAULT '',
                item_json TEXT NOT NULL,
                PRIMARY KEY (pk, sk, base_pk, base_sk)
            );
            CREATE INDEX IF NOT EXISTS \"{idx}\" ON \"{escaped}\" (base_pk, base_sk);"
    );
    (sql, Vec::new())
}

/// Drop an LSI table if it exists.
pub fn drop_lsi_table(table_name: &str, index_name: &str) -> (String, Vec<SqlParam<'static>>) {
    let lsi = format!("{table_name}::lsi::{index_name}");
    (
        format!("DROP TABLE IF EXISTS \"{}\"", escape_table_name(&lsi)),
        Vec::new(),
    )
}

/// Insert-or-replace statement for an LSI row.
pub fn lsi_insert_sql(table_name: &str, index_name: &str) -> String {
    let lsi = format!("{table_name}::lsi::{index_name}");
    format!(
        "INSERT OR REPLACE INTO \"{}\" (pk, sk, base_pk, base_sk, item_json) VALUES (?1, ?2, ?3, ?4, ?5)",
        escape_table_name(&lsi)
    )
}

/// Bound parameters for one LSI row, matching [`lsi_insert_sql`].
pub fn lsi_insert_params<'a>(
    pk: &'a str,
    sk: &'a str,
    base_pk: &'a str,
    base_sk: &'a str,
    item_json: &'a str,
) -> Vec<SqlParam<'a>> {
    vec![
        SqlParam::text(pk),
        SqlParam::text(sk),
        SqlParam::text(base_pk),
        SqlParam::text(base_sk),
        SqlParam::text(item_json),
    ]
}

/// Delete an LSI row by base-table primary key.
pub fn delete_lsi_item<'a>(
    table_name: &str,
    index_name: &str,
    base_pk: &'a str,
    base_sk: &'a str,
) -> (String, Vec<SqlParam<'a>>) {
    let lsi = format!("{table_name}::lsi::{index_name}");
    (
        format!(
            "DELETE FROM \"{}\" WHERE base_pk = ?1 AND base_sk = ?2",
            escape_table_name(&lsi)
        ),
        vec![SqlParam::text(base_pk), SqlParam::text(base_sk)],
    )
}

/// Sum the JSON length of LSI rows for one partition key.
pub fn get_lsi_partition_size<'a>(
    table_name: &str,
    index_name: &str,
    pk: &'a str,
) -> (String, Vec<SqlParam<'a>>) {
    let lsi = format!("{table_name}::lsi::{index_name}");
    (
        format!(
            "SELECT COALESCE(SUM(length(item_json)), 0) FROM \"{}\" WHERE pk = ?1",
            escape_table_name(&lsi)
        ),
        vec![SqlParam::text(pk)],
    )
}

// --- Queries (key condition + pagination) -------------------------------

/// Query base-table items by partition key, with optional sort-key condition,
/// cursor, ordering, and limit. Returns `(pk, sk, item_json)` rows.
pub fn query_items<'a>(
    table_name: &str,
    pk: &'a str,
    params: &QueryParams<'a>,
) -> (String, Vec<SqlParam<'a>>) {
    let mut sql = format!(
        "SELECT pk, sk, item_json FROM \"{}\" WHERE pk = ?1",
        escape_table_name(table_name)
    );
    let mut param_idx = 2;
    let mut out = vec![SqlParam::text(pk)];

    if let Some(cond) = params.sk_condition {
        sql.push(' ');
        sql.push_str(cond);
        for &p in params.sk_params {
            out.push(SqlParam::text(p));
            param_idx += 1;
        }
    }

    if let Some(start_sk) = params.exclusive_start_sk {
        if params.forward {
            sql.push_str(&format!(" AND sk > ?{param_idx}"));
        } else {
            sql.push_str(&format!(" AND sk < ?{param_idx}"));
        }
        out.push(SqlParam::text(start_sk));
    }

    sql.push_str(if params.forward {
        " ORDER BY sk ASC"
    } else {
        " ORDER BY sk DESC"
    });

    if let Some(lim) = params.limit {
        sql.push_str(&format!(" LIMIT {lim}"));
    }

    (sql, out)
}

/// Query a GSI by `gsi_pk`. The sort-key condition is rewritten from `sk` to
/// `gsi_sk`, and pagination uses a composite `(gsi_sk, table_pk, table_sk)`
/// cursor so hash-only GSIs paginate correctly.
pub fn query_gsi_items<'a>(
    table_name: &str,
    index_name: &str,
    gsi_pk: &'a str,
    params: &QueryParams<'a>,
) -> (String, Vec<SqlParam<'a>>) {
    let gsi = format!("{table_name}::gsi::{index_name}");
    let mut sql = format!(
        "SELECT gsi_pk, gsi_sk, item_json FROM \"{}\" WHERE gsi_pk = ?1",
        escape_table_name(&gsi)
    );
    let mut param_idx = 2;
    let mut out = vec![SqlParam::text(gsi_pk)];

    if let Some(cond) = params.sk_condition {
        // The key-condition builder emits ` sk ` / ` sk>` forms only, so this
        // targeted rewrite is safe; threading the column name through would be
        // a larger refactor.
        let gsi_cond = cond.replace(" sk ", " gsi_sk ").replace(" sk>", " gsi_sk>");
        sql.push(' ');
        sql.push_str(&gsi_cond);
        for &p in params.sk_params {
            out.push(SqlParam::text(p));
            param_idx += 1;
        }
    }

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
        out.push(SqlParam::text(start_sk));
        out.push(SqlParam::text(start_base_pk));
        out.push(SqlParam::text(start_base_sk));
    } else if let Some(start_sk) = params.exclusive_start_sk {
        if params.forward {
            sql.push_str(&format!(" AND gsi_sk > ?{param_idx}"));
        } else {
            sql.push_str(&format!(" AND gsi_sk < ?{param_idx}"));
        }
        out.push(SqlParam::text(start_sk));
    }

    sql.push_str(if params.forward {
        " ORDER BY gsi_sk ASC, table_pk ASC, table_sk ASC"
    } else {
        " ORDER BY gsi_sk DESC, table_pk DESC, table_sk DESC"
    });

    if let Some(lim) = params.limit {
        sql.push_str(&format!(" LIMIT {lim}"));
    }

    (sql, out)
}

/// Query an LSI by base partition key, with a composite
/// `(sk, base_pk, base_sk)` pagination cursor.
pub fn query_lsi_items<'a>(
    table_name: &str,
    index_name: &str,
    pk: &'a str,
    params: &QueryParams<'a>,
) -> (String, Vec<SqlParam<'a>>) {
    let lsi = format!("{table_name}::lsi::{index_name}");
    let mut sql = format!(
        "SELECT pk, sk, item_json FROM \"{}\" WHERE pk = ?1",
        escape_table_name(&lsi)
    );
    let mut param_idx = 2;
    let mut out = vec![SqlParam::text(pk)];

    if let Some(cond) = params.sk_condition {
        sql.push(' ');
        sql.push_str(cond);
        for &p in params.sk_params {
            out.push(SqlParam::text(p));
            param_idx += 1;
        }
    }

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
        out.push(SqlParam::text(start_sk));
        out.push(SqlParam::text(start_base_pk));
        out.push(SqlParam::text(start_base_sk));
    } else if let Some(start_sk) = params.exclusive_start_sk {
        if params.forward {
            sql.push_str(&format!(" AND sk > ?{param_idx}"));
        } else {
            sql.push_str(&format!(" AND sk < ?{param_idx}"));
        }
        out.push(SqlParam::text(start_sk));
    }

    sql.push_str(if params.forward {
        " ORDER BY sk ASC, base_pk ASC, base_sk ASC"
    } else {
        " ORDER BY sk DESC, base_pk DESC, base_sk DESC"
    });

    if let Some(lim) = params.limit {
        sql.push_str(&format!(" LIMIT {lim}"));
    }

    (sql, out)
}

// --- Scans (pagination + parallel segments) -----------------------------

/// Scan base-table items. Parallel scans (segment + total) filter and order by
/// the stored `hash_prefix` column for dynalite-compatible behaviour; plain
/// scans order by `(pk, sk)`.
pub fn scan_items<'a>(table_name: &str, params: &ScanParams<'a>) -> (String, Vec<SqlParam<'a>>) {
    let escaped = escape_table_name(table_name);
    let mut sql = format!("SELECT pk, sk, item_json FROM \"{escaped}\"");
    let mut out: Vec<SqlParam<'a>> = Vec::new();
    let mut where_clauses: Vec<String> = Vec::new();
    let mut param_idx = 1;

    let is_parallel = params.segment.is_some() && params.total_segments.is_some();

    if let (Some(start_pk), Some(start_sk)) = (params.exclusive_start_pk, params.exclusive_start_sk)
    {
        if is_parallel {
            where_clauses.push(format!(
                "(hash_prefix, pk, sk) > ((SELECT hash_prefix FROM \"{escaped}\" WHERE pk = ?{} AND sk = ?{} LIMIT 1), ?{}, ?{})",
                param_idx,
                param_idx + 1,
                param_idx,
                param_idx + 1
            ));
        } else {
            where_clauses.push(format!("(pk, sk) > (?{}, ?{})", param_idx, param_idx + 1));
        }
        out.push(SqlParam::text(start_pk));
        out.push(SqlParam::text(start_sk));
        param_idx += 2;
    }

    if let (Some(seg), Some(total)) = (params.segment, params.total_segments) {
        let start_bucket = ceiling_div(HASH_BUCKETS * seg, total);
        let end_bucket = ceiling_div(HASH_BUCKETS * (seg + 1), total) - 1;
        where_clauses.push(format!(
            "substr(hash_prefix, 1, 3) >= ?{} AND substr(hash_prefix, 1, 3) <= ?{}",
            param_idx,
            param_idx + 1
        ));
        out.push(SqlParam::text(format!("{start_bucket:03x}")));
        out.push(SqlParam::text(format!("{end_bucket:03x}")));
    }

    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }

    if is_parallel {
        sql.push_str(" ORDER BY hash_prefix ASC, pk ASC, sk ASC");
    } else {
        sql.push_str(" ORDER BY pk ASC, sk ASC");
    }

    if let Some(lim) = params.limit {
        sql.push_str(&format!(" LIMIT {lim}"));
    }

    (sql, out)
}

/// Scan a GSI. Parallel scans hash the base-table key in SQL via
/// `fnv1a_hash(table_pk)` (GSI tables carry no stored hash_prefix).
pub fn scan_gsi_items<'a>(
    table_name: &str,
    index_name: &str,
    params: &ScanParams<'a>,
) -> (String, Vec<SqlParam<'a>>) {
    let gsi = format!("{table_name}::gsi::{index_name}");
    let mut sql = format!(
        "SELECT gsi_pk, gsi_sk, item_json FROM \"{}\"",
        escape_table_name(&gsi)
    );
    let mut out: Vec<SqlParam<'a>> = Vec::new();
    let mut where_clauses: Vec<String> = Vec::new();
    let mut param_idx = 1;

    if let (Some(start_pk), Some(start_sk)) = (params.exclusive_start_pk, params.exclusive_start_sk)
    {
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
            out.push(SqlParam::text(start_pk));
            out.push(SqlParam::text(start_sk));
            out.push(SqlParam::text(base_pk));
            out.push(SqlParam::text(base_sk));
            param_idx += 4;
        } else {
            where_clauses.push(format!(
                "(gsi_pk, gsi_sk) > (?{}, ?{})",
                param_idx,
                param_idx + 1
            ));
            out.push(SqlParam::text(start_pk));
            out.push(SqlParam::text(start_sk));
            param_idx += 2;
        }
    }

    if let (Some(seg), Some(total)) = (params.segment, params.total_segments) {
        where_clauses.push(format!(
            "(fnv1a_hash(table_pk) % ?{}) = ?{}",
            param_idx,
            param_idx + 1
        ));
        out.push(SqlParam::Integer(total as i64));
        out.push(SqlParam::Integer(seg as i64));
    }

    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }

    sql.push_str(" ORDER BY gsi_pk ASC, gsi_sk ASC, table_pk ASC, table_sk ASC");

    if let Some(lim) = params.limit {
        sql.push_str(&format!(" LIMIT {lim}"));
    }

    (sql, out)
}

/// Scan an LSI. Parallel scans hash the base-table key in SQL via
/// `fnv1a_hash(base_pk)`.
pub fn scan_lsi_items<'a>(
    table_name: &str,
    index_name: &str,
    params: &ScanParams<'a>,
) -> (String, Vec<SqlParam<'a>>) {
    let lsi = format!("{table_name}::lsi::{index_name}");
    let mut sql = format!(
        "SELECT pk, sk, item_json FROM \"{}\"",
        escape_table_name(&lsi)
    );
    let mut out: Vec<SqlParam<'a>> = Vec::new();
    let mut where_clauses: Vec<String> = Vec::new();
    let mut param_idx = 1;

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
        out.push(SqlParam::text(start_pk));
        out.push(SqlParam::text(start_sk));
        out.push(SqlParam::text(start_base_pk));
        out.push(SqlParam::text(start_base_sk));
        param_idx += 4;
    } else if let (Some(start_pk), Some(start_sk)) =
        (params.exclusive_start_pk, params.exclusive_start_sk)
    {
        where_clauses.push(format!("(pk, sk) > (?{}, ?{})", param_idx, param_idx + 1));
        out.push(SqlParam::text(start_pk));
        out.push(SqlParam::text(start_sk));
        param_idx += 2;
    }

    if let (Some(seg), Some(total)) = (params.segment, params.total_segments) {
        where_clauses.push(format!(
            "(fnv1a_hash(base_pk) % ?{}) = ?{}",
            param_idx,
            param_idx + 1
        ));
        out.push(SqlParam::Integer(total as i64));
        out.push(SqlParam::Integer(seg as i64));
    }

    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }

    sql.push_str(" ORDER BY pk ASC, sk ASC, base_pk ASC, base_sk ASC");

    if let Some(lim) = params.limit {
        sql.push_str(&format!(" LIMIT {lim}"));
    }

    (sql, out)
}

#[cfg(all(test, any(feature = "native-sqlite", feature = "_has-encryption")))]
mod tests {
    use super::*;

    #[test]
    fn escape_table_name_doubles_quotes() {
        assert_eq!(escape_table_name("plain"), "plain");
        assert_eq!(escape_table_name(r#"a"b"#), r#"a""b"#);
    }

    #[test]
    fn get_item_builds_keyed_select() {
        let (sql, params) = get_item("Orders", "pk1", "sk1");
        assert_eq!(
            sql,
            "SELECT item_json FROM \"Orders\" WHERE pk = ?1 AND sk = ?2"
        );
        assert_eq!(params, vec![SqlParam::text("pk1"), SqlParam::text("sk1")]);
    }

    #[test]
    fn put_item_with_hash_binds_five_params_in_order() {
        let (sql, params) = put_item_with_hash("T", "p", "s", "{}", 42, "ab");
        assert!(sql.contains("INSERT OR REPLACE INTO \"T\""));
        assert!(sql.contains("(SELECT cached_at FROM \"T\" WHERE pk = ?1 AND sk = ?2)"));
        assert_eq!(
            params,
            vec![
                SqlParam::text("p"),
                SqlParam::text("s"),
                SqlParam::text("{}"),
                SqlParam::Integer(42),
                SqlParam::text("ab"),
            ]
        );
    }

    #[test]
    fn insert_table_metadata_maps_optionals_to_null() {
        let m = CreateTableMetadata {
            table_name: "T",
            key_schema: "[]",
            attribute_definitions: "[]",
            created_at: 7,
            deletion_protection_enabled: true,
            ..Default::default()
        };
        let (sql, params) = insert_table_metadata(&m);
        assert!(sql.starts_with("INSERT INTO _tables"));
        assert_eq!(params.len(), 13);
        assert_eq!(params[0], SqlParam::text("T"));
        assert_eq!(params[3], SqlParam::Null); // gsi_definitions: None
        assert_eq!(params[6], SqlParam::Integer(7)); // created_at
        assert_eq!(params[9], SqlParam::Integer(1)); // deletion_protection_enabled
        assert_eq!(params[11], SqlParam::Null); // on_demand_throughput: None
        // table_id is a freshly generated v4 UUID, so assert its shape, not value.
        match &params[12] {
            SqlParam::Text(id) => assert_eq!(id.len(), 36),
            other => panic!("table_id should be bound as text, got {other:?}"),
        }
    }

    #[test]
    fn sqlparam_round_trips_through_rusqlite() {
        // The native binding path: a SqlParam bound into a statement reads back
        // as the same value. This is the half of the conversion testable
        // without a browser; the JS-value half lives in the wasm backend.
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE t (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)",
            [],
        )
        .unwrap();
        let params = [
            SqlParam::text("hello"),
            SqlParam::Integer(-9),
            SqlParam::Real(1.5),
            SqlParam::Blob(vec![1, 2, 3].into()),
            SqlParam::Null,
        ];
        conn.execute(
            "INSERT INTO t (a, b, c, d, e) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params_from_iter(params.iter()),
        )
        .unwrap();
        let (a, b, c, d, e): (String, i64, f64, Vec<u8>, Option<String>) = conn
            .query_row("SELECT a, b, c, d, e FROM t", [], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .unwrap();
        assert_eq!(a, "hello");
        assert_eq!(b, -9);
        assert_eq!(c, 1.5);
        assert_eq!(d, vec![1, 2, 3]);
        assert_eq!(e, None);
    }

    #[test]
    fn query_items_combines_sk_condition_cursor_and_limit() {
        let params = QueryParams {
            sk_condition: Some("AND sk BETWEEN ?2 AND ?3"),
            sk_params: &["a", "b"],
            forward: true,
            limit: Some(5),
            exclusive_start_sk: Some("c"),
            ..Default::default()
        };
        let (sql, out) = query_items("T", "u1", &params);
        assert_eq!(
            sql,
            "SELECT pk, sk, item_json FROM \"T\" WHERE pk = ?1 AND sk BETWEEN ?2 AND ?3 AND sk > ?4 ORDER BY sk ASC LIMIT 5"
        );
        assert_eq!(
            out,
            vec![
                SqlParam::text("u1"),
                SqlParam::text("a"),
                SqlParam::text("b"),
                SqlParam::text("c"),
            ]
        );
    }

    #[test]
    fn query_items_reverse_uses_descending_cursor_and_order() {
        let params = QueryParams {
            forward: false,
            exclusive_start_sk: Some("c"),
            ..Default::default()
        };
        let (sql, out) = query_items("T", "u1", &params);
        assert_eq!(
            sql,
            "SELECT pk, sk, item_json FROM \"T\" WHERE pk = ?1 AND sk < ?2 ORDER BY sk DESC"
        );
        assert_eq!(out, vec![SqlParam::text("u1"), SqlParam::text("c")]);
    }

    #[test]
    fn query_gsi_items_rewrites_sk_to_gsi_sk_and_paginates_composite() {
        let params = QueryParams {
            sk_condition: Some("AND sk > ?2"),
            sk_params: &["m"],
            forward: true,
            exclusive_start_sk: Some("s"),
            exclusive_start_base_pk: Some("bp"),
            exclusive_start_base_sk: Some("bs"),
            ..Default::default()
        };
        let (sql, out) = query_gsi_items("Orders", "byStatus", "OPEN", &params);
        assert_eq!(
            sql,
            "SELECT gsi_pk, gsi_sk, item_json FROM \"Orders::gsi::byStatus\" WHERE gsi_pk = ?1 AND gsi_sk > ?2 AND (gsi_sk, table_pk, table_sk) > (?3, ?4, ?5) ORDER BY gsi_sk ASC, table_pk ASC, table_sk ASC"
        );
        assert_eq!(
            out,
            vec![
                SqlParam::text("OPEN"),
                SqlParam::text("m"),
                SqlParam::text("s"),
                SqlParam::text("bp"),
                SqlParam::text("bs"),
            ]
        );
    }

    #[test]
    fn query_lsi_items_paginates_composite_cursor() {
        let params = QueryParams {
            forward: true,
            limit: Some(3),
            exclusive_start_sk: Some("s"),
            exclusive_start_base_pk: Some("bp"),
            exclusive_start_base_sk: Some("bs"),
            ..Default::default()
        };
        let (sql, out) = query_lsi_items("T", "lsi1", "p1", &params);
        assert_eq!(
            sql,
            "SELECT pk, sk, item_json FROM \"T::lsi::lsi1\" WHERE pk = ?1 AND (sk, base_pk, base_sk) > (?2, ?3, ?4) ORDER BY sk ASC, base_pk ASC, base_sk ASC LIMIT 3"
        );
        assert_eq!(
            out,
            vec![
                SqlParam::text("p1"),
                SqlParam::text("s"),
                SqlParam::text("bp"),
                SqlParam::text("bs"),
            ]
        );
    }

    #[test]
    fn scan_items_parallel_segment_filters_by_hash_bucket() {
        let (seg, total) = (1u32, 4u32);
        let params = ScanParams {
            segment: Some(seg),
            total_segments: Some(total),
            ..Default::default()
        };
        let (sql, out) = scan_items("T", &params);
        assert_eq!(
            sql,
            "SELECT pk, sk, item_json FROM \"T\" WHERE substr(hash_prefix, 1, 3) >= ?1 AND substr(hash_prefix, 1, 3) <= ?2 ORDER BY hash_prefix ASC, pk ASC, sk ASC"
        );
        let start = ceiling_div(HASH_BUCKETS * seg, total);
        let end = ceiling_div(HASH_BUCKETS * (seg + 1), total) - 1;
        assert_eq!(
            out,
            vec![
                SqlParam::text(format!("{start:03x}")),
                SqlParam::text(format!("{end:03x}")),
            ]
        );
    }

    #[test]
    fn scan_items_parallel_with_cursor_uses_hash_prefix_subquery() {
        let (seg, total) = (0u32, 2u32);
        let params = ScanParams {
            exclusive_start_pk: Some("p"),
            exclusive_start_sk: Some("s"),
            segment: Some(seg),
            total_segments: Some(total),
            ..Default::default()
        };
        let (sql, out) = scan_items("T", &params);
        assert_eq!(
            sql,
            "SELECT pk, sk, item_json FROM \"T\" WHERE (hash_prefix, pk, sk) > ((SELECT hash_prefix FROM \"T\" WHERE pk = ?1 AND sk = ?2 LIMIT 1), ?1, ?2) AND substr(hash_prefix, 1, 3) >= ?3 AND substr(hash_prefix, 1, 3) <= ?4 ORDER BY hash_prefix ASC, pk ASC, sk ASC"
        );
        let start = ceiling_div(HASH_BUCKETS * seg, total);
        let end = ceiling_div(HASH_BUCKETS * (seg + 1), total) - 1;
        assert_eq!(
            out,
            vec![
                SqlParam::text("p"),
                SqlParam::text("s"),
                SqlParam::text(format!("{start:03x}")),
                SqlParam::text(format!("{end:03x}")),
            ]
        );
    }

    #[test]
    fn scan_gsi_items_full_cursor_and_parallel_segment() {
        let params = ScanParams {
            exclusive_start_pk: Some("gp"),
            exclusive_start_sk: Some("gs"),
            exclusive_start_base_pk: Some("bp"),
            exclusive_start_base_sk: Some("bs"),
            segment: Some(2),
            total_segments: Some(5),
            ..Default::default()
        };
        let (sql, out) = scan_gsi_items("O", "byX", &params);
        assert_eq!(
            sql,
            "SELECT gsi_pk, gsi_sk, item_json FROM \"O::gsi::byX\" WHERE (gsi_pk, gsi_sk, table_pk, table_sk) > (?1, ?2, ?3, ?4) AND (fnv1a_hash(table_pk) % ?5) = ?6 ORDER BY gsi_pk ASC, gsi_sk ASC, table_pk ASC, table_sk ASC"
        );
        assert_eq!(
            out,
            vec![
                SqlParam::text("gp"),
                SqlParam::text("gs"),
                SqlParam::text("bp"),
                SqlParam::text("bs"),
                SqlParam::Integer(5),
                SqlParam::Integer(2),
            ]
        );
    }

    #[test]
    fn scan_lsi_items_full_cursor_and_parallel_segment() {
        let params = ScanParams {
            exclusive_start_pk: Some("p"),
            exclusive_start_sk: Some("s"),
            exclusive_start_base_pk: Some("bp"),
            exclusive_start_base_sk: Some("bs"),
            segment: Some(1),
            total_segments: Some(3),
            ..Default::default()
        };
        let (sql, out) = scan_lsi_items("T", "lsi1", &params);
        assert_eq!(
            sql,
            "SELECT pk, sk, item_json FROM \"T::lsi::lsi1\" WHERE (pk, sk, base_pk, base_sk) > (?1, ?2, ?3, ?4) AND (fnv1a_hash(base_pk) % ?5) = ?6 ORDER BY pk ASC, sk ASC, base_pk ASC, base_sk ASC"
        );
        assert_eq!(
            out,
            vec![
                SqlParam::text("p"),
                SqlParam::text("s"),
                SqlParam::text("bp"),
                SqlParam::text("bs"),
                SqlParam::Integer(3),
                SqlParam::Integer(1),
            ]
        );
    }

    // The conformance gate's fast half. Pin each table-setting update builder's
    // (SQL, params) pair byte-for-byte to exactly what the native backend issued
    // before these builders existed. Pinning the params alongside the string,
    // not the string alone, catches a reorder or reshape of the bound params
    // that a string-only check would miss, so the fast gate stands on its own
    // and does not lean on the slower conformance run to find drift.
    #[test]
    fn table_setting_update_builders_are_pinned() {
        assert_eq!(
            update_table_metadata("T", "{attrs}", Some("{gsis}")),
            (
                "UPDATE _tables SET attribute_definitions = ?1, gsi_definitions = ?2 WHERE table_name = ?3".to_string(),
                vec![SqlParam::text("{attrs}"), SqlParam::text("{gsis}"), SqlParam::text("T")],
            )
        );
        assert_eq!(
            update_table_metadata("T", "{attrs}", None),
            (
                "UPDATE _tables SET attribute_definitions = ?1, gsi_definitions = ?2 WHERE table_name = ?3".to_string(),
                vec![SqlParam::text("{attrs}"), SqlParam::Null, SqlParam::text("T")],
            )
        );
        assert_eq!(
            update_provisioned_throughput("T", "{pt}"),
            (
                "UPDATE _tables SET provisioned_throughput = ?1 WHERE table_name = ?2".to_string(),
                vec![SqlParam::text("{pt}"), SqlParam::text("T")],
            )
        );
        assert_eq!(
            clear_provisioned_throughput("T"),
            (
                "UPDATE _tables SET provisioned_throughput = NULL WHERE table_name = ?1"
                    .to_string(),
                vec![SqlParam::text("T")],
            )
        );
        assert_eq!(
            update_billing_mode("T", "PAY_PER_REQUEST"),
            (
                "UPDATE _tables SET billing_mode = ?1 WHERE table_name = ?2".to_string(),
                vec![SqlParam::text("PAY_PER_REQUEST"), SqlParam::text("T")],
            )
        );
        assert_eq!(
            update_table_class("T", "STANDARD_INFREQUENT_ACCESS"),
            (
                "UPDATE _tables SET table_class = ?1 WHERE table_name = ?2".to_string(),
                vec![
                    SqlParam::text("STANDARD_INFREQUENT_ACCESS"),
                    SqlParam::text("T")
                ],
            )
        );
        assert_eq!(
            update_on_demand_throughput("T", "{odt}"),
            (
                "UPDATE _tables SET on_demand_throughput = ?1 WHERE table_name = ?2".to_string(),
                vec![SqlParam::text("{odt}"), SqlParam::text("T")],
            )
        );
        assert_eq!(
            update_deletion_protection("T", true),
            (
                "UPDATE _tables SET deletion_protection_enabled = ?1 WHERE table_name = ?2"
                    .to_string(),
                vec![SqlParam::Integer(1), SqlParam::text("T")],
            )
        );
        assert_eq!(
            update_deletion_protection("T", false),
            (
                "UPDATE _tables SET deletion_protection_enabled = ?1 WHERE table_name = ?2"
                    .to_string(),
                vec![SqlParam::Integer(0), SqlParam::text("T")],
            )
        );
    }
}
