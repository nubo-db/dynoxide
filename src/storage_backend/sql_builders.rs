//! Backend-neutral SQL builders: the single source of truth for the SQL that
//! every storage backend issues.
//!
//! Each builder returns the SQL string plus a `Vec<SqlParam>` of bound
//! parameters. The native rusqlite backend binds `SqlParam` through its
//! [`rusqlite::ToSql`] impl; the wasm wa-sqlite backend converts each
//! `SqlParam` to a JS value for the bridge. Keeping the SQL here, rather than
//! inside a backend, guarantees both backends issue identical statements: a
//! query fixed on one is fixed for both, so the two backends cannot silently
//! drift apart.
//!
//! Builders interpolate table names directly (SQLite cannot bind an
//! identifier) via [`escape_table_name`] and bind values positionally with
//! `?N` placeholders.

use crate::storage::CreateTableMetadata;

/// One bound SQL parameter, covering the SQLite value universe and nothing
/// more.
///
/// Owned rather than borrowed so builder signatures stay lifetime-free; the
/// per-call allocation is negligible against the statement execution itself.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParam {
    /// A UTF-8 text value, bound as SQLite `TEXT`.
    Text(String),
    /// A 64-bit signed integer, bound as SQLite `INTEGER`.
    Integer(i64),
    /// A double, bound as SQLite `REAL`.
    Real(f64),
    /// A byte string, bound as SQLite `BLOB`.
    Blob(Vec<u8>),
    /// SQL `NULL`.
    Null,
}

impl SqlParam {
    /// Owned text parameter.
    pub fn text(s: impl Into<String>) -> Self {
        SqlParam::Text(s.into())
    }

    /// `TEXT` when `Some`, SQL `NULL` when `None`.
    pub fn opt_text(s: Option<&str>) -> Self {
        match s {
            Some(v) => SqlParam::Text(v.to_string()),
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
impl rusqlite::ToSql for SqlParam {
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

/// The standard SELECT column list for `_tables` queries, in the order
/// the backends' metadata row-mappers expect.
pub(crate) const TABLE_METADATA_COLUMNS: &str = "table_name, key_schema, attribute_definitions, gsi_definitions, \
     lsi_definitions, stream_enabled, stream_view_type, stream_label, ttl_attribute, ttl_enabled, \
     created_at, table_status, billing_mode, provisioned_throughput, \
     sse_specification, table_class, deletion_protection_enabled";

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
pub fn insert_table_metadata(m: &CreateTableMetadata) -> (String, Vec<SqlParam>) {
    let sql = "INSERT INTO _tables (table_name, key_schema, attribute_definitions, gsi_definitions, \
         lsi_definitions, provisioned_throughput, created_at, sse_specification, table_class, \
         deletion_protection_enabled, billing_mode) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
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
    ];
    (sql, params)
}

/// Select all metadata columns for one table.
pub fn get_table_metadata(table_name: &str) -> (String, Vec<SqlParam>) {
    (
        format!("SELECT {TABLE_METADATA_COLUMNS} FROM _tables WHERE table_name = ?1"),
        vec![SqlParam::text(table_name)],
    )
}

/// Delete a table's metadata row.
pub fn delete_table_metadata(table_name: &str) -> (String, Vec<SqlParam>) {
    (
        "DELETE FROM _tables WHERE table_name = ?1".to_string(),
        vec![SqlParam::text(table_name)],
    )
}

/// List every table name, ordered.
pub fn list_table_names() -> (String, Vec<SqlParam>) {
    (
        "SELECT table_name FROM _tables ORDER BY table_name".to_string(),
        Vec::new(),
    )
}

/// Count the metadata rows matching a table name (existence check).
pub fn table_exists(table_name: &str) -> (String, Vec<SqlParam>) {
    (
        "SELECT COUNT(*) FROM _tables WHERE table_name = ?1".to_string(),
        vec![SqlParam::text(table_name)],
    )
}

// --- Data tables ---------------------------------------------------------

/// Create the per-table data table.
pub fn create_data_table(table_name: &str) -> (String, Vec<SqlParam>) {
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
pub fn drop_data_table(table_name: &str) -> (String, Vec<SqlParam>) {
    (
        format!("DROP TABLE IF EXISTS \"{}\"", escape_table_name(table_name)),
        Vec::new(),
    )
}

// --- Item CRUD -----------------------------------------------------------

/// Insert-or-replace one item, preserving any existing `cached_at`.
pub fn put_item_with_hash(
    table_name: &str,
    pk: &str,
    sk: &str,
    item_json: &str,
    item_size: usize,
    hash_prefix: &str,
) -> (String, Vec<SqlParam>) {
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
pub fn get_item(table_name: &str, pk: &str, sk: &str) -> (String, Vec<SqlParam>) {
    (
        format!(
            "SELECT item_json FROM \"{}\" WHERE pk = ?1 AND sk = ?2",
            escape_table_name(table_name)
        ),
        vec![SqlParam::text(pk), SqlParam::text(sk)],
    )
}

/// Delete one item by primary key.
pub fn delete_item(table_name: &str, pk: &str, sk: &str) -> (String, Vec<SqlParam>) {
    (
        format!(
            "DELETE FROM \"{}\" WHERE pk = ?1 AND sk = ?2",
            escape_table_name(table_name)
        ),
        vec![SqlParam::text(pk), SqlParam::text(sk)],
    )
}

/// Sum `item_size` across one partition key.
pub fn get_partition_size(table_name: &str, pk: &str) -> (String, Vec<SqlParam>) {
    (
        format!(
            "SELECT COALESCE(SUM(item_size), 0) FROM \"{}\" WHERE pk = ?1",
            escape_table_name(table_name)
        ),
        vec![SqlParam::text(pk)],
    )
}

/// Count all items in a data table.
pub fn count_items(table_name: &str) -> (String, Vec<SqlParam>) {
    (
        format!("SELECT COUNT(*) FROM \"{}\"", escape_table_name(table_name)),
        Vec::new(),
    )
}

// --- Secondary index tables (GSI/LSI) -----------------------------------

/// Create a GSI table plus its base-key index (a two-statement batch).
pub fn create_gsi_table(table_name: &str, index_name: &str) -> (String, Vec<SqlParam>) {
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
pub fn drop_gsi_table(table_name: &str, index_name: &str) -> (String, Vec<SqlParam>) {
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
pub fn gsi_insert_params(
    gsi_pk: &str,
    gsi_sk: &str,
    table_pk: &str,
    table_sk: &str,
    item_json: &str,
) -> Vec<SqlParam> {
    vec![
        SqlParam::text(gsi_pk),
        SqlParam::text(gsi_sk),
        SqlParam::text(table_pk),
        SqlParam::text(table_sk),
        SqlParam::text(item_json),
    ]
}

/// Delete a GSI row by base-table primary key.
pub fn delete_gsi_item(
    table_name: &str,
    index_name: &str,
    table_pk: &str,
    table_sk: &str,
) -> (String, Vec<SqlParam>) {
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
pub fn create_lsi_table(table_name: &str, index_name: &str) -> (String, Vec<SqlParam>) {
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
pub fn drop_lsi_table(table_name: &str, index_name: &str) -> (String, Vec<SqlParam>) {
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
pub fn lsi_insert_params(
    pk: &str,
    sk: &str,
    base_pk: &str,
    base_sk: &str,
    item_json: &str,
) -> Vec<SqlParam> {
    vec![
        SqlParam::text(pk),
        SqlParam::text(sk),
        SqlParam::text(base_pk),
        SqlParam::text(base_sk),
        SqlParam::text(item_json),
    ]
}

/// Delete an LSI row by base-table primary key.
pub fn delete_lsi_item(
    table_name: &str,
    index_name: &str,
    base_pk: &str,
    base_sk: &str,
) -> (String, Vec<SqlParam>) {
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
pub fn get_lsi_partition_size(
    table_name: &str,
    index_name: &str,
    pk: &str,
) -> (String, Vec<SqlParam>) {
    let lsi = format!("{table_name}::lsi::{index_name}");
    (
        format!(
            "SELECT COALESCE(SUM(length(item_json)), 0) FROM \"{}\" WHERE pk = ?1",
            escape_table_name(&lsi)
        ),
        vec![SqlParam::text(pk)],
    )
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
        assert_eq!(
            params,
            vec![SqlParam::text("pk1"), SqlParam::text("sk1")]
        );
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
        assert_eq!(params.len(), 11);
        assert_eq!(params[0], SqlParam::text("T"));
        assert_eq!(params[3], SqlParam::Null); // gsi_definitions: None
        assert_eq!(params[6], SqlParam::Integer(7)); // created_at
        assert_eq!(params[9], SqlParam::Integer(1)); // deletion_protection_enabled
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
        let params = vec![
            SqlParam::text("hello"),
            SqlParam::Integer(-9),
            SqlParam::Real(1.5),
            SqlParam::Blob(vec![1, 2, 3]),
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
}
