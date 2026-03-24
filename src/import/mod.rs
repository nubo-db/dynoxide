//! Import CLI for DynamoDB Export data.
//!
//! Parses DynamoDB Export JSON Lines files, optionally applies anonymisation
//! rules, and imports the data into a Dynoxide SQLite database.
//!
//! ## Pipeline
//!
//! 1. Parse TOML config (validate all rules upfront)
//! 2. Source table schemas from `--schema <file>`
//! 3. Create tables in output SQLite database
//! 4. For each table: read JSON Lines → parse → anonymise → batch insert
//! 5. VACUUM (compact the SQLite file)
//! 6. Optionally compress with zstd

pub(crate) mod anonymise;
pub(crate) mod config;
pub(crate) mod consistency;
pub(crate) mod parser;
pub(crate) mod schema;

use crate::{Database, ImportOptions};
use consistency::ConsistencyMap;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashSet;
use std::path::Path;

/// Errors from the import pipeline.
#[derive(Debug)]
pub enum ImportError {
    /// Configuration or validation error (e.g., invalid TOML, missing schema).
    Config(String),
    /// I/O or parsing error during data import.
    Data(String),
    /// Database error during table creation or item insertion.
    Database(String),
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportError::Config(msg) => write!(f, "{msg}"),
            ImportError::Data(msg) => write!(f, "{msg}"),
            ImportError::Database(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<String> for ImportError {
    fn from(s: String) -> Self {
        ImportError::Data(s)
    }
}

/// Configuration for the import operation.
pub struct ImportCommand {
    /// Source directory containing export files.
    pub source: std::path::PathBuf,
    /// Output SQLite database path (required for file-based import, None for in-memory).
    pub output: Option<std::path::PathBuf>,
    /// Schema file path (DescribeTable JSON format).
    pub schema: std::path::PathBuf,
    /// Optional anonymisation rules TOML file.
    pub rules: Option<std::path::PathBuf>,
    /// Optional table name filter (comma-separated).
    pub tables: Option<Vec<String>>,
    /// Optional zstd compression of output (only valid with file output).
    pub compress: bool,
    /// Overwrite existing output file without prompting.
    pub force: bool,
    /// Continue importing when a batch fails (default: fail-fast).
    /// When true, batch errors are recorded as warnings and import continues.
    /// When false (default), the first batch error aborts the import.
    pub continue_on_error: bool,
}

/// Result of an import operation.
#[derive(Debug)]
pub struct ImportSummary {
    /// Per-table import statistics.
    pub tables: Vec<TableImportResult>,
    /// Total items imported across all tables.
    pub total_items: usize,
    /// Total bytes imported.
    pub total_bytes: usize,
    /// Total lines skipped due to parse errors.
    pub total_skipped: usize,
    /// Warnings generated during import.
    pub warnings: Vec<String>,
    /// Output file path (may differ from input if compressed). None for in-memory imports.
    pub output_path: Option<std::path::PathBuf>,
}

/// Per-table import result.
#[derive(Debug)]
pub struct TableImportResult {
    pub table_name: String,
    pub items_imported: usize,
    pub bytes_imported: usize,
    pub lines_skipped: usize,
}

/// Execute the import pipeline into a caller-provided database.
///
/// This is the core import logic — database-agnostic. The caller is
/// responsible for creating the database and any post-import steps
/// (VACUUM, compression). This makes import usable with both file-backed
/// and in-memory databases.
pub fn run_into(db: &Database, cmd: ImportCommand) -> Result<ImportSummary, ImportError> {
    // 1. Load and validate anonymisation rules (if provided)
    let (rules, consistency_config) = if let Some(ref rules_path) = cmd.rules {
        let (rules, consistency) =
            config::load_and_validate(rules_path).map_err(ImportError::Config)?;
        eprintln!(
            "Loaded {} anonymisation rules from {}",
            rules.len(),
            rules_path.display()
        );
        (rules, consistency)
    } else {
        (Vec::new(), None)
    };

    let consistency_fields: std::collections::HashSet<String> = consistency_config
        .as_ref()
        .map(|c| c.fields.iter().cloned().collect())
        .unwrap_or_default();
    let mut consistency_map = ConsistencyMap::new();

    // 2. Load table schemas (returns both parsed schemas and raw JSON)
    let (schemas, schema_json) = schema::load_schemas(&cmd.schema)?;
    eprintln!(
        "Loaded {} table schemas from {}",
        schemas.len(),
        cmd.schema.display()
    );

    // 3. Discover export files
    let table_filter = cmd.tables.as_deref();
    let export_files = parser::discover_export_files(&cmd.source, table_filter)?;

    if export_files.is_empty() {
        return Err(ImportError::Config(format!(
            "No export files found in {}. Expected DynamoDB Export directory structure \
             (<dir>/<TableName>/data/*.json.gz) or flat directory (<dir>/*.json[.gz]).",
            cmd.source.display()
        )));
    }

    // Build a schema lookup map
    let schema_map: std::collections::HashMap<&str, &schema::TableSchema> =
        schemas.iter().map(|s| (s.table_name.as_str(), s)).collect();

    // 4. Create tables from schemas
    for (table_name, _) in &export_files {
        if !schema_map.contains_key(table_name.as_str()) {
            return Err(ImportError::Config(format!(
                "No schema found for table '{}'. Available schemas: {}",
                table_name,
                schemas
                    .iter()
                    .map(|s| s.table_name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        // Find the matching schema JSON and deserialize into a fresh CreateTableRequest
        let table_json = find_table_json(&schema_json, table_name)
            .ok_or_else(|| format!("Schema JSON not found for table '{table_name}'"))?;

        let create_request: crate::actions::create_table::CreateTableRequest =
            serde_json::from_value(table_json)
                .map_err(|e| format!("Failed to deserialize schema for '{}': {e}", table_name))?;

        db.create_table(create_request)
            .map_err(|e| format!("Failed to create table '{}': {e}", table_name))?;
    }

    // 5. Enable bulk-loading PRAGMAs (safe: fresh DB, can re-import on crash)
    db.enable_bulk_loading()
        .map_err(|e| format!("Failed to enable bulk loading: {e}"))?;

    // 6. Import data for each table
    let mut summary = ImportSummary {
        tables: Vec::new(),
        total_items: 0,
        total_bytes: 0,
        total_skipped: 0,
        warnings: Vec::new(),
        output_path: cmd.output.clone(),
    };

    let mut seen_warnings: HashSet<String> = HashSet::new();

    for (table_name, files) in &export_files {
        let table_schema = schema_map.get(table_name.as_str()).unwrap();
        let key_attrs = extract_key_attrs(&table_schema.create_request);

        let file_count = files.len();
        eprintln!("Importing table '{}' ({} files)...", table_name, file_count);

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        pb.set_message(format!("{}: parsing...", table_name));

        let mut table_items = 0usize;
        let mut table_bytes = 0usize;
        let mut table_skipped = 0usize;
        let mut batch_error: Option<String> = None;

        const BATCH_SIZE: usize = 10_000;

        for file_path in files {
            let mut batch: Vec<crate::types::Item> = Vec::with_capacity(BATCH_SIZE);

            let stats = parser::parse_export_file_streaming(file_path, |mut item| {
                // Skip processing if we've already hit a fatal batch error
                if batch_error.is_some() {
                    return;
                }

                // Apply anonymisation rules
                if !rules.is_empty() {
                    let warnings = anonymise::apply_rules(
                        &mut item,
                        &rules,
                        &mut consistency_map,
                        &consistency_fields,
                        &key_attrs,
                    );
                    for w in warnings {
                        if !seen_warnings.contains(&w) {
                            seen_warnings.insert(w.clone());
                            summary.warnings.push(w);
                        }
                    }
                }
                batch.push(item);

                // Flush batch when full
                if batch.len() >= BATCH_SIZE {
                    let chunk = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                    match db.import_items_fresh(table_name, chunk, ImportOptions::default()) {
                        Ok(result) => {
                            table_items += result.items_imported;
                            table_bytes += result.bytes_imported;
                        }
                        Err(e) => {
                            let msg = format!("Batch import error for '{}': {e}", table_name);
                            if cmd.continue_on_error {
                                summary.warnings.push(msg);
                            } else {
                                batch_error = Some(msg);
                                return;
                            }
                        }
                    }
                    pb.set_message(format!("{}: {} items", table_name, table_items));
                    pb.tick();
                }
            })?;

            // Propagate batch error after the streaming callback completes
            if let Some(err) = batch_error.take() {
                pb.abandon_with_message(format!("{}: FAILED", table_name));
                return Err(ImportError::Database(err));
            }

            table_skipped += stats.skipped;
            for warning in stats.warnings {
                summary.warnings.push(warning);
            }

            // Flush remaining items
            if !batch.is_empty() {
                let import_result = db
                    .import_items_fresh(table_name, batch, ImportOptions::default())
                    .map_err(|e| format!("Failed to import items into '{}': {e}", table_name))?;
                table_items += import_result.items_imported;
                table_bytes += import_result.bytes_imported;
                pb.set_message(format!("{}: {} items", table_name, table_items));
                pb.tick();
            }
        }

        pb.finish_with_message(format!(
            "{}: {} items, {} bytes{}",
            table_name,
            table_items,
            format_bytes(table_bytes),
            if table_skipped > 0 {
                format!(", {} skipped", table_skipped)
            } else {
                String::new()
            }
        ));

        summary.tables.push(TableImportResult {
            table_name: table_name.clone(),
            items_imported: table_items,
            bytes_imported: table_bytes,
            lines_skipped: table_skipped,
        });
        summary.total_items += table_items;
        summary.total_bytes += table_bytes;
        summary.total_skipped += table_skipped;
    }

    // 7. Restore normal PRAGMAs (important if DB will be served after import)
    db.disable_bulk_loading()
        .map_err(|e| format!("Failed to disable bulk loading: {e}"))?;

    // Report consistency map stats
    if consistency_map.field_count() > 0 {
        eprintln!(
            "Consistency map: {} fields, {} total mappings",
            consistency_map.field_count(),
            consistency_map.total_mappings()
        );
    }

    Ok(summary)
}

/// Execute the import pipeline with file-based output.
///
/// Creates a new database at a temporary path, imports data, VACUUMs,
/// then atomically renames to the final output path. If the import fails
/// at any point, the temp file is cleaned up automatically and any
/// existing output file is preserved.
pub fn run(cmd: ImportCommand) -> Result<ImportSummary, ImportError> {
    let output = cmd
        .output
        .as_ref()
        .ok_or_else(|| ImportError::Config("output path required for file-based import".into()))?;

    // Check for existing output file
    if output.exists() && !cmd.force {
        return Err(ImportError::Config(format!(
            "Output file '{}' already exists. Use --force to overwrite.",
            output.display()
        )));
    }

    let output_path = output.clone();
    let compress = cmd.compress;

    // Write to a temp file in the same directory as the output so that
    // persist() can do an atomic rename (same filesystem). On failure,
    // NamedTempFile's Drop cleans up automatically.
    let output_dir = output_path.parent().unwrap_or(Path::new("."));
    let tmp_file = tempfile::NamedTempFile::new_in(output_dir)
        .map_err(|e| ImportError::Database(format!("Failed to create temp file: {e}")))?;
    let tmp_path = tmp_file.path().to_path_buf();

    // Close the temp file handle — Database::new will open it by path.
    // Keep the NamedTempFile alive so it cleans up on error.
    let tmp_file = tmp_file.into_temp_path();

    let db = Database::new(
        tmp_path
            .to_str()
            .ok_or_else(|| ImportError::Config("Invalid temp path".to_string()))?,
    )
    .map_err(|e| ImportError::Database(format!("Failed to create output database: {e}")))?;

    let mut summary = run_into(&db, cmd)?;

    // VACUUM for compact output.
    // Drop the db and reopen to release any in-process state before compacting.
    drop(db);
    {
        let db = Database::new(
            tmp_path
                .to_str()
                .ok_or_else(|| ImportError::Config("Invalid temp path".to_string()))?,
        )
        .map_err(|e| ImportError::Database(format!("Failed to reopen database for VACUUM: {e}")))?;
        db.vacuum()
            .map_err(|e| ImportError::Database(format!("VACUUM failed: {e}")))?;
    }
    eprintln!("Database compacted.");

    // Atomically move the temp file to the final output path.
    // This overwrites any existing file (--force was already checked above).
    tmp_file.persist(&output_path).map_err(|e| {
        ImportError::Database(format!("Failed to move database to output path: {e}"))
    })?;

    summary.output_path = Some(output_path.clone());

    // Optionally compress with zstd
    if compress {
        let compressed_path = compress_output(&output_path)?;
        summary.output_path = Some(compressed_path);
    }

    Ok(summary)
}

/// Find the raw JSON for a specific table in the schema file.
/// Converts from DescribeTable format (with "Table" wrapper) to CreateTableRequest format.
fn find_table_json(schema_json: &serde_json::Value, table_name: &str) -> Option<serde_json::Value> {
    let items: Vec<&serde_json::Value> = match schema_json {
        serde_json::Value::Array(arr) => arr.iter().collect(),
        obj @ serde_json::Value::Object(_) => vec![obj],
        _ => return None,
    };

    for item in items {
        let table = item.get("Table").unwrap_or(item);
        if table.get("TableName").and_then(|v| v.as_str()) == Some(table_name) {
            // Convert from DescribeTable format to CreateTableRequest format
            // The field names are the same (PascalCase) — just strip the "Table" wrapper
            return Some(table.clone());
        }
    }
    None
}

/// Extract key attribute names from a CreateTableRequest.
fn extract_key_attrs(request: &crate::actions::create_table::CreateTableRequest) -> Vec<String> {
    request
        .key_schema
        .iter()
        .map(|ks| ks.attribute_name.clone())
        .collect()
}

/// Compress a file with zstd, removing the original.
fn compress_output(path: &Path) -> Result<std::path::PathBuf, String> {
    let compressed_path = path.with_extension("db.zst");
    eprintln!("Compressing to {}...", compressed_path.display());

    let input = std::fs::File::open(path)
        .map_err(|e| format!("Failed to open {} for compression: {e}", path.display()))?;

    let output = std::fs::File::create(&compressed_path)
        .map_err(|e| format!("Failed to create {}: {e}", compressed_path.display()))?;

    let mut encoder =
        zstd::Encoder::new(output, 3).map_err(|e| format!("Failed to create zstd encoder: {e}"))?;

    std::io::copy(&mut std::io::BufReader::new(input), &mut encoder)
        .map_err(|e| format!("Compression failed: {e}"))?;

    encoder
        .finish()
        .map_err(|e| format!("Failed to finalize compression: {e}"))?;

    // Remove the uncompressed file
    std::fs::remove_file(path).map_err(|e| format!("Failed to remove uncompressed file: {e}"))?;

    let compressed_size = std::fs::metadata(&compressed_path)
        .map(|m| m.len())
        .unwrap_or(0);
    eprintln!(
        "Compressed output: {}",
        format_bytes(compressed_size as usize)
    );

    Ok(compressed_path)
}

/// Format bytes as human-readable.
fn format_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}
