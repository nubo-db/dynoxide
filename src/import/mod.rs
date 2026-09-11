//! Import CLI for DynamoDB Export data.
//!
//! Parses DynamoDB Export JSON Lines files, optionally applies anonymisation
//! rules, and imports the data into a Dynoxide SQLite database.
//!
//! ## Pipeline
//!
//! 1. Parse TOML config (validate all rules upfront)
//! 2. Source table schemas from `--schema <file>`, and the data model from
//!    `--data-model <file>` when keys are built from attributes
//! 3. Create tables in output SQLite database
//! 4. For each table: read JSON Lines → parse → anonymise → rebuild templated
//!    keys → batch insert
//! 5. VACUUM (compact the SQLite file)
//! 6. Optionally compress with zstd

pub(crate) mod anonymise;
pub(crate) mod config;
pub(crate) mod consistency;
pub(crate) mod keys;
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
    /// Optional OneTable schema. With it, keys built from attributes
    /// (`user#${email}`) are rebuilt from the anonymised attributes after the
    /// rules run, so the entity prefix survives and key and attribute agree.
    pub data_model: Option<std::path::PathBuf>,
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

/// Scaffold empty tables from a DynamoDB DescribeTable JSON schema file.
///
/// Reads the schema file, creates each table defined in it, and skips any
/// tables that already exist. Returns the number of tables created.
///
/// The schema file format is identical to `import --schema`: a JSON file
/// containing a single `aws dynamodb describe-table` response or an array
/// of them.
pub fn scaffold_from_schema(db: &Database, path: &std::path::Path) -> Result<usize, ImportError> {
    let (schemas, schema_json) = schema::load_schemas(path).map_err(ImportError::Config)?;
    let mut created = 0;
    for table_schema in &schemas {
        let create_request = build_create_request(&schema_json, &table_schema.table_name)?;
        match db.create_table(create_request) {
            Ok(_) => created += 1,
            Err(crate::errors::DynoxideError::ResourceInUseException(_)) => {} // already exists
            Err(e) => return Err(ImportError::Database(e.to_string())),
        }
    }
    Ok(created)
}

/// Build a `CreateTableRequest` for `table_name` from raw schema JSON.
///
/// Deserializes through `CreateTableRequest`'s `Deserialize` impl (rather than
/// building the struct by hand) so GlobalSecondaryIndexes and
/// LocalSecondaryIndexes are picked up from the schema. Shared by `run_into`
/// and `scaffold_from_schema` so the two can't drift apart on which fields a
/// schema-sourced table ends up with.
fn build_create_request(
    schema_json: &serde_json::Value,
    table_name: &str,
) -> Result<crate::actions::create_table::CreateTableRequest, String> {
    let table_json = find_table_json(schema_json, table_name)
        .ok_or_else(|| format!("Schema JSON not found for table '{table_name}'"))?;

    serde_json::from_value(table_json)
        .map_err(|e| format!("Failed to deserialize schema for '{table_name}': {e}"))
}

/// Execute the import pipeline into a caller-provided database.
///
/// This is the core import logic: database-agnostic. The caller is
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

    let data_model = match cmd.data_model {
        Some(ref path) => {
            let model =
                crate::schema::onetable::parse_onetable_file(path).map_err(ImportError::Config)?;
            eprintln!(
                "Loaded data model: {} ({} entities) from {}",
                model.schema_format,
                model.entities.len(),
                path.display()
            );
            Some(model)
        }
        None => None,
    };

    // A seeded fake derives its value and does not populate the consistency
    // map, because it does not need to. That only holds while every rule
    // writing a given field agrees: mix a seeded rule with an unseeded one, or
    // two different seeds or generators, and the same input can leave with two
    // different values depending on which rule matched. Nothing downstream
    // would show that, so say it here.
    let mixed = mixed_consistency_rules(&rules, &consistency_fields);

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

        let create_request = build_create_request(&schema_json, table_name)?;

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

    // Keys can move two ways: rebuilt from a template, or rewritten by a rule
    // that names a key attribute. Either can land two source items on one
    // primary key, which is exactly the assumption `import_items_fresh`
    // trades away: it skips the GSI delete-before-insert, so an overwritten
    // base row would leave its old index entry behind and index queries
    // would answer from a row that no longer exists. Neither happens without
    // rules, so a plain import keeps the fast path that assumes every key is
    // unique.
    let key_attr_names: HashSet<String> = schemas
        .iter()
        .flat_map(|s| extract_key_attrs(&s.create_request))
        .collect();
    let rules_touch_a_key = rules.iter().any(|rule| match rule.path.first() {
        Some(crate::expressions::PathElement::Attribute(name)) => key_attr_names.contains(name),
        _ => false,
    });
    let rebuilds_keys = !rules.is_empty() && (data_model.is_some() || rules_touch_a_key);
    let insert_items = |table: &str, batch: Vec<crate::types::Item>| {
        if rebuilds_keys {
            db.import_items(table, batch, ImportOptions::default())
        } else {
            db.import_items_fresh(table, batch, ImportOptions::default())
        }
    };

    for message in mixed {
        summary.warnings.push(message);
    }

    let mut seen_warnings: HashSet<String> = HashSet::new();
    // Across every table, not per table: a rule can legitimately touch nothing
    // in one table and every item of the next, so only the whole run can say
    // that a rule did nothing at all.
    let mut rule_work: Vec<anonymise::RuleWork> = vec![Default::default(); rules.len()];

    if rules.is_empty() && data_model.is_some() {
        summary.warnings.push(
            "a data model was given but no rules, so nothing was anonymised and no key was \
             rebuilt. The model is only used to re-render keys after a rule has changed an \
             attribute one is built from"
                .to_string(),
        );
    }

    if !rules.is_empty() && data_model.is_none() {
        summary.warnings.push(
            "rules rewrite attributes only: a key built from an attribute (CUSTOMER#${email}) \
             keeps its original value. Pass --data-model <onetable.json> to rebuild keys \
             from entity templates after anonymisation"
                .to_string(),
        );
    }

    if rules_touch_a_key && data_model.is_none() {
        summary.warnings.push(
            "a rule rewrites a key attribute directly, and without a data model nothing \
             counts what that costs: two items whose rewritten key comes out the same land \
             on one row and the later one replaces the earlier. Overwrites are only counted \
             when --data-model is given, so the item count below will not show what was lost"
                .to_string(),
        );
    }

    for (table_name, files) in &export_files {
        let table_schema = schema_map.get(table_name.as_str()).unwrap();
        let key_attrs = extract_key_attrs(&table_schema.create_request);
        // Values a mask rule left whole, by attribute. Bounded by the number
        // of masked attributes, not by item count.
        let mut mask_passthroughs: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        // A data model with no rules has nothing to rebuild from: keys are
        // only re-rendered after an anonymisation moved something, so the
        // deriver would collect model-versus-schema warnings and then never
        // be asked to plan a single item. Reporting those reads as diagnostics
        // about work the run did, and it did none.
        let mut key_deriver = match data_model.as_ref().filter(|_| !rules.is_empty()) {
            Some(model) => {
                let (deriver, warnings) = keys::KeyDeriver::new(
                    model,
                    &table_schema.create_request,
                    &rules,
                    &consistency_fields,
                )
                .map_err(|e| {
                    // The warnings collected so far are part of the
                    // explanation, and the summary that carries them does
                    // not survive an error return.
                    for w in &summary.warnings {
                        eprintln!("  - {w}");
                    }
                    ImportError::Config(format!("data model: {e}"))
                })?;
                for w in warnings {
                    summary.warnings.push(format!("table '{table_name}': {w}"));
                }
                Some(deriver)
            }
            None => None,
        };

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

                // Apply anonymisation rules, then rebuild any key the data
                // model says is built from the attributes just rewritten
                if !rules.is_empty() {
                    let mut warnings = Vec::new();
                    let plan = key_deriver
                        .as_mut()
                        .and_then(|d| d.plan(&item, &mut warnings));
                    let (rule_warnings, rewritten) = anonymise::apply_rules(
                        &mut item,
                        &rules,
                        &mut consistency_map,
                        &consistency_fields,
                        &key_attrs,
                        &mut mask_passthroughs,
                        &mut rule_work,
                    );
                    warnings.extend(rule_warnings);
                    match (key_deriver.as_mut(), plan) {
                        (Some(deriver), Some(plan)) => {
                            deriver.apply(&plan, &rewritten, &mut item, &mut warnings);
                        }
                        // An item that matched no entity keeps its keys, but a
                        // rebuilt key can still land on them, and that row is
                        // lost like any other. Record it so the overwrite is
                        // counted rather than invisible.
                        (Some(deriver), None) => deriver.note_primary_key(&item),
                        (None, _) => {}
                    }
                    for w in warnings {
                        // Prefixed with the table, both so the reader knows
                        // where it came from and so the cross-table dedupe
                        // below cannot swallow table B's copy of a warning
                        // table A already raised.
                        let w = format!("table '{table_name}': {w}");
                        if seen_warnings.insert(w.clone()) {
                            summary.warnings.push(w);
                        }
                    }
                }
                batch.push(item);

                // Flush batch when full
                if batch.len() >= BATCH_SIZE {
                    let chunk = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                    match insert_items(table_name, chunk) {
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
                let import_result = insert_items(table_name, batch)
                    .map_err(|e| format!("Failed to import items into '{}': {e}", table_name))?;
                table_items += import_result.items_imported;
                table_bytes += import_result.bytes_imported;
                pb.set_message(format!("{}: {} items", table_name, table_items));
                pb.tick();
            }
        }

        if let Some(deriver) = key_deriver.as_mut() {
            let unmatched = deriver.take_unmatched();
            if unmatched > 0 {
                summary.warnings.push(format!(
                    "table '{}': {} items matched no entity in the data model \
                     (no type attribute, and no entity's key templates reproduce their keys); \
                     their keys were not rebuilt",
                    table_name, unmatched
                ));
            }
            // Both entities of a shared key attribute turned up, so the join
            // between them is broken in the output rather than merely at risk.
            // The CLI writes to a temporary file that an error discards, so
            // failing here costs the run's time, not a half-anonymised
            // database. `run_into` writes into the caller's database and
            // leaves earlier tables in place.
            let join_breaks = deriver.join_breaks();
            if let Some(first) = join_breaks.first() {
                // The warnings collected so far are what explain the failure;
                // returning without them leaves the operator with a verdict
                // and no evidence.
                for w in &summary.warnings {
                    eprintln!("  - {w}");
                }
                // `run_into` writes into a database the caller supplied and
                // goes on using, so the bulk-loading PRAGMAs have to come off
                // on the way out. Leaving `synchronous = OFF` on someone
                // else's connection trades their durability for our import.
                let _ = db.disable_bulk_loading();
                return Err(ImportError::Config(format!(
                    "table '{}': {}{}",
                    table_name,
                    first,
                    if join_breaks.len() > 1 {
                        format!(" (and {} more)", join_breaks.len() - 1)
                    } else {
                        String::new()
                    }
                )));
            }

            for (entity, key, count) in deriver.take_unrebuilt() {
                summary.warnings.push(format!(
                    "table '{table_name}': entity '{entity}' kept the original {key} on \
                     {count} items, because its template could not rebuild them. Those keys \
                     still hold the values the export arrived with"
                ));
            }

            let unchecked = deriver.unchecked_join_keys();
            if unchecked > 0 {
                summary.warnings.push(format!(
                    "table '{}': the join check stopped taking on new keys once a group \
                     reached {}, so {} items were never compared; a broken join among those \
                     would not have been caught. Items whose key was seen before the cap \
                     were still checked",
                    table_name,
                    keys::MAX_TRACKED_KEYS,
                    unchecked
                ));
            }

            let (collisions, capped) = deriver.take_collisions();
            if collisions > 0 {
                summary.warnings.push(format!(
                    "table '{}': {} items rendered the same primary key as an earlier item and \
                     overwrote it, so the output holds fewer rows than the export. Either a \
                     rule is replacing an attribute a key is built from with a constant, or a \
                     fake generator is drawing the same value twice: every generator except \
                     safe_email draws from a pool small enough that repeats are ordinary at a \
                     few hundred items, so prefer hash or a seeded safe_email for an attribute \
                     a key is built from",
                    table_name, collisions
                ));
            }
            if capped {
                summary.warnings.push(format!(
                    "table '{}': the rebuilt-key collision check stopped after {} keys; \
                     later collisions in this table were not counted",
                    table_name,
                    keys::MAX_TRACKED_KEYS
                ));
            }
        }

        let mut left_whole: Vec<(&String, &usize)> = mask_passthroughs.iter().collect();
        left_whole.sort();
        for (attribute, count) in left_whole {
            summary.warnings.push(format!(
                "table '{table_name}': {count} items kept '{attribute}' as it arrived because \
                 the value was no shorter than the characters the mask keeps, so those rows \
                 carry the original value"
            ));
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

    // A rules file is the operator's statement of which attributes hold
    // personal data. A rule that never fired means that statement was not
    // carried out, and nothing else in the output says so: the item count is
    // full, no warning is raised, and the run exits 0. A misspelt path or a
    // match expression that fits none of the data both take exactly that
    // shape, which is why this is reported per rule rather than in aggregate.
    for (index, work) in rule_work.iter().enumerate() {
        let number = index + 1;
        let path = rules
            .get(index)
            .map(|r| anonymise::path_to_field_name(&r.path))
            .unwrap_or_default();
        if work.matched == 0 {
            summary.warnings.push(format!(
                "rule {number} (path '{path}') matched no item in any table, so nothing was \
                 anonymised by it. Check the match expression, and check the path is spelled \
                 the way the export spells it"
            ));
        } else if work.rewrote == 0 {
            summary.warnings.push(format!(
                "rule {number} matched {} items but rewrote none of them: not one carried \
                 the path '{path}'. The attribute it names is absent from this data",
                work.matched
            ));
        } else if work.path_missing > 0 {
            summary.warnings.push(format!(
                "rule {number} (path '{path}') matched {} items but {} of them did not carry \
                 that attribute, so those kept the values they arrived with",
                work.matched, work.path_missing
            ));
        }
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

    // Close the temp file handle - Database::new will open it by path.
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
            // Convert from DescribeTable format to CreateTableRequest format:
            // strip the "Table" wrapper, then translate the fields whose shape
            // differs between the two.
            let mut table = table.clone();
            unwrap_describe_table_shapes(&mut table);
            return Some(table);
        }
    }
    None
}

/// Translate DescribeTable-only shapes into their CreateTableRequest
/// equivalents. DescribeTable wraps billing mode and table class in summary
/// objects (`BillingModeSummary`, `TableClassSummary`), which CreateTable
/// never reads, so both would otherwise fall back to their defaults. It also
/// reports zeroed `ProvisionedThroughput` blocks on an on-demand table and
/// its GSIs, which CreateTable rejects, so those are dropped too. The drop
/// is gated on the billing mode having come from the summary: a schema
/// already in CreateTable shape passes through untouched, so an inconsistent
/// one still fails validation exactly as it would on the CreateTable API.
fn unwrap_describe_table_shapes(table: &mut serde_json::Value) {
    let Some(obj) = table.as_object_mut() else {
        return;
    };

    let mut billing_mode_hoisted = false;
    for (summary_key, field_key) in [
        ("BillingModeSummary", "BillingMode"),
        ("TableClassSummary", "TableClass"),
    ] {
        if !obj.contains_key(field_key)
            && let Some(value) = obj.get(summary_key).and_then(|s| s.get(field_key))
        {
            let value = value.clone();
            obj.insert(field_key.to_string(), value);
            billing_mode_hoisted |= field_key == "BillingMode";
        }
    }

    if billing_mode_hoisted
        && obj.get("BillingMode").and_then(|v| v.as_str()) == Some("PAY_PER_REQUEST")
    {
        obj.remove("ProvisionedThroughput");
        if let Some(gsis) = obj
            .get_mut("GlobalSecondaryIndexes")
            .and_then(|v| v.as_array_mut())
        {
            for gsi in gsis {
                if let Some(gsi) = gsi.as_object_mut() {
                    gsi.remove("ProvisionedThroughput");
                }
            }
        }
    }
}

/// Consistency fields written by rules that do not agree on how they generate.
///
/// Returns one message per offending field. A field is fine when every rule
/// targeting it uses the same action shape; it is not when a seeded fake sits
/// beside an unseeded one, or beside a different seed or generator, because
/// the seeded rule bypasses the map the other one depends on. Two seeds, or
/// two salts, are two shapes as well: the secret is part of the derivation,
/// so the same input leaves with a different value under each. They are told
/// apart by position rather than by value, so the message never carries one.
fn mixed_consistency_rules(
    rules: &[config::ValidatedRule],
    consistency_fields: &HashSet<String>,
) -> Vec<String> {
    use crate::expressions::PathElement;

    let mut messages = Vec::new();
    for field in consistency_fields {
        // Numbering has to tell two secrets apart, not keep them. Holding the
        // copies in the same wrapper as the original means they are wiped on
        // drop like every other copy of a salt or a seed.
        let mut secrets: Vec<zeroize::Zeroizing<Vec<u8>>> = Vec::new();
        let mut secret_number = |bytes: &[u8]| -> usize {
            match secrets.iter().position(|known| known.as_slice() == bytes) {
                Some(i) => i + 1,
                None => {
                    secrets.push(zeroize::Zeroizing::new(bytes.to_vec()));
                    secrets.len()
                }
            }
        };
        let mut shapes: Vec<String> = rules
            .iter()
            .filter(|rule| {
                matches!(rule.path.first(), Some(PathElement::Attribute(name)) if name == field)
            })
            .map(|rule| match &rule.action {
                config::ValidatedAction::Fake {
                    generator,
                    seed: Some(seed),
                } => {
                    let n = secret_number(seed.as_bytes());
                    format!("seeded fake '{generator}' (seed {n})")
                }
                config::ValidatedAction::Fake {
                    generator,
                    seed: None,
                } => format!("unseeded fake '{generator}'"),
                config::ValidatedAction::Hash { salt } => {
                    let n = secret_number(salt.as_bytes());
                    format!("hash (salt {n})")
                }
                other => format!("{other:?}")
                    .split_whitespace()
                    .next()
                    .unwrap_or("action")
                    .to_lowercase(),
            })
            .collect();
        shapes.sort();
        shapes.dedup();
        if shapes.len() > 1 {
            messages.push(format!(
                "'{field}' is in [consistency] fields but its rules do not agree on how they \
                 generate ({}). A seeded fake derives its value and does not use the consistency \
                 map, so mixing it with anything else can give one input two different values",
                shapes.join(", ")
            ));
        }
    }
    messages.sort();
    messages
}

/// Extract key attribute names from a CreateTableRequest.
fn extract_key_attrs(request: &crate::actions::create_table::CreateTableRequest) -> Vec<String> {
    // Every index's keys, not just the table's. A rule naming a GSI key
    // attribute rewrites that index's grouping exactly as a rule on `pk`
    // rewrites the table's, and reading only the base schema meant neither
    // the write-path choice nor the warning noticed.
    let mut names: Vec<String> = request
        .key_schema
        .iter()
        .map(|ks| ks.attribute_name.clone())
        .collect();
    for gsi in request.global_secondary_indexes.as_deref().unwrap_or(&[]) {
        for ks in &gsi.key_schema {
            if !names.contains(&ks.attribute_name) {
                names.push(ks.attribute_name.clone());
            }
        }
    }
    for lsi in request.local_secondary_indexes.as_deref().unwrap_or(&[]) {
        for ks in &lsi.key_schema {
            if !names.contains(&ks.attribute_name) {
                names.push(ks.attribute_name.clone());
            }
        }
    }
    names
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
