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
pub mod notice;
pub(crate) mod parser;
pub(crate) mod schema;

use crate::{Database, ImportOptions};
use consistency::ConsistencyMap;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashSet;

pub use notice::{Concern, Notice};
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
    /// Every message the import raised, each with its concern.
    pub notices: Vec<Notice>,
    /// The messages of `notices`, in order. Kept for readers that want text.
    pub warnings: Vec<String>,
    /// The messages of the notices whose concern is [`Concern::Exposure`]: an
    /// original value was seen reaching the output. These are the reason the
    /// run's exit code is not 0, so a pipeline notices without reading prose.
    pub exposures: Vec<String>,
    /// Output file path (may differ from input if compressed). None for in-memory imports.
    pub output_path: Option<std::path::PathBuf>,
}

impl ImportSummary {
    /// Record a notice. The `warnings` and `exposures` views are filled from
    /// the notices once the run ends, so nothing else writes to them.
    fn notice(&mut self, notice: Notice) {
        self.notices.push(notice);
    }

    /// Fill the text views from the notices. Called once, at the end.
    fn finish(&mut self) {
        self.warnings = self.notices.iter().map(|n| n.message.clone()).collect();
        self.exposures = self
            .notices
            .iter()
            .filter(|n| n.concern == Concern::Exposure)
            .map(|n| n.message.clone())
            .collect();
    }
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
    // `run_into` writes into a database the caller supplied and goes on using,
    // so however this returns, their connection must not be left on
    // `synchronous = OFF`. There are several error exits between here and the
    // end, and restoring at each one by hand is how four of them were missed.
    let _restore_pragmas = BulkLoading { db };

    // 6. Import data for each table
    let mut summary = ImportSummary {
        tables: Vec::new(),
        total_items: 0,
        total_bytes: 0,
        total_skipped: 0,
        notices: Vec::new(),
        warnings: Vec::new(),
        exposures: Vec::new(),
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
    // Only a rule that applies to a table whose key it names counts: one
    // scoped to another table is not rewriting this one's keys.
    let rules_touch_a_key = rules.iter().any(|rule| match rule.path.first() {
        Some(crate::expressions::PathElement::Attribute(name)) => schemas.iter().any(|s| {
            rule.applies_to(&s.table_name) && extract_key_attrs(&s.create_request).contains(name)
        }),
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
        summary.notice(Notice::caution(message));
    }

    let mut seen_warnings: HashSet<String> = HashSet::new();
    // Across every table, not per table: a rule can legitimately touch nothing
    // in one table and every item of the next, so only the whole run can say
    // that a rule did nothing at all.
    let mut rule_work: Vec<anonymise::RuleWork> = vec![Default::default(); rules.len()];

    if rules.is_empty() && data_model.is_some() {
        summary.notice(Notice::caution(
            "a data model was given but no rules, so nothing was anonymised and no key was \
             rebuilt. The model is only used to re-render keys after a rule has changed an \
             attribute one is built from",
        ));
    }

    // A caution, not an observation: without a model the importer cannot tell
    // whether any key here is built from an attribute a rule rewrites, so this
    // is raised on every rules-only run. An exposure has to be something the
    // run saw happen, or the common case exits non-zero and the flag that
    // turns that off becomes the thing everyone passes.
    if !rules.is_empty() && data_model.is_none() {
        summary.notice(Notice::caution(
            "rules rewrite attributes only: a key built from an attribute (CUSTOMER#${email}) \
             keeps its original value. Pass --data-model <onetable.json> to rebuild keys \
             from entity templates after anonymisation",
        ));
    }

    let rules_touch_an_index_key = rules.iter().any(|rule| match rule.path.first() {
        Some(crate::expressions::PathElement::Attribute(name)) => schemas.iter().any(|s| {
            rule.applies_to(&s.table_name) && index_key_attrs(&s.create_request).contains(name)
        }),
        _ => false,
    });
    if rules_touch_an_index_key && data_model.is_none() {
        summary.notice(Notice::caution(
            "a rule rewrites an index key attribute directly. No row is lost, but rows that \
             an index returned together are moved apart, and without a data model nothing \
             checks whether they still belong together",
        ));
    }

    if rules_touch_a_key && data_model.is_none() {
        summary.notice(Notice::caution(
            "a rule rewrites a key attribute directly, and without a data model nothing \
             counts what that costs: two items whose rewritten key comes out the same land \
             on one row and the later one replaces the earlier. Overwrites are only counted \
             when --data-model is given, so the item count below will not show what was lost",
        ));
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
        let mut key_deriver = match data_model.as_ref() {
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
                    for n in &summary.notices {
                        eprintln!("  - {n}");
                    }
                    ImportError::Config(format!("data model: {e}"))
                })?;
                for notice in warnings {
                    summary.notice(notice.for_table(table_name));
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
                        table_name,
                        &rules,
                        &mut consistency_map,
                        &consistency_fields,
                        &key_attrs,
                        &mut anonymise::RuleTally {
                            mask_passthroughs: &mut mask_passthroughs,
                            rule_work: &mut rule_work,
                        },
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
                    for notice in warnings {
                        // Named for the table, both so the reader knows
                        // where it came from and so the cross-table dedupe
                        // below cannot swallow table B's copy of a notice
                        // table A already raised.
                        let notice = notice.for_table(table_name);
                        if seen_warnings.insert(notice.message.clone()) {
                            summary.notice(notice);
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
                                summary.notice(Notice::caution(msg));
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
            // A skipped line is a row absent from the output, which is
            // reported and counted but is not an original value reaching it.
            for warning in stats.warnings {
                summary.notice(Notice::caution(warning));
            }

            // Flush remaining items. Under --continue-on-error this batch is
            // treated as the others were: the flag covered every full batch
            // and then the last partial one failed the run anyway.
            if !batch.is_empty() {
                match insert_items(table_name, batch) {
                    Ok(result) => {
                        table_items += result.items_imported;
                        table_bytes += result.bytes_imported;
                    }
                    Err(e) => {
                        let msg = format!("Batch import error for '{}': {e}", table_name);
                        if cmd.continue_on_error {
                            summary.notice(Notice::caution(msg));
                        } else {
                            pb.abandon_with_message(format!("{}: FAILED", table_name));
                            return Err(ImportError::Database(msg));
                        }
                    }
                }
                pb.set_message(format!("{}: {} items", table_name, table_items));
                pb.tick();
            }
        }

        if let Some(deriver) = key_deriver.as_mut() {
            // Filed before the join check below, so that a run the check
            // fails still prints everything else the table had to say.
            for notice in deriver.take_notices() {
                summary.notice(notice.for_table(table_name));
            }

            // Both entities of a shared key attribute turned up, so the join
            // between them is broken in the output rather than merely at risk.
            // The CLI writes to a temporary file that an error discards, so
            // failing here costs the run's time, not a half-anonymised
            // database. `run_into` writes into the caller's database and
            // leaves earlier tables in place.
            let join_breaks = deriver.join_breaks();
            if let Some(first) = join_breaks.first() {
                // The notices collected so far are what explain the failure;
                // returning without them leaves the operator with a verdict
                // and no evidence.
                for n in &summary.notices {
                    eprintln!("  - {n}");
                }
                // The BulkLoading guard from step 5 restores the PRAGMAs.
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
        }

        let mut left_whole: Vec<(&String, &usize)> = mask_passthroughs.iter().collect();
        left_whole.sort();
        for (attribute, count) in left_whole {
            summary.notice(Notice::exposure(format!(
                "table '{table_name}': {count} items kept '{attribute}' as it arrived \
                 because the value was no shorter than the characters the mask keeps, so \
                 those rows carry the original value"
            )));
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
            .map(|r| crate::expressions::format_path_for_error(&r.path))
            .unwrap_or_default();
        // A rule scoped to tables this run did not read had no work to do,
        // so its silence is not a value left behind. Without the scope a
        // shared rules file run with --tables raised an exposure on every
        // run for the rules aimed at the tables left out.
        if let Some(tables) = rules.get(index).and_then(|r| r.tables.as_ref())
            && !tables
                .iter()
                .any(|t| export_files.iter().any(|(name, _)| name == t))
        {
            summary.notice(Notice::caution(format!(
                "rule {number} (path '{path}') is scoped to {} and none of those tables is in \
                 this run, so it was not applied",
                quoted_tables(tables)
            )));
            continue;
        }
        if work.matched == 0 {
            summary.notice(Notice::exposure(format!(
                "rule {number} (path '{path}') matched no item in any table, so nothing \
                 was anonymised by it. Check the match expression, and check the path is \
                 spelled the way the export spells it"
            )));
        } else if work.rewrote == 0 {
            summary.notice(Notice::exposure(format!(
                "rule {number} matched {} items but rewrote none of them: not one carried \
                 the path '{path}'. The attribute it names is absent from this data",
                work.matched
            )));
        }
        // No branch for "matched some items that did not carry the path": a
        // match broader than the path is the shape this page documents, so
        // most correct rules would raise it on every run and the warnings
        // would stop being read.
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

    summary.finish();
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

    // Check the path this run will write, which under --compress is the
    // archive, not the database name given. Checking the wrong one let an
    // existing archive be replaced without --force and blocked a compressed
    // run on a stray database it was never going to touch.
    let compress = cmd.compress;
    let force = cmd.force;
    let final_path = if compress {
        output.with_extension("db.zst")
    } else {
        output.clone()
    };
    if final_path.exists() && !cmd.force {
        return Err(ImportError::Config(format!(
            "Output file '{}' already exists. Use --force to overwrite.",
            final_path.display()
        )));
    }

    let output_path = output.clone();

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
    // Compress before anything lands at the output path. Compressing after
    // the rename left the uncompressed database at the final path when
    // compression failed, on a run that then returned an error, which is the
    // one shape "nothing is persisted on the error path" promised not to
    // produce.
    if compress {
        let compressed_path = final_path;
        let compressed_tmp = tempfile::NamedTempFile::new_in(output_dir)
            .map_err(|e| ImportError::Database(format!("Failed to create temp file: {e}")))?
            .into_temp_path();
        let size = compress_to(&tmp_path, &compressed_tmp)?;
        move_into_place(compressed_tmp, &compressed_path, force)?;
        eprintln!("Compressed output: {}", format_bytes(size));
        summary.output_path = Some(compressed_path);
        return Ok(summary);
    }

    move_into_place(tmp_file, &output_path, force)?;
    summary.output_path = Some(output_path);

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
pub(super) fn unwrap_describe_table_shapes(table: &mut serde_json::Value) {
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

/// `'a'`, or `'a' and 'b'`, for a message.
fn quoted_tables(names: &[String]) -> String {
    let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
    match quoted.split_last() {
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
        None => String::new(),
    }
}

/// Puts the database's ordinary PRAGMAs back however the import leaves.
struct BulkLoading<'a> {
    db: &'a Database,
}

impl Drop for BulkLoading<'_> {
    fn drop(&mut self) {
        let _ = self.db.disable_bulk_loading();
    }
}

/// The table's own key attributes.
///
/// The primary key only. Both callers are about one row replacing another, and
/// only the primary key can do that: rewriting an index key moves a row within
/// that index without losing it. Widening this to index keys made the
/// write-path choice and the collision warning describe a loss that cannot
/// happen. [`index_key_attrs`] carries the index keys for the callers that
/// want those instead.
fn extract_key_attrs(request: &crate::actions::create_table::CreateTableRequest) -> Vec<String> {
    request
        .key_schema
        .iter()
        .map(|ks| ks.attribute_name.clone())
        .collect()
}

/// Key attributes belonging to an index rather than the table.
///
/// Rewriting one of these does not lose a row, but it does move the row within
/// that index, so a query over it stops finding what it found.
fn index_key_attrs(request: &crate::actions::create_table::CreateTableRequest) -> Vec<String> {
    let base = extract_key_attrs(request);
    let mut names = Vec::new();
    let mut push = |name: &String| {
        if !base.contains(name) && !names.contains(name) {
            names.push(name.clone());
        }
    };
    for gsi in request.global_secondary_indexes.as_deref().unwrap_or(&[]) {
        for ks in &gsi.key_schema {
            push(&ks.attribute_name);
        }
    }
    for lsi in request.local_secondary_indexes.as_deref().unwrap_or(&[]) {
        for ks in &lsi.key_schema {
            push(&ks.attribute_name);
        }
    }
    names
}

/// Rename a finished temp file onto the output path.
///
/// Without `--force` the rename refuses an existing file rather than
/// replacing it. The existence check at the start of the run is not enough
/// on its own: an import takes time, and a second run writing the same path
/// can finish inside that window, after the check and before this rename.
fn move_into_place(tmp: tempfile::TempPath, dst: &Path, force: bool) -> Result<(), ImportError> {
    let moved = if force {
        tmp.persist(dst)
    } else {
        tmp.persist_noclobber(dst)
    };
    moved.map_err(|e| {
        ImportError::Database(format!(
            "Failed to move database to output path '{}': {}",
            dst.display(),
            e.error
        ))
    })
}

/// Compress `src` into `dst` with zstd, returning the compressed size.
fn compress_to(src: &Path, dst: &Path) -> Result<usize, ImportError> {
    let input = std::fs::File::open(src).map_err(|e| {
        ImportError::Database(format!(
            "Failed to open {} for compression: {e}",
            src.display()
        ))
    })?;
    let output = std::fs::File::create(dst)
        .map_err(|e| ImportError::Database(format!("Failed to create {}: {e}", dst.display())))?;
    let mut encoder = zstd::Encoder::new(output, 3)
        .map_err(|e| ImportError::Database(format!("Failed to create zstd encoder: {e}")))?;
    std::io::copy(&mut std::io::BufReader::new(input), &mut encoder)
        .map_err(|e| ImportError::Database(format!("Compression failed: {e}")))?;
    encoder
        .finish()
        .map_err(|e| ImportError::Database(format!("Failed to finalize compression: {e}")))?;
    Ok(std::fs::metadata(dst)
        .map(|m| m.len() as usize)
        .unwrap_or(0))
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

#[cfg(all(test, feature = "import"))]
mod tests {
    use super::*;

    #[test]
    fn the_text_views_are_the_notices_in_order_and_the_exposures_among_them() {
        let mut summary = ImportSummary {
            tables: Vec::new(),
            total_items: 0,
            total_bytes: 0,
            total_skipped: 0,
            notices: Vec::new(),
            warnings: vec!["stale".to_string()],
            exposures: vec!["stale".to_string()],
            output_path: None,
        };
        summary.notice(Notice::caution("first, a caution"));
        summary.notice(Notice::exposure("then an exposure"));
        summary.notice(Notice::caution("last, a caution"));
        summary.finish();

        assert_eq!(
            summary.warnings,
            vec!["first, a caution", "then an exposure", "last, a caution"]
        );
        assert_eq!(summary.exposures, vec!["then an exposure"]);
    }

    #[test]
    fn a_file_that_appeared_after_the_check_is_not_replaced_without_force() {
        // The race the up-front check cannot close: something else wrote the
        // destination while this run was importing.
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("out.db");
        std::fs::write(&dst, b"theirs").unwrap();

        let tmp = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        std::fs::write(tmp.path(), b"ours").unwrap();
        let err = move_into_place(tmp.into_temp_path(), &dst, false).unwrap_err();
        assert!(err.to_string().contains("out.db"), "{err}");
        assert_eq!(std::fs::read(&dst).unwrap(), b"theirs", "theirs survives");

        let tmp = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        std::fs::write(tmp.path(), b"ours").unwrap();
        move_into_place(tmp.into_temp_path(), &dst, true).unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"ours", "--force replaces it");
    }
}
