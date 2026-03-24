//! Dynoxide CLI binary.
//!
//! Supports subcommands:
//! - `dynoxide` or `dynoxide serve` — start the DynamoDB-compatible HTTP server
//! - `dynoxide mcp` — start the MCP server (enabled by default)
//! - `dynoxide import` — import DynamoDB Export data (enabled by default)

#[cfg(any(feature = "http-server", feature = "mcp-server"))]
use dynoxide::Database;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[cfg(any(feature = "http-server", feature = "mcp-server"))]
use tracing_subscriber::EnvFilter;

// CLI always uses vendored OpenSSL (`encryption` feature), never `encryption-cc`.
// Do NOT change these guards to `_has-encryption` — that's for the library API only.
#[cfg(all(feature = "encryption", feature = "http-server"))]
use zeroize::Zeroizing;

#[derive(Parser)]
#[command(
    name = "dynoxide",
    version,
    about = "A fast, lightweight drop-in replacement for DynamoDB Local, backed by SQLite",
    after_help = after_help_text(),
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    // --- Top-level args for backward compatibility (no subcommand = serve) ---
    // These are only used when no subcommand is given, for backward compat
    // with `dynoxide --port 8000` style invocations.
    /// Host to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: Option<String>,

    /// Port to listen on
    #[arg(short, long)]
    port: Option<u16>,

    /// Path to SQLite database file (omit for in-memory)
    #[arg(long)]
    db_path: Option<String>,

    /// Path to file containing the encryption key (requires encryption feature)
    #[arg(long, value_name = "PATH")]
    encryption_key_file: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the DynamoDB-compatible HTTP server
    Serve(ServeArgs),

    /// Start the MCP (Model Context Protocol) server
    #[cfg(feature = "mcp-server")]
    Mcp(McpArgs),

    /// Import DynamoDB Export data into a Dynoxide database
    #[cfg(feature = "import")]
    Import(ImportArgs),
}

/// Arguments for the `serve` subcommand.
#[derive(clap::Args)]
#[cfg_attr(not(feature = "http-server"), allow(dead_code))]
struct ServeArgs {
    /// Host to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on
    #[arg(short, long, default_value_t = 8000)]
    port: u16,

    /// Path to SQLite database file (omit for in-memory)
    #[arg(long)]
    db_path: Option<String>,

    /// Path to file containing the encryption key (requires encryption feature)
    #[arg(long, value_name = "PATH")]
    encryption_key_file: Option<PathBuf>,

    /// Also start MCP server over Streamable HTTP (for Claude Code integration)
    #[cfg(feature = "mcp-server")]
    #[arg(long)]
    mcp: bool,

    /// Port for the MCP HTTP transport (default: 19280)
    #[cfg(feature = "mcp-server")]
    #[arg(long, default_value_t = 19280, requires = "mcp")]
    mcp_port: u16,

    /// MCP read-only mode: reject all write operations via MCP
    #[cfg(feature = "mcp-server")]
    #[arg(long, requires = "mcp")]
    mcp_read_only: bool,

    /// Path to a OneTable schema file for MCP data model context
    #[cfg(feature = "mcp-server")]
    #[arg(long, value_name = "PATH", requires = "mcp")]
    mcp_data_model: Option<PathBuf>,
}

/// Arguments for the `mcp` subcommand.
#[cfg(feature = "mcp-server")]
#[derive(clap::Args)]
struct McpArgs {
    /// Path to SQLite database file (omit for in-memory)
    #[arg(long)]
    db_path: Option<String>,

    /// Path to file containing the encryption key (requires encryption feature)
    #[arg(long, value_name = "PATH")]
    encryption_key_file: Option<PathBuf>,

    /// Use Streamable HTTP transport instead of stdio
    #[arg(long)]
    http: bool,

    /// Port for the HTTP transport (default: 19280)
    #[arg(long, default_value_t = 19280)]
    port: u16,

    /// Read-only mode: reject all write operations
    #[arg(long)]
    read_only: bool,

    /// Maximum items returned by query/scan (caps the limit parameter)
    #[arg(long, value_name = "N")]
    max_items: Option<usize>,

    /// Maximum response size in bytes for query/scan
    #[arg(long, value_name = "BYTES")]
    max_size_bytes: Option<usize>,

    /// Path to a OneTable schema file for data model context
    #[arg(long, value_name = "PATH")]
    data_model: Option<PathBuf>,

    /// Maximum entities shown in MCP instructions summary (0 = suppress)
    #[arg(long, value_name = "N", default_value_t = 20)]
    data_model_summary_limit: usize,
}

/// Arguments for the `import` subcommand.
///
/// Operates in one of two mutually exclusive modes:
/// - **File mode** (`--output`): import to a SQLite file, then VACUUM and optionally compress.
/// - **Serve mode** (`--serve` or `--mcp`): import into an in-memory database, then start a server.
#[cfg(feature = "import")]
#[derive(clap::Args)]
struct ImportArgs {
    /// Source directory containing DynamoDB Export files
    #[arg(long)]
    source: std::path::PathBuf,

    /// Output SQLite database file path (mutually exclusive with --serve/--mcp)
    #[arg(long, conflicts_with_all = ["serve", "mcp"])]
    output: Option<std::path::PathBuf>,

    /// Schema file (JSON with DescribeTable responses)
    #[arg(long)]
    schema: std::path::PathBuf,

    /// Anonymisation rules TOML file
    #[arg(long)]
    rules: Option<std::path::PathBuf>,

    /// Comma-separated list of table names to import (default: all)
    #[arg(long, value_delimiter = ',')]
    tables: Option<Vec<String>>,

    /// Compress output with zstd (requires --output)
    #[arg(long, requires = "output")]
    compress: bool,

    /// Overwrite existing output file
    #[arg(long)]
    force: bool,

    /// Continue importing when a batch fails instead of aborting
    #[arg(long)]
    continue_on_error: bool,

    /// After import, start an HTTP server with the imported data (in-memory)
    #[cfg(feature = "http-server")]
    #[arg(long, conflicts_with = "output")]
    serve: bool,

    /// Host to bind to when using --serve
    #[cfg(feature = "http-server")]
    #[arg(long, default_value = "127.0.0.1", requires = "serve")]
    host: String,

    /// Port to listen on when using --serve
    #[cfg(feature = "http-server")]
    #[arg(long, default_value_t = 8000, requires = "serve")]
    port: u16,

    /// After import, start an MCP server with the imported data (in-memory)
    ///
    /// Can be combined with --serve to run both HTTP and MCP servers.
    /// When used alone, starts a stdio MCP server.
    /// When used with --serve, starts an HTTP MCP server on --mcp-port.
    #[cfg(feature = "mcp-server")]
    #[arg(long, conflicts_with = "output")]
    mcp: bool,

    /// Port for the MCP HTTP server when using --serve --mcp together
    #[cfg(feature = "mcp-server")]
    #[arg(long, default_value_t = 8100, requires = "mcp")]
    mcp_port: u16,

    /// Run MCP server in read-only mode (disable write operations)
    #[cfg(feature = "mcp-server")]
    #[arg(long, requires = "mcp")]
    mcp_read_only: bool,

    /// Path to a OneTable schema file for MCP data model context
    #[cfg(feature = "mcp-server")]
    #[arg(long, value_name = "PATH", requires = "mcp")]
    mcp_data_model: Option<PathBuf>,
}

/// Returns the after_help text. Encryption builds include key guidance.
fn after_help_text() -> &'static str {
    #[cfg(feature = "encryption")]
    {
        "Encryption:\n  \
         Generate a key:     openssl rand -hex 32 > key.hex\n  \
         Start encrypted:    dynoxide --db-path data.db --encryption-key-file key.hex\n  \
         Or via env var:     DYNOXIDE_ENCRYPTION_KEY=<key> dynoxide --db-path data.db\n\n\
         Connect your AWS SDK to http://<host>:<port>"
    }
    #[cfg(not(feature = "encryption"))]
    {
        "Connect your AWS SDK to http://<host>:<port>"
    }
}

// Compile error if no CLI feature is enabled — the binary needs at least one.
#[cfg(not(any(feature = "http-server", feature = "mcp-server", feature = "import")))]
compile_error!(
    "At least one of `http-server`, `mcp-server`, or `import` features must be enabled \
     to build the dynoxide binary."
);

#[cfg(any(feature = "http-server", feature = "mcp-server"))]
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

// When only the import feature is enabled (no async runtime needed)
#[cfg(all(
    feature = "import",
    not(feature = "http-server"),
    not(feature = "mcp-server")
))]
fn main() {
    if let Err(e) = run_sync() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

#[cfg(any(feature = "http-server", feature = "mcp-server"))]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        #[cfg(feature = "http-server")]
        Some(Commands::Serve(args)) => run_serve(args).await,

        #[cfg(feature = "mcp-server")]
        Some(Commands::Mcp(args)) => run_mcp(args).await,

        #[cfg(feature = "import")]
        Some(Commands::Import(args)) => run_import(args).await,

        #[cfg(feature = "http-server")]
        None => {
            // Backward compatibility: no subcommand = serve with top-level args
            let args = ServeArgs {
                host: cli.host.unwrap_or_else(|| "127.0.0.1".to_string()),
                port: cli.port.unwrap_or(8000),
                db_path: cli.db_path,
                encryption_key_file: cli.encryption_key_file,
                #[cfg(feature = "mcp-server")]
                mcp: false,
                #[cfg(feature = "mcp-server")]
                mcp_port: 19280,
                #[cfg(feature = "mcp-server")]
                mcp_read_only: false,
                #[cfg(feature = "mcp-server")]
                mcp_data_model: None,
            };
            run_serve(args).await
        }

        #[cfg(not(feature = "http-server"))]
        None => {
            eprintln!("No subcommand specified. Available subcommands:");
            #[cfg(feature = "mcp-server")]
            eprintln!("  mcp       Start the MCP server");
            #[cfg(feature = "import")]
            eprintln!("  import    Import DynamoDB Export data");
            std::process::exit(1);
        }

        #[allow(unreachable_patterns)]
        _ => unreachable!(),
    }
}

#[cfg(all(
    feature = "import",
    not(feature = "http-server"),
    not(feature = "mcp-server")
))]
fn run_sync() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        #[cfg(feature = "import")]
        Some(Commands::Import(args)) => run_import(args),

        None => {
            eprintln!("No subcommand specified. Available subcommands:");
            eprintln!("  import    Import DynamoDB Export data");
            std::process::exit(1);
        }
        #[allow(unreachable_patterns)]
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Serve subcommand (http-server feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "http-server")]
async fn run_serve(args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(feature = "encryption"))]
    reject_encryption_on_plain_build(args.encryption_key_file.as_ref())?;

    #[cfg(feature = "encryption")]
    let encryption_key =
        resolve_encryption_key(args.encryption_key_file.as_ref(), |k| std::env::var(k))?;

    #[cfg(feature = "encryption")]
    if encryption_key.is_some() && args.db_path.is_none() {
        return Err("encryption key provided but no --db-path specified.\n\
             Encryption requires a persistent database file. In-memory databases \
             are ephemeral and do not benefit from encryption at rest."
            .into());
    }

    let db = open_database(&args.db_path, {
        #[cfg(feature = "encryption")]
        {
            encryption_key.as_deref().map(|s| s.as_str())
        }
        #[cfg(not(feature = "encryption"))]
        {
            None::<&str>
        }
    })?;

    #[cfg(feature = "mcp-server")]
    if args.mcp {
        use tokio_util::sync::CancellationToken;

        let mcp_data_model = load_data_model(args.mcp_data_model.as_ref())?;
        let mcp_config = dynoxide::mcp::McpConfig {
            read_only: args.mcp_read_only,
            data_model: mcp_data_model,
            ..Default::default()
        };
        let mcp_port = args.mcp_port;
        let mcp_db = db.clone();
        let mcp_shutdown = CancellationToken::new();
        let mcp_shutdown_clone = mcp_shutdown.clone();

        eprintln!(
            "Starting DynamoDB HTTP server on {}:{} + MCP server on 127.0.0.1:{}",
            args.host, args.port, mcp_port
        );

        let (http_result, mcp_result) = tokio::join!(
            async {
                let r = dynoxide::server::start(&args.host, args.port, db).await;
                // HTTP server exited (Ctrl+C) — tell MCP to shut down too
                mcp_shutdown_clone.cancel();
                r
            },
            dynoxide::mcp::serve_http_with_shutdown(
                mcp_db,
                mcp_port,
                mcp_config,
                Some(mcp_shutdown),
            ),
        );
        http_result?;
        mcp_result?;
        return Ok(());
    }

    dynoxide::server::start(&args.host, args.port, db).await?;
    Ok(())
}

/// Reject encryption key sources on non-encryption builds with a helpful error.
#[cfg(all(
    any(feature = "http-server", feature = "mcp-server"),
    not(feature = "encryption")
))]
fn reject_encryption_on_plain_build(
    key_file: Option<&PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    if key_file.is_some() {
        return Err(
            "--encryption-key-file requires dynoxide built with encryption support.\n\
             Install with: cargo install dynoxide-rs --no-default-features \
             --features encrypted-server"
                .into(),
        );
    }

    if std::env::var("DYNOXIDE_ENCRYPTION_KEY")
        .ok()
        .filter(|k| !k.is_empty())
        .is_some()
    {
        return Err("DYNOXIDE_ENCRYPTION_KEY is set but this binary was built \
             without encryption support.\n\
             Install with: cargo install dynoxide-rs --no-default-features \
             --features encrypted-server"
            .into());
    }
    Ok(())
}

/// Load a data model from a schema file, if provided.
#[cfg(feature = "mcp-server")]
fn load_data_model(
    path: Option<&PathBuf>,
) -> Result<Option<dynoxide::schema::DataModel>, Box<dyn std::error::Error>> {
    match path {
        Some(p) => {
            let model = dynoxide::schema::onetable::parse_onetable_file(p)
                .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            eprintln!(
                "Loaded data model: {} ({} entities)",
                model.schema_format,
                model.entities.len()
            );
            Ok(Some(model))
        }
        None => Ok(None),
    }
}

/// Open the database, selecting the right constructor based on path and encryption key.
#[cfg(any(feature = "http-server", feature = "mcp-server"))]
fn open_database(
    db_path: &Option<String>,
    #[allow(unused_variables)] encryption_key: Option<&str>,
) -> Result<Database, Box<dyn std::error::Error>> {
    match db_path {
        #[cfg(feature = "encryption")]
        Some(path) if encryption_key.is_some() => {
            let key = encryption_key.unwrap();
            check_db_not_plaintext(path)?;
            Ok(Database::new_encrypted(path, key)
                .map_err(|e| wrap_encrypted_open_error(e, path))?)
        }
        Some(path) => Ok(Database::new(path)?),
        None => Ok(Database::memory()?),
    }
}

/// Check if an existing database file is unencrypted before attempting
/// encrypted open. New databases (file does not exist) proceed normally.
#[cfg(feature = "encryption")]
fn check_db_not_plaintext(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let Ok(mut file) = std::fs::File::open(path) else {
        return Ok(()); // file doesn't exist yet -- new DB, proceed
    };
    let mut header = [0u8; 16];
    if std::io::Read::read_exact(&mut file, &mut header).is_ok() && &header == b"SQLite format 3\0"
    {
        return Err(format!(
            "{path} is an unencrypted database. Encryption cannot be \
             added to an existing database in place. To start fresh \
             with encryption, use a new file path."
        )
        .into());
    }
    Ok(()) // file exists but doesn't have plaintext SQLite header -- may be encrypted
}

/// Wrap errors from Database::new_encrypted() with CLI-friendly messages.
#[cfg(feature = "encryption")]
fn wrap_encrypted_open_error(
    err: dynoxide::DynoxideError,
    path: &str,
) -> Box<dyn std::error::Error> {
    let msg = err.to_string();
    if msg.contains("not a database") || msg.contains("NotADatabase") {
        format!(
            "failed to open encrypted database at {path} -- \
             check that your encryption key is correct.\n\
             If this is a new database, ensure the path is writable."
        )
        .into()
    } else {
        Box::new(err)
    }
}

// ---------------------------------------------------------------------------
// MCP subcommand (mcp-server feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "mcp-server")]
async fn run_mcp(args: McpArgs) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(feature = "encryption"))]
    reject_encryption_on_plain_build(args.encryption_key_file.as_ref())?;

    #[cfg(feature = "encryption")]
    let encryption_key =
        resolve_encryption_key(args.encryption_key_file.as_ref(), |k| std::env::var(k))?;

    #[cfg(feature = "encryption")]
    if encryption_key.is_some() && args.db_path.is_none() {
        return Err("encryption key provided but no --db-path specified.\n\
             Encryption requires a persistent database file."
            .into());
    }

    let db = open_database(&args.db_path, {
        #[cfg(feature = "encryption")]
        {
            encryption_key.as_deref().map(|s| s.as_str())
        }
        #[cfg(not(feature = "encryption"))]
        {
            None::<&str>
        }
    })?;

    let data_model = load_data_model(args.data_model.as_ref())?;

    let mcp_config = dynoxide::mcp::McpConfig {
        read_only: args.read_only,
        max_items: args.max_items,
        max_size_bytes: args.max_size_bytes,
        data_model,
        data_model_summary_limit: args.data_model_summary_limit,
    };

    if args.http {
        dynoxide::mcp::serve_http(db, args.port, mcp_config).await
    } else {
        dynoxide::mcp::serve_stdio(db, mcp_config).await
    }
}

// ---------------------------------------------------------------------------
// Import subcommand (import feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "import")]
fn print_import_summary(summary: &dynoxide::import::ImportSummary) {
    eprintln!();
    eprintln!("Import complete:");
    eprintln!("  Tables: {}", summary.tables.len());
    eprintln!("  Items:  {}", summary.total_items);
    if summary.total_skipped > 0 {
        eprintln!("  Skipped: {}", summary.total_skipped);
    }
    if let Some(ref path) = summary.output_path {
        eprintln!("  Output: {}", path.display());
    }

    if !summary.warnings.is_empty() {
        eprintln!();
        eprintln!("Warnings:");
        for w in &summary.warnings {
            eprintln!("  - {w}");
        }
    }
}

/// Build an ImportCommand from CLI args.
#[cfg(feature = "import")]
fn build_import_command(args: &ImportArgs) -> dynoxide::import::ImportCommand {
    dynoxide::import::ImportCommand {
        source: args.source.clone(),
        output: args.output.clone(),
        schema: args.schema.clone(),
        rules: args.rules.clone(),
        tables: args.tables.clone(),
        compress: args.compress,
        force: args.force,
        continue_on_error: args.continue_on_error,
    }
}

/// Import subcommand — async version (when http-server or mcp-server is available).
/// Supports --serve and --mcp for import-then-serve workflows.
#[cfg(all(
    feature = "import",
    any(feature = "http-server", feature = "mcp-server")
))]
async fn run_import(args: ImportArgs) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "http-server")]
    let wants_serve = args.serve;
    #[cfg(not(feature = "http-server"))]
    let wants_serve = false;

    #[cfg(feature = "mcp-server")]
    let wants_mcp = args.mcp;
    #[cfg(not(feature = "mcp-server"))]
    let wants_mcp = false;

    if wants_serve || wants_mcp {
        // In-memory mode: import then serve
        let db = Database::memory()?;
        let cmd = build_import_command(&args);
        let summary = dynoxide::import::run_into(&db, cmd)?;
        print_import_summary(&summary);
        eprintln!();

        // Both --serve and --mcp: run HTTP + MCP concurrently
        #[cfg(all(feature = "http-server", feature = "mcp-server"))]
        if wants_serve && wants_mcp {
            use tokio_util::sync::CancellationToken;

            let mcp_data_model = load_data_model(args.mcp_data_model.as_ref())?;
            let mcp_config = dynoxide::mcp::McpConfig {
                read_only: args.mcp_read_only,
                data_model: mcp_data_model,
                ..Default::default()
            };
            let mcp_port = args.mcp_port;
            let mcp_db = db.clone();
            let mcp_shutdown = CancellationToken::new();
            let mcp_shutdown_clone = mcp_shutdown.clone();

            eprintln!(
                "Starting DynamoDB HTTP server on {}:{} + MCP server on 127.0.0.1:{}",
                args.host, args.port, mcp_port
            );

            let (http_result, mcp_result) = tokio::join!(
                async {
                    let r = dynoxide::server::start(&args.host, args.port, db).await;
                    mcp_shutdown_clone.cancel();
                    r
                },
                dynoxide::mcp::serve_http_with_shutdown(
                    mcp_db,
                    mcp_port,
                    mcp_config,
                    Some(mcp_shutdown),
                ),
            );
            http_result?;
            mcp_result?;
            return Ok(());
        }

        #[cfg(feature = "http-server")]
        if wants_serve {
            dynoxide::server::start(&args.host, args.port, db).await?;
            return Ok(());
        }

        #[cfg(feature = "mcp-server")]
        if wants_mcp {
            let mcp_data_model = load_data_model(args.mcp_data_model.as_ref())?;
            let mcp_config = dynoxide::mcp::McpConfig {
                data_model: mcp_data_model,
                ..Default::default()
            };
            dynoxide::mcp::serve_stdio(db, mcp_config).await?;
            return Ok(());
        }
    } else if args.output.is_some() {
        // File mode (current behavior)
        let cmd = build_import_command(&args);
        let summary = dynoxide::import::run(cmd)?;
        print_import_summary(&summary);
    } else {
        return Err("Either --output or --serve/--mcp is required.\n\
             Use --output <path> to write a database file, or\n\
             --serve to start an HTTP server with the imported data."
            .into());
    }

    Ok(())
}

/// Import subcommand — sync version (import feature only, no server features).
#[cfg(all(
    feature = "import",
    not(feature = "http-server"),
    not(feature = "mcp-server")
))]
fn run_import(args: ImportArgs) -> Result<(), Box<dyn std::error::Error>> {
    if args.output.is_none() {
        return Err(
            "--output is required. (Build with http-server or mcp-server feature \
             for --serve/--mcp support.)"
                .into(),
        );
    }

    let cmd = build_import_command(&args);
    let summary = dynoxide::import::run(cmd)?;
    print_import_summary(&summary);
    Ok(())
}

// ---------------------------------------------------------------------------
// Encryption key resolution (encryption builds only)
// ---------------------------------------------------------------------------

#[cfg(feature = "encryption")]
fn resolve_encryption_key(
    key_file: Option<&PathBuf>,
    env_fn: impl Fn(&str) -> Result<String, std::env::VarError>,
) -> Result<Option<Zeroizing<String>>, Box<dyn std::error::Error>> {
    let from_file = match key_file {
        Some(path) => Some(read_key_file(path)?),
        None => None,
    };

    let from_env = env_fn("DYNOXIDE_ENCRYPTION_KEY")
        .ok()
        .filter(|k| !k.is_empty())
        .map(Zeroizing::new);

    match (from_file, from_env) {
        (Some(_), Some(_)) => Err(format!(
            "both --encryption-key-file ({}) and DYNOXIDE_ENCRYPTION_KEY are set. \
             Provide only one key source.",
            key_file.unwrap().display()
        )
        .into()),
        (Some(key), None) | (None, Some(key)) => {
            validate_key(&key)?;
            Ok(Some(key))
        }
        (None, None) => Ok(None),
    }
}

#[cfg(feature = "encryption")]
fn read_key_file(path: &std::path::Path) -> Result<Zeroizing<String>, Box<dyn std::error::Error>> {
    let metadata = std::fs::metadata(path)
        .map_err(|e| format!("cannot read key file {}: {e}", path.display()))?;

    if !metadata.is_file() {
        return Err(format!("key file {} is not a regular file", path.display()).into());
    }

    if metadata.len() > 1024 {
        return Err(format!(
            "key file {} is {} bytes (max 1024). \
             A valid key file contains only a 64-character hex string.",
            path.display(),
            metadata.len()
        )
        .into());
    }

    // Warn on open permissions (Unix). Uses metadata from the path;
    // acceptable for a warning (not a security gate).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            eprintln!(
                "warning: key file {} has open permissions (mode {:04o}), \
                 consider chmod 600",
                path.display(),
                mode & 0o777,
            );
        }
    }

    // Read into Zeroizing<Vec<u8>> to ensure buffer is zeroized on drop
    let raw_bytes = Zeroizing::new(
        std::fs::read(path).map_err(|e| format!("cannot read key file {}: {e}", path.display()))?,
    );

    let raw_str = std::str::from_utf8(&raw_bytes)
        .map_err(|_| format!("key file {} is not valid UTF-8", path.display()))?;

    Ok(Zeroizing::new(raw_str.trim().to_string()))
    // raw_bytes (Zeroizing<Vec<u8>>) is zeroized when dropped here
}

#[cfg(feature = "encryption")]
fn validate_key(key: &str) -> Result<(), Box<dyn std::error::Error>> {
    if key.is_empty() {
        return Err("encryption key is empty".into());
    }

    // Detect common mistake: PEM file
    if key.starts_with("-----BEGIN") {
        return Err("key file appears to contain a PEM certificate/key. \
             Dynoxide requires a raw 64-character hex string. \
             Generate one with: openssl rand -hex 32"
            .into());
    }

    if key.len() != 64 {
        return Err(format!(
            "encryption key must be exactly 64 hex characters (got {}). \
             Generate a key with: openssl rand -hex 32",
            key.len()
        )
        .into());
    }

    if !key.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("encryption key contains non-hex characters. \
             Only 0-9 and a-f are allowed."
            .into());
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #[cfg(feature = "encryption")]
    use super::*;

    #[cfg(feature = "encryption")]
    mod validate_key_tests {
        use super::*;

        #[test]
        fn valid_key() {
            let key = "a".repeat(64);
            assert!(validate_key(&key).is_ok());
        }

        #[test]
        fn valid_key_mixed_case() {
            let key = "aAbBcCdDeEfF0123456789aAbBcCdDeEfF0123456789aAbBcCdDeEfF01234567";
            assert!(validate_key(key).is_ok());
        }

        #[test]
        fn empty_key() {
            let err = validate_key("").unwrap_err();
            assert!(err.to_string().contains("empty"));
        }

        #[test]
        fn pem_key() {
            let err = validate_key("-----BEGIN PRIVATE KEY-----\nfoo").unwrap_err();
            assert!(err.to_string().contains("PEM"));
        }

        #[test]
        fn wrong_length_short() {
            let key = "abcdef1234567890";
            let err = validate_key(key).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("64 hex characters"));
            assert!(msg.contains("(got 16)"));
        }

        #[test]
        fn wrong_length_long() {
            let key = "a".repeat(128);
            let err = validate_key(&key).unwrap_err();
            assert!(err.to_string().contains("(got 128)"));
        }

        #[test]
        fn non_hex_characters() {
            let key = "g".repeat(64);
            let err = validate_key(&key).unwrap_err();
            assert!(err.to_string().contains("non-hex"));
        }
    }

    #[cfg(feature = "encryption")]
    mod read_key_file_tests {
        use super::*;
        use std::io::Write;

        #[test]
        fn valid_key_file() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("key.hex");
            std::fs::write(&path, "a".repeat(64)).unwrap();
            let key = read_key_file(&path).unwrap();
            assert_eq!(key.len(), 64);
        }

        #[test]
        fn key_file_with_trailing_newline() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("key.hex");
            let mut f = std::fs::File::create(&path).unwrap();
            write!(f, "{}\n", "b".repeat(64)).unwrap();
            let key = read_key_file(&path).unwrap();
            assert_eq!(key.len(), 64);
            assert_eq!(&*key, &"b".repeat(64));
        }

        #[test]
        fn key_file_with_leading_and_trailing_whitespace() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("key.hex");
            std::fs::write(&path, format!("  {} \n", "c".repeat(64))).unwrap();
            let key = read_key_file(&path).unwrap();
            assert_eq!(key.len(), 64);
        }

        #[test]
        fn oversized_key_file() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("big.hex");
            std::fs::write(&path, "a".repeat(2048)).unwrap();
            let err = read_key_file(&path).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("2048 bytes"));
            assert!(msg.contains("max 1024"));
        }

        #[test]
        fn nonexistent_key_file() {
            let path = PathBuf::from("/tmp/dynoxide-test-nonexistent-key-file");
            let err = read_key_file(&path).unwrap_err();
            assert!(err.to_string().contains("cannot read key file"));
        }

        #[test]
        fn directory_as_key_file() {
            let dir = tempfile::tempdir().unwrap();
            let err = read_key_file(dir.path()).unwrap_err();
            assert!(err.to_string().contains("not a regular file"));
        }

        #[test]
        fn non_utf8_key_file() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("key.hex");
            std::fs::write(&path, &[0xFF, 0xFE, 0xFD]).unwrap();
            let err = read_key_file(&path).unwrap_err();
            assert!(err.to_string().contains("not valid UTF-8"));
        }
    }

    #[cfg(feature = "encryption")]
    mod resolve_key_tests {
        use super::*;

        /// Env reader that simulates the variable not being set.
        fn env_not_set(_: &str) -> Result<String, std::env::VarError> {
            Err(std::env::VarError::NotPresent)
        }

        /// Returns an env reader that yields the given value.
        fn env_with(val: &str) -> impl Fn(&str) -> Result<String, std::env::VarError> {
            let owned = val.to_string();
            move |_| Ok(owned.clone())
        }

        #[test]
        fn no_key_sources() {
            let result = resolve_encryption_key(None, env_not_set).unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn key_from_file_only() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("key.hex");
            std::fs::write(&path, "a".repeat(64)).unwrap();
            let result = resolve_encryption_key(Some(&path), env_not_set).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().len(), 64);
        }

        #[test]
        fn key_from_env_only() {
            let result = resolve_encryption_key(None, env_with(&"a".repeat(64))).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().len(), 64);
        }

        #[test]
        fn both_sources_errors() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("key.hex");
            std::fs::write(&path, "a".repeat(64)).unwrap();

            let err = resolve_encryption_key(Some(&path), env_with(&"b".repeat(64))).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("both"));
            assert!(msg.contains("--encryption-key-file"));
            assert!(msg.contains("DYNOXIDE_ENCRYPTION_KEY"));
        }

        #[test]
        fn empty_env_var_is_ignored() {
            let result = resolve_encryption_key(None, env_with("")).unwrap();
            assert!(result.is_none());
        }
    }

    #[cfg(feature = "encryption")]
    mod check_db_tests {
        use super::*;
        use dynoxide::Database;

        #[test]
        fn nonexistent_file_ok() {
            assert!(check_db_not_plaintext("/tmp/dynoxide-nonexistent-db-test").is_ok());
        }

        #[test]
        fn unencrypted_db_detected() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("plain.db");
            // Create a real unencrypted SQLite database
            let _db = Database::new(path.to_str().unwrap()).unwrap();
            drop(_db);
            let err = check_db_not_plaintext(path.to_str().unwrap()).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("unencrypted database"));
            assert!(msg.contains("new file path"));
        }

        #[test]
        fn non_sqlite_file_passes() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("random.db");
            std::fs::write(&path, b"this is not a sqlite file at all").unwrap();
            assert!(check_db_not_plaintext(path.to_str().unwrap()).is_ok());
        }

        #[test]
        fn small_file_passes() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tiny.db");
            std::fs::write(&path, b"tiny").unwrap();
            // File is too small to have full header -- read_exact fails, returns Ok
            assert!(check_db_not_plaintext(path.to_str().unwrap()).is_ok());
        }
    }
}
