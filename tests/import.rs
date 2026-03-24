//! Integration tests for the import CLI pipeline.

#[cfg(feature = "import")]
mod tests {
    use dynoxide::import::{self, ImportCommand};
    use std::io::Write;

    /// Create a temporary DynamoDB Export directory structure with test data.
    fn setup_export_dir(dir: &std::path::Path, table_name: &str, items_json: &[&str]) {
        let data_dir = dir.join(table_name).join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let file_path = data_dir.join("00000000.json");
        let mut f = std::fs::File::create(&file_path).unwrap();
        for item in items_json {
            writeln!(f, "{item}").unwrap();
        }
    }

    /// Create a gzipped export file.
    fn setup_gzipped_export(dir: &std::path::Path, table_name: &str, items_json: &[&str]) {
        use flate2::Compression;
        use flate2::write::GzEncoder;

        let data_dir = dir.join(table_name).join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let file_path = data_dir.join("00000000.json.gz");
        let f = std::fs::File::create(&file_path).unwrap();
        let mut encoder = GzEncoder::new(f, Compression::default());
        for item in items_json {
            writeln!(encoder, "{item}").unwrap();
        }
        encoder.finish().unwrap();
    }

    /// Create a schema file from DescribeTable-style JSON.
    fn create_schema_file(path: &std::path::Path, schemas: &[serde_json::Value]) {
        let json = serde_json::to_string_pretty(schemas).unwrap();
        std::fs::write(path, json).unwrap();
    }

    fn simple_table_schema(table_name: &str) -> serde_json::Value {
        serde_json::json!({
            "Table": {
                "TableName": table_name,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"}
                ]
            }
        })
    }

    #[test]
    fn test_basic_import() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");

        // Setup export data
        setup_export_dir(
            &source,
            "Users",
            &[
                r#"{"Item": {"pk": {"S": "USER#1"}, "sk": {"S": "PROFILE"}, "name": {"S": "Alice"}}}"#,
                r#"{"Item": {"pk": {"S": "USER#2"}, "sk": {"S": "PROFILE"}, "name": {"S": "Bob"}}}"#,
            ],
        );

        // Setup schema
        create_schema_file(&schema_file, &[simple_table_schema("Users")]);

        // Run import
        let summary = import::run(ImportCommand {
            source,
            output: Some(output.clone()),
            schema: schema_file,
            rules: None,
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
        })
        .unwrap();

        assert_eq!(summary.total_items, 2);
        assert_eq!(summary.total_skipped, 0);
        assert_eq!(summary.tables.len(), 1);
        assert_eq!(summary.tables[0].table_name, "Users");
        assert_eq!(summary.tables[0].items_imported, 2);

        // Verify the output database is readable
        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        let tables = db
            .list_tables(dynoxide::actions::list_tables::ListTablesRequest::default())
            .unwrap();
        assert_eq!(tables.table_names.len(), 1);
        assert_eq!(tables.table_names[0], "Users");

        // Verify items
        let scan = db
            .scan(dynoxide::actions::scan::ScanRequest {
                table_name: "Users".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(scan.count, 2);
    }

    #[test]
    fn test_import_gzipped_files() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");

        // Setup gzipped export data
        setup_gzipped_export(
            &source,
            "Orders",
            &[
                r#"{"Item": {"pk": {"S": "ORDER#1"}, "sk": {"S": "META"}, "total": {"N": "42.50"}}}"#,
                r#"{"Item": {"pk": {"S": "ORDER#2"}, "sk": {"S": "META"}, "total": {"N": "99.99"}}}"#,
                r#"{"Item": {"pk": {"S": "ORDER#3"}, "sk": {"S": "META"}, "total": {"N": "10.00"}}}"#,
            ],
        );

        create_schema_file(&schema_file, &[simple_table_schema("Orders")]);

        let summary = import::run(ImportCommand {
            source,
            output: Some(output),
            schema: schema_file,
            rules: None,
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
        })
        .unwrap();

        assert_eq!(summary.total_items, 3);
    }

    #[test]
    fn test_import_with_table_filter() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");

        // Setup two tables
        setup_export_dir(
            &source,
            "Users",
            &[r#"{"Item": {"pk": {"S": "U#1"}, "sk": {"S": "P"}}}"#],
        );
        setup_export_dir(
            &source,
            "Orders",
            &[r#"{"Item": {"pk": {"S": "O#1"}, "sk": {"S": "M"}}}"#],
        );

        create_schema_file(
            &schema_file,
            &[simple_table_schema("Users"), simple_table_schema("Orders")],
        );

        // Import only Users
        let summary = import::run(ImportCommand {
            source,
            output: Some(output),
            schema: schema_file,
            rules: None,
            tables: Some(vec!["Users".to_string()]),
            compress: false,
            force: false,
            continue_on_error: false,
        })
        .unwrap();

        assert_eq!(summary.tables.len(), 1);
        assert_eq!(summary.tables[0].table_name, "Users");
    }

    #[test]
    fn test_import_with_anonymisation() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "Users",
            &[
                r#"{"Item": {"pk": {"S": "USER#1"}, "sk": {"S": "PROFILE"}, "email": {"S": "alice@example.com"}, "name": {"S": "Alice Smith"}}}"#,
                r#"{"Item": {"pk": {"S": "USER#2"}, "sk": {"S": "PROFILE"}, "email": {"S": "bob@example.com"}, "notes": {"S": "Some private notes"}}}"#,
            ],
        );

        create_schema_file(&schema_file, &[simple_table_schema("Users")]);

        // Write rules TOML
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }

[[rules]]
match = "attribute_exists(notes)"
path = "notes"
action = { type = "redact" }
"#,
        )
        .unwrap();

        let summary = import::run(ImportCommand {
            source,
            output: Some(output.clone()),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
        })
        .unwrap();

        assert_eq!(summary.total_items, 2);

        // Verify anonymisation
        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        let scan = db
            .scan(dynoxide::actions::scan::ScanRequest {
                table_name: "Users".to_string(),
                ..Default::default()
            })
            .unwrap();

        for item in scan.items.as_ref().unwrap() {
            // Email should be anonymised (not the original)
            if let Some(dynoxide::AttributeValue::S(email)) = item.get("email") {
                assert_ne!(email, "alice@example.com");
                assert_ne!(email, "bob@example.com");
            }
            // Notes should be redacted
            if let Some(dynoxide::AttributeValue::S(notes)) = item.get("notes") {
                assert_eq!(notes, "[REDACTED]");
            }
        }
    }

    #[test]
    fn test_import_with_cross_table_consistency() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        // Same email appears in both tables
        setup_export_dir(
            &source,
            "Users",
            &[
                r#"{"Item": {"pk": {"S": "USER#1"}, "sk": {"S": "P"}, "email": {"S": "shared@example.com"}}}"#,
            ],
        );
        setup_export_dir(
            &source,
            "Orders",
            &[
                r#"{"Item": {"pk": {"S": "ORD#1"}, "sk": {"S": "M"}, "email": {"S": "shared@example.com"}}}"#,
            ],
        );

        create_schema_file(
            &schema_file,
            &[simple_table_schema("Users"), simple_table_schema("Orders")],
        );

        // SAFETY: single-threaded test, no concurrent env reads
        unsafe { std::env::set_var("TEST_HASH_SALT", "test-salt-value") };
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "hash", salt_env = "TEST_HASH_SALT" }

[consistency]
fields = ["email"]
"#,
        )
        .unwrap();

        let summary = import::run(ImportCommand {
            source,
            output: Some(output.clone()),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
        })
        .unwrap();

        assert_eq!(summary.total_items, 2);

        // Both tables should have the same hashed email
        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();

        let users_scan = db
            .scan(dynoxide::actions::scan::ScanRequest {
                table_name: "Users".to_string(),
                ..Default::default()
            })
            .unwrap();
        let orders_scan = db
            .scan(dynoxide::actions::scan::ScanRequest {
                table_name: "Orders".to_string(),
                ..Default::default()
            })
            .unwrap();

        let user_email = users_scan.items.as_ref().unwrap()[0].get("email").unwrap();
        let order_email = orders_scan.items.as_ref().unwrap()[0].get("email").unwrap();

        assert_eq!(
            user_email, order_email,
            "same email should hash to same value across tables"
        );
        // Should not be the original
        assert_ne!(
            user_email,
            &dynoxide::AttributeValue::S("shared@example.com".to_string())
        );
    }

    #[test]
    fn test_import_malformed_lines_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");

        setup_export_dir(
            &source,
            "Users",
            &[
                r#"{"Item": {"pk": {"S": "U#1"}, "sk": {"S": "P"}}}"#,
                "this is not valid json",
                r#"{"Item": {"pk": {"S": "U#2"}, "sk": {"S": "P"}}}"#,
                r#"{"MissingItemField": {}}"#,
            ],
        );

        create_schema_file(&schema_file, &[simple_table_schema("Users")]);

        let summary = import::run(ImportCommand {
            source,
            output: Some(output),
            schema: schema_file,
            rules: None,
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
        })
        .unwrap();

        assert_eq!(summary.total_items, 2);
        assert_eq!(summary.total_skipped, 2);
        assert_eq!(summary.warnings.len(), 2);
    }

    #[test]
    fn test_import_with_compression() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");

        setup_export_dir(
            &source,
            "Users",
            &[r#"{"Item": {"pk": {"S": "U#1"}, "sk": {"S": "P"}, "data": {"S": "hello world"}}}"#],
        );

        create_schema_file(&schema_file, &[simple_table_schema("Users")]);

        let summary = import::run(ImportCommand {
            source,
            output: Some(output.clone()),
            schema: schema_file,
            rules: None,
            tables: None,
            compress: true,
            force: false,
            continue_on_error: false,
        })
        .unwrap();

        assert_eq!(summary.total_items, 1);

        // Output should be the compressed file
        let expected_path = tmp.path().join("output.db.zst");
        assert_eq!(summary.output_path, Some(expected_path.clone()));
        assert!(expected_path.exists());
        // Original should be removed
        assert!(!output.exists());
    }

    #[test]
    fn test_import_missing_schema_error() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");

        setup_export_dir(
            &source,
            "Users",
            &[r#"{"Item": {"pk": {"S": "U#1"}, "sk": {"S": "P"}}}"#],
        );

        // Schema for a different table
        create_schema_file(&schema_file, &[simple_table_schema("Orders")]);

        let result = import::run(ImportCommand {
            source,
            output: Some(output),
            schema: schema_file,
            rules: None,
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
        });

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No schema found for table 'Users'")
        );
    }

    #[test]
    fn test_import_into_memory_database() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let schema_file = tmp.path().join("schema.json");

        setup_export_dir(
            &source,
            "Users",
            &[
                r#"{"Item": {"pk": {"S": "USER#1"}, "sk": {"S": "PROFILE"}, "name": {"S": "Alice"}}}"#,
                r#"{"Item": {"pk": {"S": "USER#2"}, "sk": {"S": "PROFILE"}, "name": {"S": "Bob"}}}"#,
            ],
        );

        create_schema_file(&schema_file, &[simple_table_schema("Users")]);

        // Import into an in-memory database using run_into
        let db = dynoxide::Database::memory().unwrap();
        let summary = import::run_into(
            &db,
            ImportCommand {
                source,
                output: None,
                schema: schema_file,
                rules: None,
                tables: None,
                compress: false,
                force: false,
                continue_on_error: false,
            },
        )
        .unwrap();

        assert_eq!(summary.total_items, 2);
        assert_eq!(summary.total_skipped, 0);
        assert!(summary.output_path.is_none());

        // Verify the in-memory database has the data
        let tables = db
            .list_tables(dynoxide::actions::list_tables::ListTablesRequest::default())
            .unwrap();
        assert_eq!(tables.table_names.len(), 1);
        assert_eq!(tables.table_names[0], "Users");

        let scan = db
            .scan(dynoxide::actions::scan::ScanRequest {
                table_name: "Users".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(scan.count, 2);
    }
}
