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

    // ---------------------------------------------------------------------------
    // scaffold_from_schema tests
    // ---------------------------------------------------------------------------

    #[test]
    fn test_scaffold_creates_table() {
        let tmp = tempfile::tempdir().unwrap();
        let schema_file = tmp.path().join("schema.json");
        create_schema_file(&schema_file, &[simple_table_schema("Users")]);

        let db = dynoxide::Database::memory().unwrap();
        let n = import::scaffold_from_schema(&db, &schema_file).unwrap();
        assert_eq!(n, 1);

        let tables = db
            .list_tables(dynoxide::actions::list_tables::ListTablesRequest::default())
            .unwrap();
        assert_eq!(tables.table_names, vec!["Users"]);
    }

    #[test]
    fn test_scaffold_multiple_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let schema_file = tmp.path().join("schema.json");
        create_schema_file(
            &schema_file,
            &[
                simple_table_schema("Users"),
                simple_table_schema("Orders"),
                simple_table_schema("Products"),
            ],
        );

        let db = dynoxide::Database::memory().unwrap();
        let n = import::scaffold_from_schema(&db, &schema_file).unwrap();
        assert_eq!(n, 3);

        let mut tables = db
            .list_tables(dynoxide::actions::list_tables::ListTablesRequest::default())
            .unwrap()
            .table_names;
        tables.sort();
        assert_eq!(tables, vec!["Orders", "Products", "Users"]);
    }

    #[test]
    fn test_scaffold_skips_existing_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let schema_file = tmp.path().join("schema.json");
        create_schema_file(&schema_file, &[simple_table_schema("Users")]);

        let db = dynoxide::Database::memory().unwrap();

        // First call creates the table.
        let n = import::scaffold_from_schema(&db, &schema_file).unwrap();
        assert_eq!(n, 1);

        // Second call should succeed and skip the already-existing table.
        let n = import::scaffold_from_schema(&db, &schema_file).unwrap();
        assert_eq!(n, 0);

        // Still exactly one table.
        let tables = db
            .list_tables(dynoxide::actions::list_tables::ListTablesRequest::default())
            .unwrap();
        assert_eq!(tables.table_names.len(), 1);
    }

    #[test]
    fn test_scaffold_with_gsi() {
        let tmp = tempfile::tempdir().unwrap();
        let schema_file = tmp.path().join("schema.json");
        let schema = serde_json::json!({
            "Table": {
                "TableName": "Events",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "gsi1pk", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [{
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": "gsi1pk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }]
            }
        });
        create_schema_file(&schema_file, &[schema]);

        let db = dynoxide::Database::memory().unwrap();
        let n = import::scaffold_from_schema(&db, &schema_file).unwrap();
        assert_eq!(n, 1);

        let info = db
            .describe_table(dynoxide::actions::describe_table::DescribeTableRequest {
                table_name: "Events".to_string(),
            })
            .unwrap();
        assert!(info.table.global_secondary_indexes.is_some());
        assert_eq!(info.table.global_secondary_indexes.unwrap().len(), 1);
    }

    #[test]
    fn test_scaffold_with_lsi() {
        // `created_at` is used only by the LSI's sort key, not by the table's
        // own key schema. A hand-parsed CreateTableRequest that drops
        // LocalSecondaryIndexes leaves it orphaned and table creation fails.
        let tmp = tempfile::tempdir().unwrap();
        let schema_file = tmp.path().join("schema.json");
        let schema = serde_json::json!({
            "Table": {
                "TableName": "Orders",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "created_at", "AttributeType": "S"}
                ],
                "LocalSecondaryIndexes": [{
                    "IndexName": "by-created-at",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }]
            }
        });
        create_schema_file(&schema_file, &[schema]);

        let db = dynoxide::Database::memory().unwrap();
        let n = import::scaffold_from_schema(&db, &schema_file).unwrap();
        assert_eq!(n, 1);

        let info = db
            .describe_table(dynoxide::actions::describe_table::DescribeTableRequest {
                table_name: "Orders".to_string(),
            })
            .unwrap();
        assert!(info.table.local_secondary_indexes.is_some());
        assert_eq!(info.table.local_secondary_indexes.unwrap().len(), 1);
    }

    #[test]
    fn test_scaffold_billing_mode_and_table_class_from_describe_table() {
        // DescribeTable wraps billing mode and table class in summary objects
        // and reports zeroed ProvisionedThroughput blocks for on-demand tables.
        // The schema path must unwrap the summaries and drop the throughput
        // blocks, or the table comes back PROVISIONED / STANDARD.
        let tmp = tempfile::tempdir().unwrap();
        let schema_file = tmp.path().join("schema.json");
        let schema = serde_json::json!({
            "Table": {
                "TableName": "OnDemand",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "gsi1pk", "AttributeType": "S"}
                ],
                "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"},
                "TableClassSummary": {"TableClass": "STANDARD_INFREQUENT_ACCESS"},
                "ProvisionedThroughput": {
                    "NumberOfDecreasesToday": 0,
                    "ReadCapacityUnits": 0,
                    "WriteCapacityUnits": 0
                },
                "GlobalSecondaryIndexes": [{
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": "gsi1pk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "NumberOfDecreasesToday": 0,
                        "ReadCapacityUnits": 0,
                        "WriteCapacityUnits": 0
                    }
                }]
            }
        });
        create_schema_file(&schema_file, &[schema]);

        let db = dynoxide::Database::memory().unwrap();
        let n = import::scaffold_from_schema(&db, &schema_file).unwrap();
        assert_eq!(n, 1);

        let info = db
            .describe_table(dynoxide::actions::describe_table::DescribeTableRequest {
                table_name: "OnDemand".to_string(),
            })
            .unwrap();
        let billing = info
            .table
            .billing_mode_summary
            .expect("BillingModeSummary should survive the schema round trip");
        assert_eq!(billing.billing_mode, "PAY_PER_REQUEST");
        let class = info
            .table
            .table_class_summary
            .expect("TableClassSummary should survive the schema round trip");
        assert_eq!(class.table_class, "STANDARD_INFREQUENT_ACCESS");
    }

    #[test]
    fn test_scaffold_keeps_provisioned_throughput_for_provisioned_tables() {
        // The strip only applies to on-demand tables. A provisioned table's
        // describe output carries real capacity values, and they must survive.
        let tmp = tempfile::tempdir().unwrap();
        let schema_file = tmp.path().join("schema.json");
        let schema = serde_json::json!({
            "Table": {
                "TableName": "Provisioned",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "gsi1pk", "AttributeType": "S"}
                ],
                "BillingModeSummary": {"BillingMode": "PROVISIONED"},
                "ProvisionedThroughput": {
                    "NumberOfDecreasesToday": 0,
                    "ReadCapacityUnits": 7,
                    "WriteCapacityUnits": 3
                },
                "GlobalSecondaryIndexes": [{
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": "gsi1pk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "NumberOfDecreasesToday": 0,
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 2
                    }
                }]
            }
        });
        create_schema_file(&schema_file, &[schema]);

        let db = dynoxide::Database::memory().unwrap();
        let n = import::scaffold_from_schema(&db, &schema_file).unwrap();
        assert_eq!(n, 1);

        let info = db
            .describe_table(dynoxide::actions::describe_table::DescribeTableRequest {
                table_name: "Provisioned".to_string(),
            })
            .unwrap();
        let pt = info
            .table
            .provisioned_throughput
            .expect("provisioned throughput should survive the schema round trip");
        assert_eq!(pt.read_capacity_units, 7);
        assert_eq!(pt.write_capacity_units, 3);
    }

    #[test]
    fn test_scaffold_rejects_inconsistent_create_table_shaped_schema() {
        // A schema already in CreateTable shape passes through untouched, so
        // a top-level PAY_PER_REQUEST paired with ProvisionedThroughput still
        // fails validation, exactly as it would on the CreateTable API.
        let tmp = tempfile::tempdir().unwrap();
        let schema_file = tmp.path().join("schema.json");
        let schema = serde_json::json!({
            "TableName": "Inconsistent",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            }
        });
        create_schema_file(&schema_file, &[schema]);

        let db = dynoxide::Database::memory().unwrap();
        let err = import::scaffold_from_schema(&db, &schema_file).unwrap_err();
        assert!(
            err.to_string().contains("PAY_PER_REQUEST"),
            "expected the CreateTable validation error, got: {err}"
        );
    }

    #[test]
    fn test_scaffold_round_trips_own_describe_output() {
        // A table's own DescribeTable output, used as a schema file, must
        // recreate the table without degrading billing mode or table class.
        let source_db = dynoxide::Database::memory().unwrap();
        source_db
            .create_table(dynoxide::actions::create_table::CreateTableRequest {
                table_name: "RoundTrip".to_string(),
                key_schema: vec![dynoxide::types::KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: dynoxide::types::KeyType::HASH,
                }],
                attribute_definitions: vec![dynoxide::types::AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: dynoxide::types::ScalarAttributeType::S,
                }],
                billing_mode: Some("PAY_PER_REQUEST".to_string()),
                table_class: Some("STANDARD_INFREQUENT_ACCESS".to_string()),
                ..Default::default()
            })
            .unwrap();
        let described = source_db
            .describe_table(dynoxide::actions::describe_table::DescribeTableRequest {
                table_name: "RoundTrip".to_string(),
            })
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let schema_file = tmp.path().join("schema.json");
        let schema = serde_json::json!({
            "Table": serde_json::to_value(&described.table).unwrap()
        });
        create_schema_file(&schema_file, &[schema]);

        let db = dynoxide::Database::memory().unwrap();
        let n = import::scaffold_from_schema(&db, &schema_file).unwrap();
        assert_eq!(n, 1);

        let info = db
            .describe_table(dynoxide::actions::describe_table::DescribeTableRequest {
                table_name: "RoundTrip".to_string(),
            })
            .unwrap();
        let billing = info
            .table
            .billing_mode_summary
            .expect("billing mode should survive a describe-scaffold round trip");
        assert_eq!(billing.billing_mode, "PAY_PER_REQUEST");
        let class = info
            .table
            .table_class_summary
            .expect("table class should survive a describe-scaffold round trip");
        assert_eq!(class.table_class, "STANDARD_INFREQUENT_ACCESS");
    }

    #[test]
    fn test_scaffold_missing_file_errors() {
        let db = dynoxide::Database::memory().unwrap();
        let result = import::scaffold_from_schema(
            &db,
            std::path::Path::new("/tmp/dynoxide-nonexistent-schema-file.json"),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to read schema file")
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
