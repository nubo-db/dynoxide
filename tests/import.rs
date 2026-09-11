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
            data_model: None,
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
            data_model: None,
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
            data_model: None,
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
            data_model: None,
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
        unsafe { std::env::set_var("TEST_HASH_SALT", "test-salt-value-0123456789") };
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
            data_model: None,
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
            data_model: None,
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
            data_model: None,
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
            data_model: None,
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
    fn test_scaffold_round_trips_a_vector_index_alongside_a_gsi_and_lsi() {
        // A table's own DescribeTable output, fed back in as a schema file,
        // must rebuild every index type. The round trip is the invariant: it
        // catches a dropped field without anyone maintaining a list of fields.
        let source_db = dynoxide::Database::memory().unwrap();
        let create = serde_json::json!({
            "TableName": "VecRoundTrip",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsiPk", "AttributeType": "S"},
                {"AttributeName": "lsiSk", "AttributeType": "S"},
                {"AttributeName": "tenant", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gsiPk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "LocalSecondaryIndexes": [{
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsiSk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }],
            "VectorIndexes": [{
                "IndexName": "vix",
                "VectorAttribute": {"AttributeName": "embedding"},
                "SearchSchema": [
                    {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}
                ],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["label"]},
                "Dimensions": 8,
                "DistanceFunction": "EUCLIDEAN"
            }]
        });
        source_db
            .create_table(serde_json::from_value(create).unwrap())
            .unwrap();
        let described = source_db
            .describe_table(dynoxide::actions::describe_table::DescribeTableRequest {
                table_name: "VecRoundTrip".to_string(),
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
                table_name: "VecRoundTrip".to_string(),
            })
            .unwrap();

        // A vector index forces PAY_PER_REQUEST, and DescribeTable reports a
        // zeroed ProvisionedThroughput block beside it that CreateTable
        // rejects. The round trip only works because the billing-mode hoist
        // strips that block, so this covers the seam between the two.
        let billing = info
            .table
            .billing_mode_summary
            .expect("billing mode survives");
        assert_eq!(billing.billing_mode, "PAY_PER_REQUEST");

        let gsis = info.table.global_secondary_indexes.expect("GSI survives");
        assert_eq!(gsis.len(), 1);
        assert_eq!(gsis[0].index_name, "gsi1");
        let lsis = info.table.local_secondary_indexes.expect("LSI survives");
        assert_eq!(lsis.len(), 1);
        assert_eq!(lsis[0].index_name, "lsi1");

        let vixs = info.table.vector_indexes.expect("vector index survives");
        assert_eq!(vixs.len(), 1);
        let vix = &vixs[0];
        assert_eq!(vix.index_name, "vix");
        assert_eq!(vix.vector_attribute.attribute_name, "embedding");
        assert_eq!(vix.dimensions, 8);
        assert_eq!(vix.distance_function, "EUCLIDEAN");
        assert_eq!(
            vix.projection.projection_type,
            Some(dynoxide::types::ProjectionType::INCLUDE)
        );
        assert_eq!(
            vix.projection.non_key_attributes.as_deref(),
            Some(["label".to_string()].as_slice())
        );
        let schema_elems = vix.search_schema.as_ref().expect("SearchSchema survives");
        assert_eq!(schema_elems.len(), 1);
        assert_eq!(schema_elems[0].attribute_name, "tenant");
        assert_eq!(schema_elems[0].search_schema_element_type, "HASH");
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
                data_model: None,
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

    /// Single-table schema with a GSI1 on gs1pk/gs1sk, matching the OneTable
    /// fixture in tests/fixtures/onetable-test-schema.json.
    fn single_table_schema(table_name: &str) -> serde_json::Value {
        serde_json::json!({
            "Table": {
                "TableName": table_name,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "gs1pk", "AttributeType": "S"},
                    {"AttributeName": "gs1sk", "AttributeType": "S"},
                    {"AttributeName": "gs2pk", "AttributeType": "S"},
                    {"AttributeName": "gs2sk", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [
                    {
                        "IndexName": "GSI1",
                        "KeySchema": [
                            {"AttributeName": "gs1pk", "KeyType": "HASH"},
                            {"AttributeName": "gs1sk", "KeyType": "RANGE"}
                        ],
                        "Projection": {"ProjectionType": "ALL"}
                    },
                    {
                        "IndexName": "GSI2",
                        "KeySchema": [
                            {"AttributeName": "gs2pk", "KeyType": "HASH"},
                            {"AttributeName": "gs2sk", "KeyType": "RANGE"}
                        ],
                        "Projection": {"ProjectionType": "KEYS_ONLY"}
                    }
                ]
            }
        })
    }

    fn onetable_fixture() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/onetable-test-schema.json")
    }

    fn scan_all(db: &dynoxide::Database, table: &str) -> Vec<dynoxide::Item> {
        db.scan(dynoxide::actions::scan::ScanRequest {
            table_name: table.to_string(),
            ..Default::default()
        })
        .unwrap()
        .items
        .unwrap()
    }

    fn string_attr(item: &dynoxide::Item, name: &str) -> String {
        match item.get(name) {
            Some(dynoxide::AttributeValue::S(s)) => s.clone(),
            other => panic!("{name} should be a string, got {other:?}"),
        }
    }

    #[test]
    fn test_rule_values_scope_a_match_by_key_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "App",
            &[
                r#"{"Item": {"pk": {"S": "CUSTOMER#1"}, "sk": {"S": "PROFILE"}, "notes": {"S": "customer notes"}}}"#,
                r#"{"Item": {"pk": {"S": "ORDER#1"}, "sk": {"S": "PROFILE"}, "notes": {"S": "order notes"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[simple_table_schema("App")]);

        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "begins_with(pk, :prefix)"
values = { ":prefix" = "CUSTOMER#" }
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
            data_model: None,
        })
        .unwrap();
        assert_eq!(summary.total_items, 2);

        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        for item in scan_all(&db, "App") {
            let pk = string_attr(&item, "pk");
            let notes = string_attr(&item, "notes");
            if pk.starts_with("CUSTOMER#") {
                assert_eq!(notes, "[REDACTED]", "rule should reach the customer item");
            } else {
                assert_eq!(notes, "order notes", "rule must not reach the order item");
            }
        }
    }

    #[test]
    fn test_rule_values_must_cover_every_reference() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "App",
            &[r#"{"Item": {"pk": {"S": "CUSTOMER#1"}, "sk": {"S": "PROFILE"}}}"#],
        );
        create_schema_file(&schema_file, &[simple_table_schema("App")]);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "begins_with(pk, :prefix)"
path = "sk"
action = { type = "redact" }
"#,
        )
        .unwrap();

        let err = import::run(ImportCommand {
            source,
            output: Some(tmp.path().join("output.db")),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: None,
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Rule 1"), "{msg}");
        assert!(msg.contains(":prefix"), "{msg}");
    }

    #[test]
    fn test_data_model_rederives_keys_from_anonymised_attributes() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "App",
            &[
                r#"{"Item": {"_type": {"S": "User"}, "pk": {"S": "account#acc1"}, "sk": {"S": "user#alice@example.com"}, "gs1pk": {"S": "user#alice@example.com"}, "gs1sk": {"S": "user#"}, "accountId": {"S": "acc1"}, "email": {"S": "alice@example.com"}, "role": {"S": "admin"}}}"#,
                r#"{"Item": {"_type": {"S": "Account"}, "pk": {"S": "account#acc1"}, "sk": {"S": "account#"}, "id": {"S": "acc1"}, "name": {"S": "Acme"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[single_table_schema("App")]);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }
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
            data_model: Some(onetable_fixture()),
        })
        .unwrap();
        assert_eq!(summary.total_items, 2);
        assert!(
            summary.warnings.is_empty(),
            "keys that reproduce from their templates warn about nothing: {:?}",
            summary.warnings
        );

        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        let items = scan_all(&db, "App");
        let user = items
            .iter()
            .find(|i| string_attr(i, "_type") == "User")
            .unwrap();
        let account = items
            .iter()
            .find(|i| string_attr(i, "_type") == "Account")
            .unwrap();

        let email = string_attr(user, "email");
        assert_ne!(email, "alice@example.com");
        assert_eq!(string_attr(user, "pk"), "account#acc1");
        assert_eq!(string_attr(user, "sk"), format!("user#{email}"));
        assert_eq!(string_attr(user, "gs1pk"), format!("user#{email}"));
        assert_eq!(string_attr(user, "gs1sk"), "user#");

        assert_eq!(string_attr(account, "pk"), "account#acc1");
        assert_eq!(string_attr(account, "sk"), "account#");
        assert_eq!(string_attr(account, "name"), "Acme");
    }

    #[test]
    fn test_data_model_leaves_a_key_its_template_does_not_reproduce() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        // sk does not follow the User template, so it must be left alone and
        // reported rather than silently rewritten.
        setup_export_dir(
            &source,
            "App",
            &[
                r#"{"Item": {"_type": {"S": "User"}, "pk": {"S": "account#acc1"}, "sk": {"S": "legacy-profile"}, "accountId": {"S": "acc1"}, "email": {"S": "alice@example.com"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[single_table_schema("App")]);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }
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
            data_model: Some(onetable_fixture()),
        })
        .unwrap();

        let mismatch = summary
            .warnings
            .iter()
            .find(|w| w.contains("entity 'User'") && w.contains("does not reproduce sk"))
            .unwrap_or_else(|| panic!("expected a mismatch warning: {:?}", summary.warnings));
        assert!(
            !mismatch.contains("legacy-profile"),
            "a warning must not quote the key value: {mismatch}"
        );

        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        let user = &scan_all(&db, "App")[0];
        assert_eq!(string_attr(user, "sk"), "legacy-profile");
        assert_ne!(string_attr(user, "email"), "alice@example.com");
    }

    #[test]
    fn test_rules_without_data_model_warn_that_keys_are_not_rewritten() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "App",
            &[
                r#"{"Item": {"pk": {"S": "CUSTOMER#alice@example.com"}, "sk": {"S": "PROFILE"}, "email": {"S": "alice@example.com"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[simple_table_schema("App")]);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }
"#,
        )
        .unwrap();

        let summary = import::run(ImportCommand {
            source,
            output: Some(output),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: None,
        })
        .unwrap();

        assert!(
            summary.warnings.iter().any(|w| w.contains("--data-model")),
            "expected the keys-not-rewritten notice: {:?}",
            summary.warnings
        );
    }

    #[test]
    fn test_data_model_reports_rows_collapsing_under_a_constant_action() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "App",
            &[
                r#"{"Item": {"_type": {"S": "User"}, "pk": {"S": "account#acc1"}, "sk": {"S": "user#alice@example.com"}, "accountId": {"S": "acc1"}, "email": {"S": "alice@example.com"}}}"#,
                r#"{"Item": {"_type": {"S": "User"}, "pk": {"S": "account#acc1"}, "sk": {"S": "user#bob@example.com"}, "accountId": {"S": "acc1"}, "email": {"S": "bob@example.com"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[single_table_schema("App")]);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
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
            data_model: Some(onetable_fixture()),
        })
        .unwrap();

        assert!(
            summary
                .warnings
                .iter()
                .any(|w| w.contains("overwrites the last")),
            "expected the up-front constant-action warning: {:?}",
            summary.warnings
        );
        assert!(
            summary
                .warnings
                .iter()
                .any(|w| w.contains("1 items rendered the same primary key")),
            "expected the collision count: {:?}",
            summary.warnings
        );

        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        let items = scan_all(&db, "App");
        assert_eq!(items.len(), 1);
        assert_eq!(string_attr(&items[0], "sk"), "user#[REDACTED]");
    }

    #[test]
    fn test_data_model_lets_a_rule_on_a_key_win() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "App",
            &[
                r#"{"Item": {"_type": {"S": "User"}, "pk": {"S": "account#acc1"}, "sk": {"S": "user#alice@example.com"}, "accountId": {"S": "acc1"}, "email": {"S": "alice@example.com"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[single_table_schema("App")]);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(sk)"
path = "sk"
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
            data_model: Some(onetable_fixture()),
        })
        .unwrap();
        assert!(
            summary
                .warnings
                .iter()
                .any(|w| w.contains("targets key attribute 'sk' directly")),
            "{:?}",
            summary.warnings
        );

        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        let user = &scan_all(&db, "App")[0];
        assert_eq!(string_attr(user, "sk"), "[REDACTED]");
    }

    /// A OneTable model where Customer and Order both key on `email`, which
    /// is what a single-table design does to keep them in one partition.
    fn shared_key_model(path: &std::path::Path) {
        std::fs::write(
            path,
            r#"{
                "format": "onetable:1.1.0",
                "indexes": { "primary": { "hash": "pk", "sort": "sk" } },
                "params": { "typeField": "_type" },
                "models": {
                    "Customer": {
                        "pk": { "type": "string", "value": "CUSTOMER#${email}" },
                        "sk": { "type": "string", "value": "PROFILE" },
                        "email": { "type": "string" }
                    },
                    "Order": {
                        "pk": { "type": "string", "value": "CUSTOMER#${email}" },
                        "sk": { "type": "string", "value": "ORDER#${orderId}" },
                        "email": { "type": "string" },
                        "orderId": { "type": "string" }
                    }
                }
            }"#,
        )
        .unwrap();
    }

    const CUSTOMER_ITEM: &str = r#"{"Item": {"_type": {"S": "Customer"}, "pk": {"S": "CUSTOMER#a@x.co"}, "sk": {"S": "PROFILE"}, "email": {"S": "a@x.co"}}}"#;
    const ORDER_ITEM: &str = r#"{"Item": {"_type": {"S": "Order"}, "pk": {"S": "CUSTOMER#a@x.co"}, "sk": {"S": "ORDER#1"}, "email": {"S": "a@x.co"}, "orderId": {"S": "1"}}}"#;

    const FAKE_EMAIL_RULE: &str = r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }
"#;

    fn shared_key_import(
        tmp: &std::path::Path,
        items: &[&str],
        rules_toml: &str,
    ) -> Result<import::ImportSummary, import::ImportError> {
        let source = tmp.join("export");
        let schema_file = tmp.join("schema.json");
        let rules_file = tmp.join("rules.toml");
        let model_file = tmp.join("model.json");

        setup_export_dir(&source, "App", items);
        create_schema_file(&schema_file, &[simple_table_schema("App")]);
        std::fs::write(&rules_file, rules_toml).unwrap();
        shared_key_model(&model_file);

        import::run(ImportCommand {
            source,
            output: Some(tmp.join("output.db")),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: Some(model_file),
        })
    }

    #[test]
    fn test_shared_key_attribute_without_consistency_fails_once_both_entities_appear() {
        let tmp = tempfile::tempdir().unwrap();
        let err = shared_key_import(tmp.path(), &[CUSTOMER_ITEM, ORDER_ITEM], FAKE_EMAIL_RULE)
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("entity 'Customer' and entity 'Order'"),
            "{msg}"
        );
        assert!(msg.contains("Add 'email' to [consistency] fields"), "{msg}");

        // Nothing is persisted on the error path.
        assert!(
            !tmp.path().join("output.db").exists(),
            "a failed import must not leave a half-anonymised database"
        );
    }

    #[test]
    fn test_one_entity_alone_imports_without_a_consistency_block() {
        let tmp = tempfile::tempdir().unwrap();
        let summary = shared_key_import(tmp.path(), &[CUSTOMER_ITEM], FAKE_EMAIL_RULE)
            .expect("a slice holding one entity has no join to lose");
        assert_eq!(summary.total_items, 1);
        assert!(
            summary.warnings.iter().any(|w| w.contains("will not join")),
            "the risk is still worth stating up front: {:?}",
            summary.warnings
        );
    }

    #[test]
    fn test_listing_the_shared_attribute_in_consistency_imports_both_entities() {
        let tmp = tempfile::tempdir().unwrap();
        let rules = format!(
            "{FAKE_EMAIL_RULE}\n[consistency]\nfields = [{}]\n",
            "\"email\""
        );
        let summary = shared_key_import(tmp.path(), &[CUSTOMER_ITEM, ORDER_ITEM], &rules).unwrap();
        assert_eq!(summary.total_items, 2);
        assert!(
            !summary.warnings.iter().any(|w| w.contains("will not join")),
            "{:?}",
            summary.warnings
        );

        // The order is still in its customer's partition.
        let db = dynoxide::Database::new(tmp.path().join("output.db").to_str().unwrap()).unwrap();
        let items = scan_all(&db, "App");
        let pks: std::collections::HashSet<String> =
            items.iter().map(|i| string_attr(i, "pk")).collect();
        assert_eq!(pks.len(), 1, "customer and order should share a partition");
        assert!(!pks.iter().next().unwrap().contains("a@x.co"));
    }

    #[test]
    fn test_a_key_rule_that_does_not_match_an_entity_does_not_block_its_rebuild() {
        // The sk rule matches only Account items. A User's sk must still be
        // rebuilt from its template, or the real address survives in the key
        // while the attribute beside it is anonymised.
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "App",
            &[
                r#"{"Item": {"_type": {"S": "User"}, "pk": {"S": "account#acc1"}, "sk": {"S": "user#alice@real.co.uk"}, "accountId": {"S": "acc1"}, "email": {"S": "alice@real.co.uk"}}}"#,
                r#"{"Item": {"_type": {"S": "Account"}, "pk": {"S": "account#acc1"}, "sk": {"S": "account#"}, "id": {"S": "acc1"}, "accountName": {"S": "Acme"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[single_table_schema("App")]);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(accountName)"
path = "sk"
action = { type = "redact" }

[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }
"#,
        )
        .unwrap();

        import::run(ImportCommand {
            source,
            output: Some(output.clone()),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: Some(onetable_fixture()),
        })
        .unwrap();

        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        let items = scan_all(&db, "App");
        let user = items
            .iter()
            .find(|i| string_attr(i, "_type") == "User")
            .unwrap();

        let email = string_attr(user, "email");
        assert_ne!(email, "alice@real.co.uk");
        assert_eq!(
            string_attr(user, "sk"),
            format!("user#{email}"),
            "sk must be rebuilt: the sk rule never matched this item"
        );

        // Belt and braces: the real address is nowhere in the output.
        for item in &items {
            for value in item.values() {
                if let dynoxide::AttributeValue::S(s) = value {
                    assert!(!s.contains("alice@real.co.uk"), "leaked in {s}");
                }
            }
        }
    }

    #[test]
    fn test_two_entities_with_no_shared_value_import_without_a_consistency_block() {
        // Both entities present and keyed on the same template, but no
        // address in common, so there was never a join to lose.
        let tmp = tempfile::tempdir().unwrap();
        let unrelated_order = r#"{"Item": {"_type": {"S": "Order"}, "pk": {"S": "CUSTOMER#b@y.co"}, "sk": {"S": "ORDER#1"}, "email": {"S": "b@y.co"}, "orderId": {"S": "1"}}}"#;
        let summary = shared_key_import(
            tmp.path(),
            &[CUSTOMER_ITEM, unrelated_order],
            FAKE_EMAIL_RULE,
        )
        .expect("no shared value means no broken join");
        assert_eq!(summary.total_items, 2);
        assert!(
            summary.warnings.iter().any(|w| w.contains("will not join")),
            "the risk is still worth stating up front: {:?}",
            summary.warnings
        );
    }

    #[test]
    fn test_an_item_without_a_type_attribute_resolves_to_its_own_entity() {
        // Customer and Order share a partition template and differ only by a
        // constant sk. With no discriminator, resolving the Order as a
        // Customer would leave its sort key holding the original value.
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");
        let model_file = tmp.path().join("model.json");

        setup_export_dir(
            &source,
            "App",
            &[
                r#"{"Item": {"pk": {"S": "CUSTOMER#a@real.co.uk"}, "sk": {"S": "PROFILE"}, "email": {"S": "a@real.co.uk"}}}"#,
                r#"{"Item": {"pk": {"S": "CUSTOMER#a@real.co.uk"}, "sk": {"S": "ORDER#ref-a@real.co.uk"}, "email": {"S": "a@real.co.uk"}, "orderId": {"S": "ref-a@real.co.uk"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[simple_table_schema("App")]);
        shared_key_model(&model_file);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }

[[rules]]
match = "attribute_exists(orderId)"
path = "orderId"
action = { type = "fake", generator = "word" }

[consistency]
fields = ["email"]
"#,
        )
        .unwrap();

        import::run(ImportCommand {
            source,
            output: Some(output.clone()),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: Some(model_file),
        })
        .unwrap();

        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        for item in scan_all(&db, "App") {
            for value in item.values() {
                if let dynoxide::AttributeValue::S(s) = value {
                    assert!(
                        !s.contains("a@real.co.uk"),
                        "the original address survived in {s}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_a_rebuilt_key_collision_across_batches_leaves_no_stale_index_row() {
        // Two items in separate export files collapse onto one primary key
        // once their redacted email is rebuilt into it, but keep distinct GSI
        // keys. The overwritten item's index row must go with it, or a GSI
        // query answers from a base row that no longer exists.
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");
        let model_file = tmp.path().join("model.json");

        let data_dir = source.join("App").join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        for (file, email, order) in [
            ("00000000.json", "a@x.co", "1"),
            ("00000001.json", "b@y.co", "2"),
        ] {
            std::fs::write(
                data_dir.join(file),
                format!(
                    r#"{{"Item": {{"_type": {{"S": "Order"}}, "pk": {{"S": "CUSTOMER#{email}"}}, "sk": {{"S": "ORDER"}}, "gs1pk": {{"S": "ORDER#{order}"}}, "gs1sk": {{"S": "ORDER"}}, "email": {{"S": "{email}"}}, "orderId": {{"S": "{order}"}}}}}}"#
                ) + "\n",
            )
            .unwrap();
        }

        create_schema_file(&schema_file, &[single_table_schema("App")]);
        std::fs::write(
            &model_file,
            r#"{
                "format": "onetable:1.1.0",
                "indexes": {
                    "primary": { "hash": "pk", "sort": "sk" },
                    "gs1": { "hash": "gs1pk", "sort": "gs1sk", "name": "GSI1" }
                },
                "params": { "typeField": "_type" },
                "models": {
                    "Order": {
                        "pk": { "type": "string", "value": "CUSTOMER#${email}" },
                        "sk": { "type": "string", "value": "ORDER" },
                        "gs1pk": { "type": "string", "value": "ORDER#${orderId}" },
                        "gs1sk": { "type": "string", "value": "ORDER" },
                        "email": { "type": "string" },
                        "orderId": { "type": "string" }
                    }
                }
            }"#,
        )
        .unwrap();
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
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
            data_model: Some(model_file),
        })
        .unwrap();
        assert!(
            summary
                .warnings
                .iter()
                .any(|w| w.contains("rendered the same primary key")),
            "the collapse should still be reported: {:?}",
            summary.warnings
        );

        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        let base = scan_all(&db, "App");
        assert_eq!(base.len(), 1, "the two collapsed onto one row");

        let indexed = db
            .scan(dynoxide::actions::scan::ScanRequest {
                table_name: "App".to_string(),
                index_name: Some("GSI1".to_string()),
                ..Default::default()
            })
            .unwrap()
            .items
            .unwrap();
        assert_eq!(
            indexed.len(),
            1,
            "the overwritten item's index row must go with it, got {indexed:?}"
        );
        assert_eq!(
            string_attr(&indexed[0], "gs1pk"),
            string_attr(&base[0], "gs1pk")
        );
    }

    #[test]
    fn test_mixed_rules_on_a_consistency_field_are_reported() {
        // A seeded fake derives its value and skips the consistency map, so
        // pairing it with an unseeded rule on the same field means one input
        // can leave with two values. Nothing in the output shows that.
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "Users",
            &[
                r#"{"Item": {"pk": {"S": "U#1"}, "sk": {"S": "P"}, "email": {"S": "a@x.co"}, "vip": {"BOOL": true}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[simple_table_schema("Users")]);
        // SAFETY: single-threaded test, no concurrent env reads
        unsafe { std::env::set_var("TEST_MIXED_SEED", "mixed-seed-0123456789") };
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(vip)"
path = "email"
action = { type = "fake", generator = "safe_email", seed_env = "TEST_MIXED_SEED" }

[[rules]]
match = "attribute_not_exists(vip)"
path = "email"
action = { type = "fake", generator = "safe_email" }

[consistency]
fields = ["email"]
"#,
        )
        .unwrap();

        let summary = import::run(ImportCommand {
            source,
            output: Some(tmp.path().join("out.db")),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: None,
        })
        .unwrap();

        assert!(
            summary
                .warnings
                .iter()
                .any(|w| w.contains("'email'") && w.contains("do not agree")),
            "expected the mixed-rule warning: {:?}",
            summary.warnings
        );
    }

    #[test]
    fn test_consistent_rules_on_a_consistency_field_are_quiet() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "Users",
            &[r#"{"Item": {"pk": {"S": "U#1"}, "sk": {"S": "P"}, "email": {"S": "a@x.co"}}}"#],
        );
        create_schema_file(&schema_file, &[simple_table_schema("Users")]);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }

[consistency]
fields = ["email"]
"#,
        )
        .unwrap();

        let summary = import::run(ImportCommand {
            source,
            output: Some(tmp.path().join("out.db")),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: None,
        })
        .unwrap();

        assert!(
            !summary.warnings.iter().any(|w| w.contains("do not agree")),
            "one rule shape should not warn: {:?}",
            summary.warnings
        );
    }

    /// A OneTable model where the order keys on the customer's address under
    /// a different attribute name, which is the ordinary single-table join.
    fn differently_named_source_model(path: &std::path::Path) {
        std::fs::write(
            path,
            r#"{
                "format": "onetable:1.1.0",
                "indexes": { "primary": { "hash": "pk", "sort": "sk" } },
                "params": { "typeField": "_type" },
                "models": {
                    "Customer": {
                        "pk": { "type": "string", "value": "CUSTOMER#${email}" },
                        "sk": { "type": "string", "value": "PROFILE" },
                        "email": { "type": "string" }
                    },
                    "Order": {
                        "pk": { "type": "string", "value": "CUSTOMER#${customerEmail}" },
                        "sk": { "type": "string", "value": "ORDER#${orderId}" },
                        "customerEmail": { "type": "string" },
                        "orderId": { "type": "string" }
                    }
                }
            }"#,
        )
        .unwrap();
    }

    #[test]
    fn test_a_sibling_keyed_on_an_untouched_attribute_still_breaks_the_join() {
        // Only the customer's address has a rule. The order keeps
        // `customerEmail` as it arrived, so its partition still holds the
        // real address while the customer has moved to a fake one: the join
        // is broken and personal data is left in a key.
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");
        let model_file = tmp.path().join("model.json");
        let order = r#"{"Item": {"_type": {"S": "Order"}, "pk": {"S": "CUSTOMER#a@x.co"}, "sk": {"S": "ORDER#1"}, "customerEmail": {"S": "a@x.co"}, "orderId": {"S": "1"}}}"#;

        setup_export_dir(&source, "App", &[CUSTOMER_ITEM, order]);
        create_schema_file(&schema_file, &[simple_table_schema("App")]);
        std::fs::write(&rules_file, FAKE_EMAIL_RULE).unwrap();
        differently_named_source_model(&model_file);

        let err = import::run(ImportCommand {
            source,
            output: Some(tmp.path().join("output.db")),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: Some(model_file),
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("entity 'Customer' and entity 'Order'"),
            "{msg}"
        );
        assert!(msg.contains("same attribute name"), "{msg}");
        assert!(
            !tmp.path().join("output.db").exists(),
            "a failed import must not leave a half-anonymised database"
        );
    }

    #[test]
    fn test_a_rebuilt_key_landing_on_an_unmatched_row_is_counted() {
        // The stranger matches no entity and keeps its keys. The user's sort
        // key is rebuilt onto exactly those keys, so the stranger's row is
        // overwritten, and that has to be counted like any other collision.
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "App",
            &[
                r#"{"Item": {"pk": {"S": "account#acc1"}, "sk": {"S": "user#[REDACTED]"}, "note": {"S": "no type, no address"}}}"#,
                r#"{"Item": {"_type": {"S": "User"}, "pk": {"S": "account#acc1"}, "sk": {"S": "user#alice@example.com"}, "accountId": {"S": "acc1"}, "email": {"S": "alice@example.com"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[single_table_schema("App")]);
        std::fs::write(
            &rules_file,
            r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
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
            data_model: Some(onetable_fixture()),
        })
        .unwrap();

        assert!(
            summary
                .warnings
                .iter()
                .any(|w| w.contains("1 items rendered the same primary key")),
            "the overwritten stranger must be counted: {:?}",
            summary.warnings
        );
        let db = dynoxide::Database::new(output.to_str().unwrap()).unwrap();
        assert_eq!(scan_all(&db, "App").len(), 1, "one row survived");
    }

    fn mixed_rule_import(tmp: &std::path::Path, rules_toml: &str) -> import::ImportSummary {
        let source = tmp.join("export");
        let schema_file = tmp.join("schema.json");
        let rules_file = tmp.join("rules.toml");
        setup_export_dir(
            &source,
            "Users",
            &[
                r#"{"Item": {"pk": {"S": "U#1"}, "sk": {"S": "P"}, "email": {"S": "a@x.co"}, "vip": {"BOOL": true}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[simple_table_schema("Users")]);
        std::fs::write(&rules_file, rules_toml).unwrap();
        import::run(ImportCommand {
            source,
            output: Some(tmp.join("out.db")),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: None,
        })
        .unwrap()
    }

    #[test]
    fn test_two_seeds_on_a_consistency_field_are_reported() {
        // Two seeded rules look alike, but the seed is part of the
        // derivation, so the same address leaves with a different value under
        // each. The message counts the seeds rather than printing them.
        // SAFETY: these names are used by this test alone
        unsafe {
            std::env::set_var("TEST_TWO_SEEDS_A", "seed-alpha-0123456789");
            std::env::set_var("TEST_TWO_SEEDS_B", "seed-bravo-0123456789");
        }
        let rules = |second: &str| {
            format!(
                r#"
[[rules]]
match = "attribute_exists(vip)"
path = "email"
action = {{ type = "fake", generator = "safe_email", seed_env = "TEST_TWO_SEEDS_A" }}

[[rules]]
match = "attribute_not_exists(vip)"
path = "email"
action = {{ type = "fake", generator = "safe_email", seed_env = "{second}" }}

[consistency]
fields = ["email"]
"#
            )
        };

        let tmp = tempfile::tempdir().unwrap();
        let summary = mixed_rule_import(tmp.path(), &rules("TEST_TWO_SEEDS_B"));
        let warning = summary
            .warnings
            .iter()
            .find(|w| w.contains("'email'") && w.contains("do not agree"))
            .unwrap_or_else(|| panic!("expected the mixed-rule warning: {:?}", summary.warnings));
        assert!(
            warning.contains("(seed 1)") && warning.contains("(seed 2)"),
            "{warning}"
        );
        assert!(
            !warning.contains("seed-alpha") && !warning.contains("seed-bravo"),
            "neither seed must be printed: {warning}"
        );

        // The same seed twice is one shape, and stays quiet.
        let tmp = tempfile::tempdir().unwrap();
        let summary = mixed_rule_import(tmp.path(), &rules("TEST_TWO_SEEDS_A"));
        assert!(
            !summary.warnings.iter().any(|w| w.contains("do not agree")),
            "one seed under two names is one derivation: {:?}",
            summary.warnings
        );
    }

    #[test]
    fn test_two_salts_on_a_consistency_field_are_reported() {
        // SAFETY: these names are used by this test alone
        unsafe {
            std::env::set_var("TEST_TWO_SALTS_A", "salt-alpha-0123456789");
            std::env::set_var("TEST_TWO_SALTS_B", "salt-bravo-0123456789");
        }
        let tmp = tempfile::tempdir().unwrap();
        let summary = mixed_rule_import(
            tmp.path(),
            r#"
[[rules]]
match = "attribute_exists(vip)"
path = "email"
action = { type = "hash", salt_env = "TEST_TWO_SALTS_A" }

[[rules]]
match = "attribute_not_exists(vip)"
path = "email"
action = { type = "hash", salt_env = "TEST_TWO_SALTS_B" }

[consistency]
fields = ["email"]
"#,
        );
        let warning = summary
            .warnings
            .iter()
            .find(|w| w.contains("'email'") && w.contains("do not agree"))
            .unwrap_or_else(|| panic!("expected the mixed-rule warning: {:?}", summary.warnings));
        assert!(
            warning.contains("(salt 1)") && warning.contains("(salt 2)"),
            "{warning}"
        );
        assert!(
            !warning.contains("salt-alpha") && !warning.contains("salt-bravo"),
            "neither salt must be printed: {warning}"
        );
    }

    /// Two users, a rules file, and an output nobody should trust.
    fn run_with_rule(rule_toml: &str) -> dynoxide::import::ImportSummary {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("export");
        let output = tmp.path().join("output.db");
        let schema_file = tmp.path().join("schema.json");
        let rules_file = tmp.path().join("rules.toml");

        setup_export_dir(
            &source,
            "Users",
            &[
                r#"{"Item": {"pk": {"S": "USER#1"}, "sk": {"S": "PROFILE"}, "email": {"S": "alice@real.co.uk"}}}"#,
                r#"{"Item": {"pk": {"S": "USER#2"}, "sk": {"S": "PROFILE"}, "email": {"S": "bob@real.co.uk"}}}"#,
            ],
        );
        create_schema_file(&schema_file, &[simple_table_schema("Users")]);
        std::fs::write(&rules_file, rule_toml).unwrap();

        import::run(ImportCommand {
            source,
            output: Some(output),
            schema: schema_file,
            rules: Some(rules_file),
            tables: None,
            compress: false,
            force: false,
            continue_on_error: false,
            data_model: None,
        })
        .unwrap()
    }

    #[test]
    fn a_misspelt_path_is_reported_rather_than_passing_for_a_clean_run() {
        // The whole failure this warning exists for: the run reports a full
        // item count and exits 0, while every address it was pointed at is
        // still in the output.
        let summary = run_with_rule(
            r#"
    [[rules]]
    match = "attribute_exists(pk)"
    path = "emial"
    action = { type = "redact" }
    "#,
        );

        assert_eq!(summary.total_items, 2, "the import still succeeded");
        assert!(
            summary
                .warnings
                .iter()
                .any(|w| w.contains("rule 1") && w.contains("rewrote none of them")),
            "a rule that rewrote nothing must say so: {:?}",
            summary.warnings
        );
    }

    #[test]
    fn a_match_expression_that_fits_nothing_is_reported() {
        let summary = run_with_rule(
            r#"
    [[rules]]
    match = "attribute_exists(no_such_attribute)"
    path = "email"
    action = { type = "redact" }
    "#,
        );

        assert!(
            summary
                .warnings
                .iter()
                .any(|w| w.contains("rule 1") && w.contains("matched no item")),
            "a rule that matched nothing must say so: {:?}",
            summary.warnings
        );
    }

    #[test]
    fn a_rule_that_did_its_job_says_nothing() {
        // The other direction. A warning that fires on every ordinary run is
        // a warning nobody reads.
        let summary = run_with_rule(
            r#"
    [[rules]]
    match = "attribute_exists(email)"
    path = "email"
    action = { type = "redact" }
    "#,
        );

        assert!(
            !summary.warnings.iter().any(|w| w.contains("rule 1")),
            "a rule that worked must stay quiet: {:?}",
            summary.warnings
        );
    }
}
