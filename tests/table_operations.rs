//! Phase 2 integration tests: Table operations (CreateTable, DeleteTable, DescribeTable, ListTables).

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::delete_table::DeleteTableRequest;
use dynoxide::actions::describe_table::DescribeTableRequest;
use dynoxide::actions::list_tables::ListTablesRequest;
use dynoxide::types::*;

fn make_db() -> Database {
    Database::memory().unwrap()
}

fn simple_create_request(name: &str) -> CreateTableRequest {
    CreateTableRequest {
        table_name: name.to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        ..Default::default()
    }
}

fn composite_key_create_request(name: &str) -> CreateTableRequest {
    CreateTableRequest {
        table_name: name.to_string(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::HASH,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::RANGE,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// CreateTable
// ---------------------------------------------------------------------------

#[test]
fn test_create_table_hash_key_only() {
    let db = make_db();
    let resp = db.create_table(simple_create_request("HashOnly")).unwrap();
    assert_eq!(resp.table_description.table_name, "HashOnly");
    assert_eq!(
        resp.table_description.table_arn,
        "arn:aws:dynamodb:dynoxide:000000000000:table/HashOnly"
    );
    assert_eq!(resp.table_description.table_status, "CREATING");
    assert_eq!(resp.table_description.key_schema.len(), 1);
    assert_eq!(resp.table_description.item_count, Some(0));
}

#[test]
fn test_create_table_hash_range_key() {
    let db = make_db();
    let resp = db
        .create_table(composite_key_create_request("Composite"))
        .unwrap();
    assert_eq!(resp.table_description.key_schema.len(), 2);
}

#[test]
fn test_create_table_with_gsi() {
    let db = make_db();
    let request = CreateTableRequest {
        table_name: "WithGSI".to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: "ByGsiPk".to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::HASH,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::ALL),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        billing_mode: None,
        provisioned_throughput: None,
        stream_specification: None,
        ..Default::default()
    };

    let resp = db.create_table(request).unwrap();
    let gsis = resp.table_description.global_secondary_indexes.unwrap();
    assert_eq!(gsis.len(), 1);
    assert_eq!(gsis[0].index_name, "ByGsiPk");
    assert_eq!(gsis[0].index_status, "CREATING");
    assert_eq!(
        gsis[0].index_arn,
        "arn:aws:dynamodb:dynoxide:000000000000:table/WithGSI/index/ByGsiPk"
    );
}

#[test]
fn test_create_table_already_exists() {
    let db = make_db();
    db.create_table(simple_create_request("Duplicate")).unwrap();

    let err = db
        .create_table(simple_create_request("Duplicate"))
        .unwrap_err();
    assert!(err.to_string().contains("Table already exists"));
}

#[test]
fn test_create_table_invalid_name_too_short() {
    let db = make_db();
    let mut req = simple_create_request("ab");
    req.table_name = "ab".to_string();

    let err = db.create_table(req).unwrap_err();
    assert!(err.to_string().contains("TableName must be at least 3"));
}

#[test]
fn test_create_table_invalid_name_bad_chars() {
    let db = make_db();
    let mut req = simple_create_request("bad name!");
    req.table_name = "bad name!".to_string();

    let err = db.create_table(req).unwrap_err();
    assert!(err.to_string().contains("tableName"));
}

#[test]
fn test_create_table_missing_key_in_definitions() {
    let db = make_db();
    let request = CreateTableRequest {
        table_name: "MissingDef".to_string(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::HASH,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::RANGE,
            },
        ],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        ..Default::default()
    };

    let err = db.create_table(request).unwrap_err();
    // DynamoDB returns the generic error for 2-key schemas with missing definitions
    assert!(
        err.to_string()
            .contains("Some index key attribute have no definition")
            || err
                .to_string()
                .contains("not defined in AttributeDefinitions"),
        "Expected key-not-defined error, got: {}",
        err
    );
}

// ---------------------------------------------------------------------------
// DeleteTable
// ---------------------------------------------------------------------------

#[test]
fn test_delete_existing_table() {
    let db = make_db();
    db.create_table(simple_create_request("ToDelete")).unwrap();

    let resp = db
        .delete_table(DeleteTableRequest {
            table_name: "ToDelete".to_string(),
        })
        .unwrap();
    assert_eq!(resp.table_description.table_status, "DELETING");
    assert_eq!(
        resp.table_description.table_arn,
        "arn:aws:dynamodb:dynoxide:000000000000:table/ToDelete"
    );

    // Table should be gone
    let err = db
        .describe_table(DescribeTableRequest {
            table_name: "ToDelete".to_string(),
        })
        .unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[test]
fn test_delete_nonexistent_table() {
    let db = make_db();
    let err = db
        .delete_table(DeleteTableRequest {
            table_name: "Ghost".to_string(),
        })
        .unwrap_err();
    assert!(err.to_string().contains("not found"));
}

// ---------------------------------------------------------------------------
// DescribeTable
// ---------------------------------------------------------------------------

#[test]
fn test_describe_existing_table() {
    let db = make_db();
    db.create_table(composite_key_create_request("Described"))
        .unwrap();

    let resp = db
        .describe_table(DescribeTableRequest {
            table_name: "Described".to_string(),
        })
        .unwrap();

    assert_eq!(resp.table.table_name, "Described");
    assert_eq!(
        resp.table.table_arn,
        "arn:aws:dynamodb:dynoxide:000000000000:table/Described"
    );
    assert_eq!(resp.table.table_status, "ACTIVE");
    assert_eq!(resp.table.key_schema.len(), 2);
    assert_eq!(resp.table.attribute_definitions.len(), 2);
    assert!(resp.table.creation_date_time.is_some());
    assert_eq!(resp.table.item_count, Some(0));
}

/// #55: TableId is assigned once and stays stable, rather than changing on
/// every call. CreateTable and repeated DescribeTable calls all return the
/// same TableId.
#[test]
fn test_table_id_is_stable_across_calls() {
    let db = make_db();
    let created = db.create_table(simple_create_request("StableId")).unwrap();
    let create_id = created.table_description.table_id.clone();
    assert!(create_id.is_some(), "CreateTable should return a TableId");

    let d1 = db
        .describe_table(DescribeTableRequest {
            table_name: "StableId".to_string(),
        })
        .unwrap();
    let d2 = db
        .describe_table(DescribeTableRequest {
            table_name: "StableId".to_string(),
        })
        .unwrap();

    assert_eq!(
        d1.table.table_id, create_id,
        "DescribeTable TableId must match the CreateTable TableId"
    );
    assert_eq!(
        d2.table.table_id, d1.table.table_id,
        "TableId must be stable across repeated DescribeTable calls"
    );
}

/// #55: a dropped-and-recreated table gets a new TableId, matching AWS, even
/// when the recreate happens immediately. The id is random per incarnation, not
/// derived from table state, so a same-second recreate still changes it.
#[test]
fn test_table_id_changes_after_recreate() {
    let db = make_db();

    db.create_table(simple_create_request("RecreateId"))
        .unwrap();
    let first = db
        .describe_table(DescribeTableRequest {
            table_name: "RecreateId".to_string(),
        })
        .unwrap()
        .table
        .table_id;

    db.delete_table(DeleteTableRequest {
        table_name: "RecreateId".to_string(),
    })
    .unwrap();

    db.create_table(simple_create_request("RecreateId"))
        .unwrap();
    let second = db
        .describe_table(DescribeTableRequest {
            table_name: "RecreateId".to_string(),
        })
        .unwrap()
        .table
        .table_id;

    assert!(first.is_some() && second.is_some());
    assert_ne!(
        first, second,
        "a recreated table must be assigned a new TableId"
    );
}

#[test]
fn test_describe_nonexistent_table() {
    let db = make_db();
    let err = db
        .describe_table(DescribeTableRequest {
            table_name: "Nope".to_string(),
        })
        .unwrap_err();
    assert!(err.to_string().contains("not found"));
}

// ---------------------------------------------------------------------------
// ListTables
// ---------------------------------------------------------------------------

#[test]
fn test_list_tables_empty() {
    let db = make_db();
    let resp = db
        .list_tables(ListTablesRequest {
            exclusive_start_table_name: None,
            limit: None,
        })
        .unwrap();
    assert!(resp.table_names.is_empty());
    assert!(resp.last_evaluated_table_name.is_none());
}

#[test]
fn test_list_tables_multiple() {
    let db = make_db();
    for name in &["Alpha", "Beta", "Gamma"] {
        db.create_table(simple_create_request(name)).unwrap();
    }

    let resp = db
        .list_tables(ListTablesRequest {
            exclusive_start_table_name: None,
            limit: None,
        })
        .unwrap();
    assert_eq!(resp.table_names, vec!["Alpha", "Beta", "Gamma"]);
    assert!(resp.last_evaluated_table_name.is_none());
}

#[test]
fn test_list_tables_pagination() {
    let db = make_db();
    for name in &["Alpha", "Beta", "Gamma", "Delta"] {
        db.create_table(simple_create_request(name)).unwrap();
    }

    // Page 1
    let resp = db
        .list_tables(ListTablesRequest {
            exclusive_start_table_name: None,
            limit: Some(2),
        })
        .unwrap();
    assert_eq!(resp.table_names, vec!["Alpha", "Beta"]);
    assert_eq!(resp.last_evaluated_table_name, Some("Beta".to_string()));

    // Page 2
    let resp = db
        .list_tables(ListTablesRequest {
            exclusive_start_table_name: Some("Beta".to_string()),
            limit: Some(2),
        })
        .unwrap();
    assert_eq!(resp.table_names, vec!["Delta", "Gamma"]);
    assert!(resp.last_evaluated_table_name.is_none());
}

// ---------------------------------------------------------------------------
// JSON round-trip of request types
// ---------------------------------------------------------------------------

#[test]
fn test_create_table_request_from_json() {
    let json = r#"{
        "TableName": "FromJson",
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}
        ]
    }"#;

    let request: CreateTableRequest = serde_json::from_str(json).unwrap();
    let db = make_db();
    let resp = db.create_table(request).unwrap();
    assert_eq!(resp.table_description.table_name, "FromJson");
}
