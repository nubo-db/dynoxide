//! Vector index control-plane tests: CreateTable acceptance and validation,
//! DescribeTable reflection, and DeleteTable cleanup.
//!
//! Error strings are pinned byte-for-byte to real DynamoDB behaviour captured
//! in eu-west-2 on 2026-08-11.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::delete_table::DeleteTableRequest;
use dynoxide::actions::describe_table::DescribeTableRequest;
use dynoxide::storage::Storage;
use serde_json::json;

fn make_db() -> Database {
    Database::memory().unwrap()
}

/// A minimal valid vector index definition, mirroring the shape the AWS SDK
/// sends on the wire (VectorAttribute is a structure, not a bare string).
fn vix_json(name: &str) -> serde_json::Value {
    json!({
        "IndexName": name,
        "VectorAttribute": {"AttributeName": "embedding"},
        "Dimensions": 3,
        "DistanceFunction": "COSINE",
        "Projection": {"ProjectionType": "ALL"}
    })
}

fn base_request(table: &str, vixs: serde_json::Value) -> serde_json::Value {
    json!({
        "TableName": table,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": vixs
    })
}

fn parse(req: serde_json::Value) -> CreateTableRequest {
    serde_json::from_value(req).unwrap()
}

fn describe(
    db: &Database,
    table: &str,
) -> dynoxide::actions::describe_table::DescribeTableResponse {
    db.describe_table(DescribeTableRequest {
        table_name: table.to_string(),
    })
    .unwrap()
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

#[test]
fn create_reports_creating_and_describe_reports_active_without_backfilling() {
    let db = make_db();
    let resp = db
        .create_table(parse(base_request("VecTable", json!([vix_json("vix")]))))
        .unwrap();

    let created = resp
        .table_description
        .vector_indexes
        .as_ref()
        .expect("CreateTable response should reflect the vector index");
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].index_status, "CREATING");
    assert!(created[0].backfilling.is_none());

    let desc = describe(&db, "VecTable");
    let vixs = desc
        .table
        .vector_indexes
        .as_ref()
        .expect("DescribeTable should reflect the vector index");
    assert_eq!(vixs.len(), 1);
    let vix = &vixs[0];
    assert_eq!(vix.index_name, "vix");
    assert_eq!(vix.index_status, "ACTIVE");
    assert_eq!(vix.dimensions, 3);
    assert_eq!(vix.distance_function, "COSINE");
    assert_eq!(vix.vector_attribute.attribute_name, "embedding");
    assert_eq!(
        vix.projection.projection_type,
        Some(dynoxide::types::ProjectionType::ALL)
    );
    assert!(vix.search_schema.is_none());
    assert_eq!(vix.item_count, Some(0));
    assert_eq!(vix.index_size_bytes, Some(0));
    assert!(vix.index_arn.contains("/index/vix"));

    // Backfilling is never present on the CreateTable path: it must be absent
    // from the serialised response, not serialised as null or false.
    let body = serde_json::to_string(&desc).unwrap();
    assert!(
        !body.contains("Backfilling"),
        "Backfilling should be absent from the serialised description, got: {body}"
    );
}

#[test]
fn search_schema_round_trips_through_describe() {
    let db = make_db();
    let req = json!({
        "TableName": "VecSchema",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "tenant", "AttributeType": "S"},
            {"AttributeName": "category", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{
            "IndexName": "vix",
            "VectorAttribute": {"AttributeName": "embedding"},
            "SearchSchema": [
                {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"},
                {"AttributeName": "category", "SearchSchemaElementType": "INLINE_FILTER"}
            ],
            "Dimensions": 3,
            "DistanceFunction": "EUCLIDEAN",
            "Projection": {"ProjectionType": "KEYS_ONLY"}
        }]
    });
    db.create_table(parse(req)).unwrap();

    let desc = describe(&db, "VecSchema");
    let vix = &desc.table.vector_indexes.as_ref().unwrap()[0];
    let schema = vix.search_schema.as_ref().expect("SearchSchema preserved");
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].attribute_name, "tenant");
    assert_eq!(schema[0].search_schema_element_type, "HASH");
    assert_eq!(schema[1].attribute_name, "category");
    assert_eq!(schema[1].search_schema_element_type, "INLINE_FILTER");
    assert_eq!(vix.distance_function, "EUCLIDEAN");
}

#[test]
fn dimensions_4096_boundary_accepted() {
    let db = make_db();
    let mut vix = vix_json("vix");
    vix["Dimensions"] = json!(4096);
    db.create_table(parse(base_request("VecMax", json!([vix]))))
        .unwrap();
    let desc = describe(&db, "VecMax");
    assert_eq!(
        desc.table.vector_indexes.as_ref().unwrap()[0].dimensions,
        4096
    );
}

// ---------------------------------------------------------------------------
// Captured error strings (eu-west-2, 2026-08-11)
// ---------------------------------------------------------------------------

#[test]
fn provisioned_mode_rejected_with_captured_string() {
    let db = make_db();
    let req = json!({
        "TableName": "VecProv",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        "VectorIndexes": [vix_json("vix")]
    });
    let err = db.create_table(parse(req)).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Vector indexes are only supported \
         for PAY_PER_REQUEST tables"
    );
}

#[test]
fn dimensions_zero_rejected_at_request_model_layer() {
    let mut vix = vix_json("vix");
    vix["Dimensions"] = json!(0);
    let result =
        serde_json::from_value::<CreateTableRequest>(base_request("VecZero", json!([vix])));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains(
            "1 validation error detected: Value '0' at 'vectorIndexes.1.member.dimensions' \
             failed to satisfy constraint: Member must have value greater than or equal to 1"
        ),
        "Expected the enveloped request-model rejection, got: {err}"
    );
}

#[test]
fn dimensions_4097_rejected_with_bare_captured_string() {
    let db = make_db();
    let mut vix = vix_json("vix");
    vix["Dimensions"] = json!(4097);
    let err = db
        .create_table(parse(base_request("VecBig", json!([vix]))))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Number of dimensions must be between \
         1 and 4096 inclusive."
    );
}

#[test]
fn search_schema_attribute_missing_from_attribute_definitions() {
    let db = make_db();
    let mut vix = vix_json("vix");
    vix["SearchSchema"] = json!([
        {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}
    ]);
    let err = db
        .create_table(parse(base_request("VecMissing", json!([vix]))))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: One element in SearchSchema is not \
         defined in attribute definitions"
    );
}

#[test]
fn sixth_vector_index_rejected_with_captured_string() {
    let db = make_db();
    let vixs: Vec<serde_json::Value> = (0..6).map(|i| vix_json(&format!("vix-{i}"))).collect();
    let err = db
        .create_table(parse(base_request("VecSix", json!(vixs))))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: VectorIndex count exceeds the \
         per-table limit of 5"
    );
}

#[test]
fn conflicting_dimensions_rejected_with_captured_string() {
    let db = make_db();
    let mut second = vix_json("vix2");
    second["Dimensions"] = json!(4);
    let err = db
        .create_table(parse(base_request(
            "VecConflict",
            json!([vix_json("vix"), second]),
        )))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Conflicting attribute definition for \
         'embedding'. All VectorIndexes on the same vector attribute must use the same \
         dimensions."
    );
}

#[test]
fn index_name_below_three_characters_rejected_at_request_model_layer() {
    let result = serde_json::from_value::<CreateTableRequest>(base_request(
        "VecShort",
        json!([vix_json("vx")]),
    ));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains(
            "Value 'vx' at 'vectorIndexes.1.member.indexName' failed to satisfy \
             constraint: Member must have length greater than or equal to 3"
        ),
        "Expected the request-model length constraint, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Coexistence with GSIs and LSIs, and physical shadow tables
// ---------------------------------------------------------------------------

fn full_house_request(table: &str) -> serde_json::Value {
    json!({
        "TableName": table,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
            {"AttributeName": "lsi_sk", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "LocalSecondaryIndexes": [{
            "IndexName": "lsi1",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsi_sk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "VectorIndexes": [vix_json("vix")]
    })
}

fn physical_table_exists(storage: &Storage, name: &str) -> bool {
    let count: i64 = storage
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [name],
            |r| r.get(0),
        )
        .unwrap();
    count == 1
}

#[tokio::test(flavor = "current_thread")]
async fn vector_index_coexists_with_gsi_and_lsi() {
    let storage = Storage::memory().unwrap();
    let req: CreateTableRequest = serde_json::from_value(full_house_request("Mixed")).unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();

    // All three index families have physical tables alongside the data table.
    assert!(physical_table_exists(&storage, "Mixed"));
    assert!(physical_table_exists(&storage, "Mixed::gsi::gsi1"));
    assert!(physical_table_exists(&storage, "Mixed::lsi::lsi1"));
    assert!(physical_table_exists(&storage, "Mixed::vector::vix"));

    // Metadata for all three coexists in the description.
    let desc = dynoxide::actions::describe_table::execute(
        &storage,
        DescribeTableRequest {
            table_name: "Mixed".to_string(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        desc.table.global_secondary_indexes.as_ref().unwrap().len(),
        1
    );
    assert_eq!(
        desc.table.local_secondary_indexes.as_ref().unwrap().len(),
        1
    );
    assert_eq!(desc.table.vector_indexes.as_ref().unwrap().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn delete_drops_vector_shadow_tables_and_recreate_succeeds() {
    let storage = Storage::memory().unwrap();
    let req: CreateTableRequest = serde_json::from_value(full_house_request("Cycle")).unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();
    assert!(physical_table_exists(&storage, "Cycle::vector::vix"));

    dynoxide::actions::delete_table::execute(
        &storage,
        DeleteTableRequest {
            table_name: "Cycle".to_string(),
        },
    )
    .await
    .unwrap();
    assert!(!physical_table_exists(&storage, "Cycle"));
    assert!(!physical_table_exists(&storage, "Cycle::gsi::gsi1"));
    assert!(!physical_table_exists(&storage, "Cycle::lsi::lsi1"));
    assert!(!physical_table_exists(&storage, "Cycle::vector::vix"));

    // Recreating with the same table and index names succeeds and the
    // metadata is fresh (the shadow tables were dropped with the base table).
    let req: CreateTableRequest = serde_json::from_value(full_house_request("Cycle")).unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();
    assert!(physical_table_exists(&storage, "Cycle::vector::vix"));
    let desc = dynoxide::actions::describe_table::execute(
        &storage,
        DescribeTableRequest {
            table_name: "Cycle".to_string(),
        },
    )
    .await
    .unwrap();
    let vix = &desc.table.vector_indexes.as_ref().unwrap()[0];
    assert_eq!(vix.index_status, "ACTIVE");
}

// ---------------------------------------------------------------------------
// Edge shapes
// ---------------------------------------------------------------------------

#[test]
fn empty_vector_indexes_list_is_treated_as_absent() {
    let db = make_db();
    db.create_table(parse(base_request("VecEmpty", json!([]))))
        .unwrap();
    let desc = describe(&db, "VecEmpty");
    assert!(desc.table.vector_indexes.is_none());
}

#[test]
fn table_without_vector_indexes_reports_none() {
    let db = make_db();
    let req = json!({
        "TableName": "Plain",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    });
    let resp = db.create_table(parse(req)).unwrap();
    assert!(resp.table_description.vector_indexes.is_none());
    let desc = describe(&db, "Plain");
    assert!(desc.table.vector_indexes.is_none());
    let body = serde_json::to_string(&desc).unwrap();
    assert!(!body.contains("VectorIndexes"));
}
