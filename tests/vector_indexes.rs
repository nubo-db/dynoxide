//! Vector index control-plane tests: CreateTable acceptance and validation,
//! DescribeTable reflection, DeleteTable cleanup, and the UpdateTable
//! `VectorIndexUpdates` path with its synchronous backfill.
//!
//! Error strings are pinned byte-for-byte to real DynamoDB behaviour captured
//! in eu-west-2 on 2026-08-11, with a follow-up capture on 2026-08-12 that
//! was byte-identical in eu-west-2 and us-east-1.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::delete_table::DeleteTableRequest;
use dynoxide::actions::describe_table::DescribeTableRequest;
use dynoxide::actions::update_table::{UpdateTableRequest, VectorIndexUpdate};
use dynoxide::actions::vector_lifecycle::VectorIndexLifecycle;
use dynoxide::storage::Storage;
use dynoxide::storage_backend::ManualClock;
use dynoxide::types::{
    AttributeDefinition, KeySchemaElement, KeyType, Projection, ProjectionType,
    ScalarAttributeType, SearchSchemaElement, VectorAttributeDefinition, VectorIndex,
};
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

/// Unwrap a request-model rejection raised inside serde deserialisation.
/// The raw serde error carries the internal `VALIDATION:` marker that the
/// server layer strips before anything reaches the wire; strip it here so
/// assertions pin the full client-visible message.
fn request_model_error<T: std::fmt::Debug>(
    result: std::result::Result<T, serde_json::Error>,
) -> String {
    let err = result.unwrap_err().to_string();
    err.strip_prefix("VALIDATION:")
        .expect("request-model rejections carry the VALIDATION: marker")
        .to_string()
}

/// A minimal valid typed vector index, for exercising the programmatic
/// (non-JSON) validation path.
fn typed_vix(name: &str) -> VectorIndex {
    VectorIndex {
        index_name: name.to_string(),
        vector_attribute: VectorAttributeDefinition {
            attribute_name: "embedding".to_string(),
        },
        search_schema: None,
        projection: Projection {
            projection_type: Some(ProjectionType::ALL),
            non_key_attributes: None,
        },
        dimensions: 3,
        distance_function: "COSINE".to_string(),
    }
}

/// A minimal valid typed CreateTable request carrying the given vector
/// indexes, for exercising the programmatic (non-JSON) validation path.
fn typed_request(table: &str, vixs: Vec<VectorIndex>) -> CreateTableRequest {
    CreateTableRequest {
        table_name: table.to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some("PAY_PER_REQUEST".to_string()),
        vector_indexes: Some(vixs),
        ..Default::default()
    }
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
// Captured error strings (eu-west-2, 2026-08-11; follow-up capture 2026-08-12,
// byte-identical in eu-west-2 and us-east-1)
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
    let err = request_model_error(serde_json::from_value::<CreateTableRequest>(base_request(
        "VecZero",
        json!([vix]),
    )));
    assert_eq!(
        err,
        "1 validation error detected: Value '0' at 'vectorIndexes.1.member.dimensions' \
         failed to satisfy constraint: Member must have value greater than or equal to 1"
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
    let err = request_model_error(serde_json::from_value::<CreateTableRequest>(base_request(
        "VecShort",
        json!([vix_json("vx")]),
    )));
    assert_eq!(
        err,
        "1 validation error detected: Value 'vx' at 'vectorIndexes.1.member.indexName' \
         failed to satisfy constraint: Member must have length greater than or equal to 3"
    );
}

#[test]
fn duplicate_vector_index_names_rejected_with_captured_string() {
    // Two vector indexes sharing a name use the classic cross-index string.
    // Captured from real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    let err = db
        .create_table(parse(base_request(
            "VecDup",
            json!([vix_json("vix"), vix_json("vix")]),
        )))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Duplicate index name: vix"
    );
}

#[test]
fn vector_index_name_colliding_with_gsi_name_rejected_with_captured_string() {
    // LSIs, GSIs, and vector indexes share one name namespace. Captured from
    // real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    let req = json!({
        "TableName": "VecGsiClash",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{
            "IndexName": "shared",
            "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "VectorIndexes": [vix_json("shared")]
    });
    let err = db.create_table(parse(req)).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Duplicate index name: shared"
    );
}

#[test]
fn empty_vector_indexes_list_rejected_with_captured_string() {
    // An empty VectorIndexes list is rejected, not normalised to absent.
    // Captured from real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    let err = db
        .create_table(parse(base_request("VecEmpty", json!([]))))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: List of VectorIndexes is empty"
    );
}

#[test]
fn dimensions_above_u32_range_rejected_with_captured_string() {
    // An integer above the u32 range still gets the standard over-range
    // message, never a raw serde error. Captured from real DynamoDB
    // (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    let mut vix = vix_json("vix");
    vix["Dimensions"] = json!(4_294_967_296_i64);
    let err = db
        .create_table(parse(base_request("VecHuge", json!([vix]))))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Number of dimensions must be between \
         1 and 4096 inclusive."
    );
}

#[test]
fn fractional_dimensions_accepted_and_truncated() {
    // Fractional Dimensions are accepted and truncated: 3.5 creates the
    // index and DescribeTable reports 3. Captured from real DynamoDB
    // (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    let mut vix = vix_json("vix");
    vix["Dimensions"] = json!(3.5);
    db.create_table(parse(base_request("VecFrac", json!([vix]))))
        .unwrap();
    let desc = describe(&db, "VecFrac");
    assert_eq!(desc.table.vector_indexes.as_ref().unwrap()[0].dimensions, 3);
}

#[test]
fn negative_fractional_dimensions_render_the_truncated_value() {
    // The lower-bound error reports the value after truncation toward zero,
    // so -2.7 renders as '-2'. The truncation model is captured (3.5 is
    // accepted as 3); the rejected-side rendering follows that model and
    // this test guards the truncate-before-render behaviour.
    let mut vix = vix_json("vix");
    vix["Dimensions"] = json!(-2.7);
    let err = request_model_error(serde_json::from_value::<CreateTableRequest>(base_request(
        "VecNegFrac",
        json!([vix]),
    )));
    assert_eq!(
        err,
        "1 validation error detected: Value '-2' at 'vectorIndexes.1.member.dimensions' \
         failed to satisfy constraint: Member must have value greater than or equal to 1"
    );
}

#[test]
fn second_search_schema_element_reports_its_own_position() {
    // A later SearchSchema element must report its real 1-based position.
    let db = make_db();
    let mut vix = typed_vix("vix");
    vix.search_schema = Some(vec![
        SearchSchemaElement {
            attribute_name: "tenant".to_string(),
            search_schema_element_type: "HASH".to_string(),
        },
        SearchSchemaElement {
            attribute_name: "kind".to_string(),
            search_schema_element_type: "RANGE".to_string(),
        },
    ]);
    let mut req = typed_request("VecElemSecond", vec![vix]);
    req.attribute_definitions.push(AttributeDefinition {
        attribute_name: "tenant".to_string(),
        attribute_type: ScalarAttributeType::S,
    });
    req.attribute_definitions.push(AttributeDefinition {
        attribute_name: "kind".to_string(),
        attribute_type: ScalarAttributeType::S,
    });
    let err = db.create_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "1 validation error detected: Value 'RANGE' at \
         'vectorIndexes.1.member.searchSchema.2.member.searchSchemaElementType' failed to \
         satisfy constraint: Member must satisfy enum value set: [HASH, INLINE_FILTER]"
    );
}

#[test]
fn search_schema_with_two_hash_elements_rejected_with_captured_string() {
    // At most one HASH element per SearchSchema; the reported value is the
    // HASH count. Captured from real DynamoDB (eu-west-2 and us-east-1,
    // 2026-08-12).
    let db = make_db();
    let req = json!({
        "TableName": "VecTwoHash",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "tenant", "AttributeType": "S"},
            {"AttributeName": "region", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{
            "IndexName": "vix",
            "VectorAttribute": {"AttributeName": "embedding"},
            "SearchSchema": [
                {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"},
                {"AttributeName": "region", "SearchSchemaElementType": "HASH"}
            ],
            "Dimensions": 3,
            "DistanceFunction": "COSINE",
            "Projection": {"ProjectionType": "ALL"}
        }]
    });
    let err = db.create_table(parse(req)).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Value '2' at 'SearchSchema' failed to \
         satisfy constraint: Member must have HASH count less than or equal to 1"
    );
}

#[test]
fn search_schema_duplicate_attribute_name_rejected_with_captured_string() {
    // The same attribute may not appear twice in one SearchSchema, even
    // under different element types. Captured from real DynamoDB (eu-west-2
    // and us-east-1, 2026-08-12).
    let db = make_db();
    let req = json!({
        "TableName": "VecDupAttr",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "tenant", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{
            "IndexName": "vix",
            "VectorAttribute": {"AttributeName": "embedding"},
            "SearchSchema": [
                {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"},
                {"AttributeName": "tenant", "SearchSchemaElementType": "INLINE_FILTER"}
            ],
            "Dimensions": 3,
            "DistanceFunction": "COSINE",
            "Projection": {"ProjectionType": "ALL"}
        }]
    });
    let err = db.create_table(parse(req)).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: SearchSchema contains a duplicate \
         AttributeName"
    );
}

#[test]
fn vector_attribute_declared_in_attribute_definitions_rejected_with_captured_string() {
    // The vector attribute must not itself appear in AttributeDefinitions.
    // Captured from real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    let req = json!({
        "TableName": "VecAttrDef",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "embedding", "AttributeType": "B"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [vix_json("vix")]
    });
    let err = db.create_table(parse(req)).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Conflicting attribute definition for \
         'embedding'. An attribute cannot be defined in AttributeDefinitions when used as a \
         VectorAttribute."
    );
}

// ---------------------------------------------------------------------------
// Layer coverage for enum rejections, list boundaries, and position reporting
// ---------------------------------------------------------------------------

#[test]
fn invalid_distance_function_rejected_at_request_model_layer() {
    let mut vix = vix_json("vix");
    vix["DistanceFunction"] = json!("MANHATTAN");
    let err = request_model_error(serde_json::from_value::<CreateTableRequest>(base_request(
        "VecDist",
        json!([vix]),
    )));
    assert_eq!(
        err,
        "1 validation error detected: Value 'MANHATTAN' at \
         'vectorIndexes.1.member.distanceFunction' failed to satisfy constraint: \
         Member must satisfy enum value set: [COSINE, DOT_PRODUCT, EUCLIDEAN]"
    );
}

#[test]
fn invalid_distance_function_rejected_at_operation_layer() {
    // The programmatic path never runs the request-model collectors, so the
    // typed validator raises the constraint at create_table time.
    let db = make_db();
    let mut vix = typed_vix("vix");
    vix.distance_function = "MANHATTAN".to_string();
    let err = db
        .create_table(typed_request("VecDistTyped", vec![vix]))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "1 validation error detected: Value 'MANHATTAN' at \
         'vectorIndexes.1.member.distanceFunction' failed to satisfy constraint: \
         Member must satisfy enum value set: [COSINE, DOT_PRODUCT, EUCLIDEAN]"
    );
}

#[test]
fn invalid_search_schema_element_type_rejected_at_request_model_layer() {
    let mut vix = vix_json("vix");
    vix["SearchSchema"] = json!([
        {"AttributeName": "tenant", "SearchSchemaElementType": "RANGE"}
    ]);
    let err = request_model_error(serde_json::from_value::<CreateTableRequest>(base_request(
        "VecElemType",
        json!([vix]),
    )));
    assert_eq!(
        err,
        "1 validation error detected: Value 'RANGE' at \
         'vectorIndexes.1.member.searchSchema.1.member.searchSchemaElementType' failed to \
         satisfy constraint: Member must satisfy enum value set: [HASH, INLINE_FILTER]"
    );
}

#[test]
fn invalid_search_schema_element_type_rejected_at_operation_layer() {
    let db = make_db();
    let mut vix = typed_vix("vix");
    vix.search_schema = Some(vec![SearchSchemaElement {
        attribute_name: "tenant".to_string(),
        search_schema_element_type: "RANGE".to_string(),
    }]);
    let mut req = typed_request("VecElemTyped", vec![vix]);
    req.attribute_definitions.push(AttributeDefinition {
        attribute_name: "tenant".to_string(),
        attribute_type: ScalarAttributeType::S,
    });
    let err = db.create_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "1 validation error detected: Value 'RANGE' at \
         'vectorIndexes.1.member.searchSchema.1.member.searchSchemaElementType' failed to \
         satisfy constraint: Member must satisfy enum value set: [HASH, INLINE_FILTER]"
    );
}

#[test]
fn five_vector_indexes_accepted_at_the_boundary() {
    let db = make_db();
    let vixs: Vec<serde_json::Value> = (0..5).map(|i| vix_json(&format!("vix-{i}"))).collect();
    db.create_table(parse(base_request("VecFive", json!(vixs))))
        .unwrap();
    let desc = describe(&db, "VecFive");
    assert_eq!(desc.table.vector_indexes.as_ref().unwrap().len(), 5);
}

#[test]
fn second_invalid_vector_index_reports_its_own_position() {
    // A later entry must report its real 1-based position, not position 1.
    // The programmatic path exercises the typed validator directly, since
    // the JSON path catches name constraints in the request-model collector.
    let db = make_db();
    let err = db
        .create_table(typed_request(
            "VecSecond",
            vec![typed_vix("vix"), typed_vix("vx")],
        ))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "1 validation error detected: Value 'vx' at 'vectorIndexes.2.member.indexName' \
         failed to satisfy constraint: Member must have length greater than or equal to 3"
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

/// One index's arm of a `ConsumedCapacity`, or `None` when the write left that
/// index alone and the arm is absent rather than present and zeroed.
fn index_arm(
    map: &Option<std::collections::HashMap<String, dynoxide::types::CapacityDetail>>,
    name: &str,
) -> Option<f64> {
    map.as_ref()
        .and_then(|m| m.get(name))
        .map(|d| d.capacity_units)
}

/// Per-index capacity reporting and vector shadow-table maintenance run back to
/// back inside one write transaction. The classic arms must read exactly as
/// they do on a table with no vector index (total 3, table 1, gsi 1, lsi 1,
/// from the #176 capture) while the vector arm reports beside them, so this is
/// the one place all three families are proven correct in a single response.
#[tokio::test(flavor = "current_thread")]
async fn classic_index_capacity_is_unchanged_beside_vector_maintenance() {
    let storage = Storage::memory().unwrap();
    let req: CreateTableRequest = serde_json::from_value(full_house_request("MixedCap")).unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();

    let put = serde_json::from_value(json!({
        "TableName": "MixedCap",
        "Item": {
            "pk": {"S": "a"}, "sk": {"S": "1"},
            "gsi_pk": {"S": "g"}, "lsi_sk": {"S": "l"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        },
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let cc = dynoxide::actions::put_item::execute(&storage, put)
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    assert_eq!(cc.capacity_units, 3.0, "total");
    assert_eq!(index_arm(&cc.global_secondary_indexes, "gsi1"), Some(1.0));
    assert_eq!(index_arm(&cc.local_secondary_indexes, "lsi1"), Some(1.0));
    // All three families report in one response, and the vector bytes stay off
    // the unit total asserted above.
    assert_eq!(vector_arm(&cc, "vix"), Some(1024.0));
    assert_eq!(shadow_row_count(&storage, "MixedCap::vector::vix"), 1);

    let delete = serde_json::from_value(json!({
        "TableName": "MixedCap",
        "Key": {"pk": {"S": "a"}, "sk": {"S": "1"}},
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let cc = dynoxide::actions::delete_item::execute(&storage, delete)
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    assert_eq!(cc.capacity_units, 3.0, "total");
    assert_eq!(index_arm(&cc.global_secondary_indexes, "gsi1"), Some(1.0));
    assert_eq!(index_arm(&cc.local_secondary_indexes, "lsi1"), Some(1.0));
    assert_eq!(vector_arm(&cc, "vix"), Some(1024.0));
    assert_eq!(shadow_row_count(&storage, "MixedCap::vector::vix"), 0);
}

/// The batch path parses each index family's definitions once per table and
/// hands the slices to the defs-accepting fan-out, so its arms are worth
/// pinning separately from the single-item paths.
#[tokio::test(flavor = "current_thread")]
async fn batch_write_reports_classic_index_capacity_beside_vector_maintenance() {
    let storage = Storage::memory().unwrap();
    let req: CreateTableRequest = serde_json::from_value(full_house_request("MixedBatch")).unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();

    let batch = serde_json::from_value(json!({
        "RequestItems": {
            "MixedBatch": [
                {"PutRequest": {"Item": {
                    "pk": {"S": "a"}, "sk": {"S": "1"},
                    "gsi_pk": {"S": "g"}, "lsi_sk": {"S": "l"},
                    "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
                }}},
                {"PutRequest": {"Item": {
                    "pk": {"S": "b"}, "sk": {"S": "1"},
                    "gsi_pk": {"S": "g"}, "lsi_sk": {"S": "l"},
                    "embedding": {"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]}
                }}}
            ]
        },
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let resp = dynoxide::actions::batch_write_item::execute(&storage, batch)
        .await
        .unwrap();
    let per_table = resp.consumed_capacity.expect("INDEXES reports capacity");
    let cc = per_table
        .iter()
        .find(|c| c.table_name == "MixedBatch")
        .expect("the written table reports an entry");
    // Two items, each charged one unit on each of the table, the GSI and the LSI.
    assert_eq!(cc.capacity_units, 6.0, "total");
    assert_eq!(index_arm(&cc.global_secondary_indexes, "gsi1"), Some(2.0));
    assert_eq!(index_arm(&cc.local_secondary_indexes, "lsi1"), Some(2.0));
    assert_eq!(shadow_row_count(&storage, "MixedBatch::vector::vix"), 2);
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
    let desc = describe_raw(&storage, "Mixed").await;
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
        &VectorIndexLifecycle::new(),
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
    let desc = describe_raw(&storage, "Cycle").await;
    let vix = &desc.table.vector_indexes.as_ref().unwrap()[0];
    assert_eq!(vix.index_status, "ACTIVE");
}

#[tokio::test(flavor = "current_thread")]
async fn create_succeeds_when_orphaned_vector_shadow_table_exists() {
    // An orphaned shadow table, as a partial failure or an older binary's
    // DeleteTable could leave behind, must neither wedge recreation nor leak
    // stale rows into the fresh index.
    let storage = Storage::memory().unwrap();
    storage
        .conn()
        .execute_batch(
            "CREATE TABLE \"Orphan::vector::vix\" (stale TEXT);
             INSERT INTO \"Orphan::vector::vix\" (stale) VALUES ('row');",
        )
        .unwrap();

    let req: CreateTableRequest =
        serde_json::from_value(base_request("Orphan", json!([vix_json("vix")]))).unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();

    // The shadow table was recreated with the current shape and no rows.
    let stale_rows: i64 = storage
        .conn()
        .query_row("SELECT COUNT(*) FROM \"Orphan::vector::vix\"", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(stale_rows, 0);
    let has_vector_json: i64 = storage
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('Orphan::vector::vix') \
             WHERE name = 'vector_json'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(has_vector_json, 1);
}

// ---------------------------------------------------------------------------
// UpdateTable VectorIndexUpdates: create with synchronous backfill, delete,
// and the captured error strings
// ---------------------------------------------------------------------------

async fn create_plain_ppr_table(storage: &Storage, table: &str) {
    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": table,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .unwrap();
    dynoxide::actions::create_table::execute(storage, req)
        .await
        .unwrap();
}

async fn put_raw_item(storage: &Storage, table: &str, item: serde_json::Value) {
    let req = serde_json::from_value(json!({
        "TableName": table,
        "Item": item,
    }))
    .unwrap();
    dynoxide::actions::put_item::execute(storage, req)
        .await
        .unwrap();
}

/// Drive one UpdateTable against a lifecycle nothing else can see, for a test
/// about what the call does rather than about the creation window it opens.
async fn update_table_raw(
    storage: &Storage,
    req: serde_json::Value,
) -> dynoxide::Result<dynoxide::actions::update_table::UpdateTableResponse> {
    update_table_raw_with(storage, &VectorIndexLifecycle::new(), req).await
}

/// As [`update_table_raw`], but against a caller-held lifecycle, so a later
/// describe, search, or delete sees the window this call opens.
async fn update_table_raw_with(
    storage: &Storage,
    lifecycle: &VectorIndexLifecycle,
    req: serde_json::Value,
) -> dynoxide::Result<dynoxide::actions::update_table::UpdateTableResponse> {
    let req: UpdateTableRequest = serde_json::from_value(req).unwrap();
    dynoxide::actions::update_table::execute(storage, req, lifecycle).await
}

/// Describe a table straight through the action, against a lifecycle nothing
/// has armed.
async fn describe_raw(
    storage: &Storage,
    table: &str,
) -> dynoxide::actions::describe_table::DescribeTableResponse {
    describe_raw_with(storage, &VectorIndexLifecycle::new(), table).await
}

/// As [`describe_raw`], but reading a caller-held lifecycle.
async fn describe_raw_with(
    storage: &Storage,
    lifecycle: &VectorIndexLifecycle,
    table: &str,
) -> dynoxide::actions::describe_table::DescribeTableResponse {
    dynoxide::actions::describe_table::execute(
        storage,
        DescribeTableRequest {
            table_name: table.to_string(),
        },
        lifecycle,
    )
    .await
    .unwrap()
}

fn shadow_row_count(storage: &Storage, shadow_table: &str) -> i64 {
    storage
        .conn()
        .query_row(
            &format!("SELECT COUNT(*) FROM \"{shadow_table}\""),
            [],
            |r| r.get(0),
        )
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn update_table_creates_vector_index_and_backfills_only_valid_items() {
    let clock = ManualClock::new(1_700_000_000);
    let storage = Storage::memory().unwrap().with_clock(clock.arc());
    let lifecycle = VectorIndexLifecycle::new();
    create_plain_ppr_table(&storage, "VecFill").await;

    // One valid vector, plus one of each invalid shape a live write would
    // reject: wrong dimension count, non-numeric element, wrong type,
    // out-of-f32-range element, and a missing attribute. Backfill over
    // pre-existing invalid values skips them silently (captured from real
    // DynamoDB, eu-west-2 and us-east-1, 2026-08-12).
    put_raw_item(
        &storage,
        "VecFill",
        json!({
            "pk": {"S": "valid"},
            "embedding": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]}
        }),
    )
    .await;
    put_raw_item(
        &storage,
        "VecFill",
        json!({
            "pk": {"S": "wrong-dims"},
            "embedding": {"L": [{"N": "1"}, {"N": "2"}]}
        }),
    )
    .await;
    put_raw_item(
        &storage,
        "VecFill",
        json!({
            "pk": {"S": "non-numeric"},
            "embedding": {"L": [{"N": "1"}, {"S": "x"}, {"N": "3"}]}
        }),
    )
    .await;
    put_raw_item(
        &storage,
        "VecFill",
        json!({
            "pk": {"S": "wrong-type"},
            "embedding": {"S": "not-a-list"}
        }),
    )
    .await;
    put_raw_item(
        &storage,
        "VecFill",
        json!({
            "pk": {"S": "out-of-range"},
            "embedding": {"L": [{"N": "1E+39"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    put_raw_item(&storage, "VecFill", json!({"pk": {"S": "no-vector"}})).await;

    let resp = update_table_raw_with(
        &storage,
        &lifecycle,
        json!({
            "TableName": "VecFill",
            "VectorIndexUpdates": [{"Create": vix_json("vix")}]
        }),
    )
    .await
    .unwrap();

    // The UpdateTable response reports the table UPDATING and the new index
    // CREATING (captured lifecycle walk, eu-west-2, 2026-08-11).
    assert_eq!(resp.table_description.table_status, "UPDATING");
    let vixs = resp.table_description.vector_indexes.as_ref().unwrap();
    assert_eq!(vixs.len(), 1);
    assert_eq!(vixs[0].index_status, "CREATING");

    // Only the valid item was backfilled, with its f32 copy stored.
    assert_eq!(shadow_row_count(&storage, "VecFill::vector::vix"), 1);
    let (table_pk, vector_json): (String, String) = storage
        .conn()
        .query_row(
            "SELECT table_pk, vector_json FROM \"VecFill::vector::vix\"",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(table_pk, "S:valid");
    assert_eq!(vector_json, "[1.0,2.0,3.0]");

    // The index the call just added is still creating, so DescribeTable
    // reports CREATING with Backfilling present.
    let desc = describe_raw_with(&storage, &lifecycle, "VecFill").await;
    let vix = &desc.table.vector_indexes.as_ref().unwrap()[0];
    assert_eq!(vix.index_status, "CREATING");
    assert_eq!(vix.backfilling, Some(true));

    // Past the window it reports ACTIVE, and Backfilling leaves the serialised
    // description rather than turning false.
    clock.tick(std::time::Duration::from_secs(60));
    let desc = describe_raw_with(&storage, &lifecycle, "VecFill").await;
    let vix = &desc.table.vector_indexes.as_ref().unwrap()[0];
    assert_eq!(vix.index_status, "ACTIVE");
    let body = serde_json::to_string(&desc).unwrap();
    assert!(
        !body.contains("Backfilling"),
        "Backfilling should be absent from the serialised description, got: {body}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn update_table_backfill_skips_hash_values_a_live_write_would_reject() {
    let storage = Storage::memory().unwrap();
    create_plain_ppr_table(&storage, "VecHash").await;

    put_raw_item(
        &storage,
        "VecHash",
        json!({
            "pk": {"S": "scoped"},
            "tenant": {"S": "acme"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    // Valid vector but no HASH attribute: unreachable through a HASH-schema
    // index, so it gets no row (the sparse-index pattern).
    put_raw_item(
        &storage,
        "VecHash",
        json!({
            "pk": {"S": "unscoped"},
            "embedding": {"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]}
        }),
    )
    .await;
    // A HASH value whose type differs from the declared AttributeDefinitions
    // type, and an empty-string HASH value, are both rejected by a live write
    // once the index exists (captured from real DynamoDB: the type mismatch
    // in eu-west-2 and us-east-1 on 2026-08-12, the empty string in eu-west-2
    // on 2026-08-11), so backfill skips both shapes.
    put_raw_item(
        &storage,
        "VecHash",
        json!({
            "pk": {"S": "type-clash"},
            "tenant": {"N": "7"},
            "embedding": {"L": [{"N": "0"}, {"N": "0"}, {"N": "1"}]}
        }),
    )
    .await;
    put_raw_item(
        &storage,
        "VecHash",
        json!({
            "pk": {"S": "empty-hash"},
            "tenant": {"S": ""},
            "embedding": {"L": [{"N": "1"}, {"N": "1"}, {"N": "0"}]}
        }),
    )
    .await;

    update_table_raw(
        &storage,
        json!({
            "TableName": "VecHash",
            "AttributeDefinitions": [{"AttributeName": "tenant", "AttributeType": "S"}],
            "VectorIndexUpdates": [{"Create": {
                "IndexName": "vix",
                "VectorAttribute": {"AttributeName": "embedding"},
                "SearchSchema": [
                    {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}
                ],
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }),
    )
    .await
    .unwrap();

    assert_eq!(shadow_row_count(&storage, "VecHash::vector::vix"), 1);
    let hash_value: String = storage
        .conn()
        .query_row("SELECT hash_value FROM \"VecHash::vector::vix\"", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(hash_value, "S:acme");
}

#[tokio::test(flavor = "current_thread")]
async fn update_table_backfill_keys_only_projection_stores_exact_item() {
    let storage = Storage::memory().unwrap();
    create_plain_ppr_table(&storage, "VecProjKeys").await;
    put_raw_item(
        &storage,
        "VecProjKeys",
        json!({
            "pk": {"S": "a"},
            "tenant": {"S": "t1"},
            "extra": {"S": "x"},
            "embedding": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]}
        }),
    )
    .await;

    update_table_raw(
        &storage,
        json!({
            "TableName": "VecProjKeys",
            "AttributeDefinitions": [{"AttributeName": "tenant", "AttributeType": "S"}],
            "VectorIndexUpdates": [{"Create": {
                "IndexName": "vix",
                "VectorAttribute": {"AttributeName": "embedding"},
                "SearchSchema": [
                    {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}
                ],
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }}]
        }),
    )
    .await
    .unwrap();

    // KEYS_ONLY projects the table keys plus the SearchSchema attributes and
    // nothing else: no vector copy, no other non-key attributes.
    let item_json: String = storage
        .conn()
        .query_row(
            "SELECT item_json FROM \"VecProjKeys::vector::vix\"",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let item: serde_json::Value = serde_json::from_str(&item_json).unwrap();
    assert_eq!(item, json!({"pk": {"S": "a"}, "tenant": {"S": "t1"}}));
}

#[tokio::test(flavor = "current_thread")]
async fn update_table_backfill_include_projection_stores_exact_item() {
    let storage = Storage::memory().unwrap();
    create_plain_ppr_table(&storage, "VecProjInc").await;
    put_raw_item(
        &storage,
        "VecProjInc",
        json!({
            "pk": {"S": "a"},
            "tenant": {"S": "t1"},
            "note": {"S": "kept"},
            "other": {"S": "dropped"},
            "embedding": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]}
        }),
    )
    .await;

    update_table_raw(
        &storage,
        json!({
            "TableName": "VecProjInc",
            "AttributeDefinitions": [{"AttributeName": "tenant", "AttributeType": "S"}],
            "VectorIndexUpdates": [{"Create": {
                "IndexName": "vix",
                "VectorAttribute": {"AttributeName": "embedding"},
                "SearchSchema": [
                    {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}
                ],
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "Projection": {
                    "ProjectionType": "INCLUDE",
                    "NonKeyAttributes": ["embedding", "note"]
                }
            }}]
        }),
    )
    .await
    .unwrap();

    // INCLUDE projects the table keys, the SearchSchema attributes, and the
    // named non-key attributes; the vector attribute appears as its f32 copy
    // and nothing else rides along.
    let item_json: String = storage
        .conn()
        .query_row(
            "SELECT item_json FROM \"VecProjInc::vector::vix\"",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let item: serde_json::Value = serde_json::from_str(&item_json).unwrap();
    assert_eq!(
        item,
        json!({
            "pk": {"S": "a"},
            "tenant": {"S": "t1"},
            "note": {"S": "kept"},
            "embedding": {"L": [{"N": "1.0"}, {"N": "2.0"}, {"N": "3.0"}]}
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn update_table_backfill_stores_inline_filter_values_exactly() {
    let storage = Storage::memory().unwrap();
    create_plain_ppr_table(&storage, "VecFilter").await;
    put_raw_item(
        &storage,
        "VecFilter",
        json!({
            "pk": {"S": "with"},
            "tenant": {"S": "t"},
            "category": {"S": "books"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    put_raw_item(
        &storage,
        "VecFilter",
        json!({
            "pk": {"S": "without"},
            "tenant": {"S": "t"},
            "embedding": {"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]}
        }),
    )
    .await;

    update_table_raw(
        &storage,
        json!({
            "TableName": "VecFilter",
            "AttributeDefinitions": [
                {"AttributeName": "tenant", "AttributeType": "S"},
                {"AttributeName": "category", "AttributeType": "S"}
            ],
            "VectorIndexUpdates": [{"Create": {
                "IndexName": "vix",
                "VectorAttribute": {"AttributeName": "embedding"},
                "SearchSchema": [
                    {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"},
                    {"AttributeName": "category", "SearchSchemaElementType": "INLINE_FILTER"}
                ],
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }),
    )
    .await
    .unwrap();

    // Both items are indexed: an absent INLINE_FILTER attribute does not make
    // the item sparse, and it stays absent from the JSON rather than becoming
    // null.
    assert_eq!(shadow_row_count(&storage, "VecFilter::vector::vix"), 2);
    let filter_for = |pk: &str| -> String {
        storage
            .conn()
            .query_row(
                "SELECT filter_json FROM \"VecFilter::vector::vix\" WHERE table_pk = ?1",
                [pk],
                |r| r.get(0),
            )
            .unwrap()
    };
    let with = filter_for("S:with");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&with).unwrap(),
        json!({"category": {"S": "books"}})
    );
    let without = filter_for("S:without");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&without).unwrap(),
        json!({})
    );
    assert!(
        !without.contains("null"),
        "an absent filter attribute must be absent, never null, got: {without}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn update_table_deletes_vector_index_leaving_base_table_and_gsis_untouched() {
    let storage = Storage::memory().unwrap();
    let req: CreateTableRequest = serde_json::from_value(full_house_request("VecDrop")).unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();
    put_raw_item(
        &storage,
        "VecDrop",
        json!({
            "pk": {"S": "a"},
            "sk": {"S": "1"},
            "gsi_pk": {"S": "g"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;

    update_table_raw(
        &storage,
        json!({
            "TableName": "VecDrop",
            "VectorIndexUpdates": [{"Delete": {"IndexName": "vix"}}]
        }),
    )
    .await
    .unwrap();

    // The shadow table is gone; the base table and the other index families
    // are untouched, physically and in metadata.
    assert!(!physical_table_exists(&storage, "VecDrop::vector::vix"));
    assert!(physical_table_exists(&storage, "VecDrop"));
    assert!(physical_table_exists(&storage, "VecDrop::gsi::gsi1"));
    assert!(physical_table_exists(&storage, "VecDrop::lsi::lsi1"));

    let desc = describe_raw(&storage, "VecDrop").await;
    assert!(desc.table.vector_indexes.is_none());
    assert_eq!(
        desc.table.global_secondary_indexes.as_ref().unwrap().len(),
        1
    );
    assert_eq!(
        desc.table.local_secondary_indexes.as_ref().unwrap().len(),
        1
    );

    let base_rows: i64 = storage
        .conn()
        .query_row("SELECT COUNT(*) FROM \"VecDrop\"", [], |r| r.get(0))
        .unwrap();
    assert_eq!(base_rows, 1);
}

#[test]
fn two_vector_index_actions_in_one_call_rejected_with_captured_string() {
    // The GSI online-index machinery's own error. Captured from real
    // DynamoDB (eu-west-2, 2026-08-11).
    let db = make_db();
    db.create_table(parse(base_request("VecTwoActs", json!([vix_json("one")]))))
        .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecTwoActs",
        "VectorIndexUpdates": [
            {"Delete": {"IndexName": "one"}},
            {"Create": vix_json("two")}
        ]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "Subscriber limit exceeded: Only 1 online index can be created or deleted \
         simultaneously per table"
    );
}

#[test]
fn vector_update_entry_with_both_actions_rejected_with_captured_string() {
    // One entry carrying Create and Delete breaks a per-object structural
    // rule with its own string, not the one-action-per-call
    // LimitExceededException. Captured from real DynamoDB (eu-west-2 and
    // us-east-1, 2026-08-12).
    let db = make_db();
    db.create_table(parse(base_request("VecBothActs", json!([vix_json("one")]))))
        .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecBothActs",
        "VectorIndexUpdates": [{
            "Create": vix_json("two"),
            "Delete": {"IndexName": "one"}
        }]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Only one vector index action is \
         allowed per VectorIndexUpdate object"
    );
}

#[test]
fn vector_create_named_after_existing_gsi_or_lsi_rejected_with_captured_string() {
    // The duplicate check spans index families: a vector create colliding
    // with a live GSI name carries the vector path's wording, captured from
    // real DynamoDB (eu-west-2 and us-east-1, 2026-08-12). The LSI direction
    // follows the shared name namespace captured on the CreateTable path.
    let db = make_db();
    let req: CreateTableRequest = serde_json::from_value(full_house_request("VecXFam")).unwrap();
    db.create_table(req).unwrap();
    for clash in ["gsi1", "lsi1"] {
        let req: UpdateTableRequest = serde_json::from_value(json!({
            "TableName": "VecXFam",
            "VectorIndexUpdates": [{"Create": vix_json(clash)}]
        }))
        .unwrap();
        let err = db.update_table(req).unwrap_err().to_string();
        assert_eq!(err, "Attempting to create an index which already exists");
    }
}

#[test]
fn gsi_create_named_after_existing_vector_index_rejected_with_captured_string() {
    // A GSI create colliding with a live vector index name carries the
    // vector path's wording, not the GSI same-family string. Captured from
    // real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    let req: CreateTableRequest = serde_json::from_value(full_house_request("GsiXFam")).unwrap();
    db.create_table(req).unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "GsiXFam",
        "AttributeDefinitions": [{"AttributeName": "cat", "AttributeType": "S"}],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "vix",
            "KeySchema": [{"AttributeName": "cat", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(err, "Attempting to create an index which already exists");
}

#[test]
fn gsi_create_keyed_on_live_vector_attribute_rejected_with_captured_string() {
    // A GSI keyed on a live vector attribute is a redefinition: the message
    // interpolates the attribute, the live index's dimensions, the declared
    // scalar type, and the proposed key type. Captured from real DynamoDB
    // (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    db.create_table(parse(base_request("GsiVecKey", json!([vix_json("vix")]))))
        .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "GsiVecKey",
        "AttributeDefinitions": [{"AttributeName": "embedding", "AttributeType": "S"}],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "gsi2",
            "KeySchema": [{"AttributeName": "embedding", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Attributes cannot be redefined. \
         Please check that your attribute has the same type as previously defined. \
         Existing schema: VectorIndexSchema:[VectorAttribute: key{embedding:L:3}] \
         New schema: Schema:[SchemaElement: key{embedding:S:HASH}]"
    );
}

#[test]
fn update_table_missing_dimensions_rejected_at_request_model_layer() {
    // The UpdateTable path renders 'vectorIndexUpdates.N.member.create'
    // field paths, enveloped like CreateTable's collectors. Captured from
    // real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let mut create = vix_json("vix");
    create.as_object_mut().unwrap().remove("Dimensions");
    let err = request_model_error(serde_json::from_value::<UpdateTableRequest>(json!({
        "TableName": "VecNoDims",
        "VectorIndexUpdates": [{"Create": create}]
    })));
    assert_eq!(
        err,
        "1 validation error detected: Value null at \
         'vectorIndexUpdates.1.member.create.dimensions' failed to satisfy constraint: \
         Member must not be null"
    );
}

#[test]
fn update_table_request_model_envelope_reports_gsi_entries_before_vector_entries() {
    // One envelope across index families, GSI entries before vector entries.
    // Captured from real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let mut create = vix_json("vix");
    create.as_object_mut().unwrap().remove("Dimensions");
    let err = request_model_error(serde_json::from_value::<UpdateTableRequest>(json!({
        "TableName": "VecEnvelope",
        "GlobalSecondaryIndexUpdates": [{"Update": {"IndexName": "gsi1"}}],
        "VectorIndexUpdates": [{"Create": create}]
    })));
    assert_eq!(
        err,
        "2 validation errors detected: Value null at \
         'globalSecondaryIndexUpdates.1.member.update.provisionedThroughput' failed to \
         satisfy constraint: Member must not be null; Value null at \
         'vectorIndexUpdates.1.member.create.dimensions' failed to satisfy constraint: \
         Member must not be null"
    );
}

#[test]
fn update_table_operation_layer_paths_carry_the_create_segment() {
    // The typed validator renders the UpdateTable path shape too, always as
    // entry 1 since a call carries at most one create action. A position-2
    // pin is impossible on this operation: two entries are rejected by the
    // one-action-per-call limit before any per-entry validation runs.
    let db = make_db();
    db.create_table(parse(base_request(
        "VecPathTyped",
        json!([vix_json("vix")]),
    )))
    .unwrap();
    let req = UpdateTableRequest {
        table_name: "VecPathTyped".to_string(),
        vector_index_updates: Some(vec![VectorIndexUpdate {
            create: Some(typed_vix("vx")),
            delete: None,
        }]),
        ..Default::default()
    };
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "1 validation error detected: Value 'vx' at \
         'vectorIndexUpdates.1.member.create.indexName' failed to satisfy constraint: \
         Member must have length greater than or equal to 3"
    );
}

#[test]
fn vector_create_with_duplicate_name_rejected_with_captured_string() {
    // The vector path has its own duplicate wording, with no index name in
    // it: not the GSI string and not CreateTable's classic cross-index one.
    // Captured from real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    db.create_table(parse(base_request("VecDupUpd", json!([vix_json("vix")]))))
        .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecDupUpd",
        "VectorIndexUpdates": [{"Create": vix_json("vix")}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(err, "Attempting to create an index which already exists");
}

#[test]
fn vector_delete_of_missing_index_rejected_with_captured_string() {
    // Bare index name, no quoting. Captured from real DynamoDB (eu-west-2
    // and us-east-1, 2026-08-12).
    let db = make_db();
    db.create_table(parse(json!({
        "TableName": "VecDelMiss",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    })))
    .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecDelMiss",
        "VectorIndexUpdates": [{"Delete": {"IndexName": "absent"}}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "Requested resource not found: Index absent for table VecDelMiss"
    );
}

#[test]
fn vector_create_on_provisioned_table_rejected_with_captured_string() {
    // Captured from real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    db.create_table(parse(json!({
        "TableName": "VecProvUpd",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    })))
    .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecProvUpd",
        "VectorIndexUpdates": [{"Create": vix_json("vix")}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Vector indexes are only supported \
         for PAY_PER_REQUEST tables"
    );
}

#[test]
fn billing_switch_to_provisioned_rejected_while_vector_indexes_exist() {
    // The flip gate has its own string, distinct from the create-time
    // gate's. Captured from real DynamoDB (eu-west-2 and us-east-1,
    // 2026-08-12).
    let db = make_db();
    db.create_table(parse(base_request("VecFlip", json!([vix_json("vix")]))))
        .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecFlip",
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Tables with vector indexes must \
         be in PAY_PER_REQUEST mode"
    );
}

#[test]
fn billing_switch_combined_with_delete_of_last_vector_index_rejected() {
    // The gate reads the stored definitions, so deleting the last vector
    // index and flipping in the same call is still rejected. Captured from
    // real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
    let db = make_db();
    db.create_table(parse(base_request("VecFlipDel", json!([vix_json("vix")]))))
        .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecFlipDel",
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        "VectorIndexUpdates": [{"Delete": {"IndexName": "vix"}}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Tables with vector indexes must \
         be in PAY_PER_REQUEST mode"
    );
}

#[test]
fn fifth_vector_index_via_update_table_accepted_at_the_boundary() {
    let db = make_db();
    let vixs: Vec<serde_json::Value> = (0..4).map(|i| vix_json(&format!("vix-{i}"))).collect();
    db.create_table(parse(base_request("VecFour", json!(vixs))))
        .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecFour",
        "VectorIndexUpdates": [{"Create": vix_json("vix-4")}]
    }))
    .unwrap();
    db.update_table(req).unwrap();
    let desc = describe(&db, "VecFour");
    let vixs = desc.table.vector_indexes.as_ref().unwrap();
    assert_eq!(vixs.len(), 5);

    // One description, two lifecycles: the four created with the table are
    // ACTIVE with no Backfilling, the fifth is inside the window UpdateTable
    // opened.
    for vix in vixs.iter().filter(|v| v.index_name != "vix-4") {
        assert_eq!(vix.index_status, "ACTIVE", "{}", vix.index_name);
        assert_eq!(vix.backfilling, None, "{}", vix.index_name);
    }
    let added = vixs.iter().find(|v| v.index_name == "vix-4").unwrap();
    assert_eq!(added.index_status, "CREATING");
    assert_eq!(added.backfilling, Some(true));
}

#[test]
fn sixth_vector_index_via_update_table_rejected_with_captured_string() {
    // Same count-limit string as CreateTable's. Captured from real DynamoDB
    // (eu-west-2, 2026-08-11).
    let db = make_db();
    let vixs: Vec<serde_json::Value> = (0..5).map(|i| vix_json(&format!("vix-{i}"))).collect();
    db.create_table(parse(base_request("VecFiveUpd", json!(vixs))))
        .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecFiveUpd",
        "VectorIndexUpdates": [{"Create": vix_json("vix-5")}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: VectorIndex count exceeds the \
         per-table limit of 5"
    );
}

#[test]
fn update_table_create_with_conflicting_dimensions_rejected_with_captured_string() {
    // The string is captured on the CreateTable path (eu-west-2, 2026-08-11);
    // the invariant is structural, so this call site pins the same bytes
    // against a live index.
    let db = make_db();
    db.create_table(parse(base_request("VecDimUpd", json!([vix_json("vix")]))))
        .unwrap();
    let mut second = vix_json("vix2");
    second["Dimensions"] = json!(4);
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecDimUpd",
        "VectorIndexUpdates": [{"Create": second}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Conflicting attribute definition for \
         'embedding'. All VectorIndexes on the same vector attribute must use the same \
         dimensions."
    );
}

#[test]
fn update_table_create_with_vector_attribute_declared_rejected_with_captured_string() {
    // The string is captured on the CreateTable path (eu-west-2 and
    // us-east-1, 2026-08-12); this call site checks the merged definitions,
    // so a declaration arriving in the update's delta trips it too.
    let db = make_db();
    db.create_table(parse(json!({
        "TableName": "VecAttrUpd",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    })))
    .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecAttrUpd",
        "AttributeDefinitions": [{"AttributeName": "embedding", "AttributeType": "B"}],
        "VectorIndexUpdates": [{"Create": vix_json("vix")}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Conflicting attribute definition for \
         'embedding'. An attribute cannot be defined in AttributeDefinitions when used as a \
         VectorAttribute."
    );
}

#[test]
fn empty_vector_index_update_entry_rejected() {
    // An entry carrying neither action mirrors the GSI structural message
    // with the two actions the vector family has; this shape is not captured.
    let db = make_db();
    db.create_table(parse(base_request("VecEmptyUpd", json!([vix_json("vix")]))))
        .unwrap();
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecEmptyUpd",
        "VectorIndexUpdates": [{}]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err().to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: One of VectorIndexUpdate.Create, \
         VectorIndexUpdate.Delete must not be null"
    );
}

#[test]
fn gsi_delete_preserves_attribute_used_by_vector_search_schema() {
    // Regression guard for AttributeDefinitions reconciliation: deleting a
    // GSI whose key attribute a surviving vector index's SearchSchema also
    // uses must not prune that attribute.
    let db = make_db();
    db.create_table(parse(json!({
        "TableName": "VecShared",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "tenant", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "tenant", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "VectorIndexes": [{
            "IndexName": "vix",
            "VectorAttribute": {"AttributeName": "embedding"},
            "SearchSchema": [
                {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}
            ],
            "Dimensions": 3,
            "DistanceFunction": "COSINE",
            "Projection": {"ProjectionType": "ALL"}
        }]
    })))
    .unwrap();

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecShared",
        "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "gsi1"}}]
    }))
    .unwrap();
    db.update_table(req).unwrap();

    let desc = describe(&db, "VecShared");
    assert!(desc.table.global_secondary_indexes.is_none());
    assert_eq!(desc.table.vector_indexes.as_ref().unwrap().len(), 1);
    assert!(
        desc.table
            .attribute_definitions
            .iter()
            .any(|d| d.attribute_name == "tenant"),
        "the surviving vector index's SearchSchema attribute must survive \
         reconciliation, got: {:?}",
        desc.table.attribute_definitions
    );
}

// ---------------------------------------------------------------------------
// Edge shapes
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Write-path index maintenance: live writes keep the shadow tables correct
// (f32 copies, silent de-indexing, captured write validation)
// ---------------------------------------------------------------------------

/// A table named `table` with one 3-dim COSINE index named `vix` over
/// `embedding`, no SearchSchema.
async fn create_vector_table(storage: &Storage, table: &str) {
    let req: CreateTableRequest =
        serde_json::from_value(base_request(table, json!([vix_json("vix")]))).unwrap();
    dynoxide::actions::create_table::execute(storage, req)
        .await
        .unwrap();
}

/// As [`create_vector_table`], but with a SearchSchema declaring `tenant` as
/// the HASH attribute (declared `S` in AttributeDefinitions).
async fn create_hash_schema_vector_table(storage: &Storage, table: &str) {
    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": table,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "tenant", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{
            "IndexName": "vix",
            "VectorAttribute": {"AttributeName": "embedding"},
            "SearchSchema": [
                {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}
            ],
            "Dimensions": 3,
            "DistanceFunction": "COSINE",
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .unwrap();
    dynoxide::actions::create_table::execute(storage, req)
        .await
        .unwrap();
}

/// Attempt a PutItem, returning the result instead of unwrapping.
async fn try_put_raw_item(
    storage: &Storage,
    table: &str,
    item: serde_json::Value,
) -> dynoxide::Result<()> {
    let req = serde_json::from_value(json!({
        "TableName": table,
        "Item": item,
    }))
    .unwrap();
    dynoxide::actions::put_item::execute(storage, req)
        .await
        .map(|_| ())
}

/// The single shadow row's (hash_value, vector_json, item_json).
fn shadow_row(storage: &Storage, shadow_table: &str) -> (String, String, String) {
    storage
        .conn()
        .query_row(
            &format!("SELECT hash_value, vector_json, item_json FROM \"{shadow_table}\""),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn put_item_stores_f32_copy_and_base_keeps_full_precision() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecLive").await;

    // 16777217 is the first integer f32 cannot represent: the index copy
    // truncates to 16777216 while the base table keeps what was written
    // (captured from real DynamoDB, eu-west-2, 2026-08-11). "1" reads back
    // as "1.0" through the index copy.
    put_raw_item(
        &storage,
        "VecLive",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "16777217"}, {"N": "1"}, {"N": "0"}]}
        }),
    )
    .await;

    assert_eq!(shadow_row_count(&storage, "VecLive::vector::vix"), 1);
    let (hash_value, vector_json, item_json) = shadow_row(&storage, "VecLive::vector::vix");
    assert_eq!(hash_value, "");
    assert_eq!(vector_json, "[16777216.0,1.0,0.0]");
    let item: serde_json::Value = serde_json::from_str(&item_json).unwrap();
    assert_eq!(
        item["embedding"],
        json!({"L": [{"N": "16777216.0"}, {"N": "1.0"}, {"N": "0.0"}]})
    );

    let base_json: String = storage
        .conn()
        .query_row("SELECT item_json FROM \"VecLive\"", [], |r| r.get(0))
        .unwrap();
    let base: serde_json::Value = serde_json::from_str(&base_json).unwrap();
    assert_eq!(
        base["embedding"],
        json!({"L": [{"N": "16777217"}, {"N": "1"}, {"N": "0"}]})
    );
}

#[tokio::test(flavor = "current_thread")]
async fn put_item_overwrite_replaces_the_shadow_row() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecOver").await;

    put_raw_item(
        &storage,
        "VecOver",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    put_raw_item(
        &storage,
        "VecOver",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]}
        }),
    )
    .await;

    assert_eq!(shadow_row_count(&storage, "VecOver::vector::vix"), 1);
    let (_, vector_json, _) = shadow_row(&storage, "VecOver::vector::vix");
    assert_eq!(vector_json, "[0.0,1.0,0.0]");
}

#[tokio::test(flavor = "current_thread")]
async fn delete_item_removes_the_shadow_row() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecDel").await;

    put_raw_item(
        &storage,
        "VecDel",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    assert_eq!(shadow_row_count(&storage, "VecDel::vector::vix"), 1);

    let req = serde_json::from_value(json!({
        "TableName": "VecDel",
        "Key": {"pk": {"S": "a"}}
    }))
    .unwrap();
    dynoxide::actions::delete_item::execute(&storage, req)
        .await
        .unwrap();

    assert_eq!(shadow_row_count(&storage, "VecDel::vector::vix"), 0);
    assert_eq!(shadow_row_count(&storage, "VecDel"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn update_item_remove_deindexes_and_restore_reindexes() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecUpd").await;

    put_raw_item(
        &storage,
        "VecUpd",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    assert_eq!(shadow_row_count(&storage, "VecUpd::vector::vix"), 1);

    // REMOVE de-indexes the item; the base item survives without the
    // attribute (captured from real DynamoDB, eu-west-2, 2026-08-11).
    let remove = serde_json::from_value(json!({
        "TableName": "VecUpd",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "REMOVE embedding"
    }))
    .unwrap();
    dynoxide::actions::update_item::execute(&storage, remove)
        .await
        .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecUpd::vector::vix"), 0);
    assert_eq!(shadow_row_count(&storage, "VecUpd"), 1);

    // Restoring the attribute re-indexes the item.
    let restore = serde_json::from_value(json!({
        "TableName": "VecUpd",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET embedding = :v",
        "ExpressionAttributeValues": {
            ":v": {"L": [{"N": "0"}, {"N": "0"}, {"N": "1"}]}
        }
    }))
    .unwrap();
    dynoxide::actions::update_item::execute(&storage, restore)
        .await
        .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecUpd::vector::vix"), 1);
    let (_, vector_json, _) = shadow_row(&storage, "VecUpd::vector::vix");
    assert_eq!(vector_json, "[0.0,0.0,1.0]");
}

#[tokio::test(flavor = "current_thread")]
async fn items_missing_hash_or_vector_attribute_write_without_rows() {
    let storage = Storage::memory().unwrap();
    create_hash_schema_vector_table(&storage, "VecSparse").await;

    // Valid vector but no HASH attribute: writes fine, no row (captured from
    // real DynamoDB, eu-west-2, 2026-08-11: accepted, unreachable through the
    // HASH-schema index).
    put_raw_item(
        &storage,
        "VecSparse",
        json!({
            "pk": {"S": "no-hash"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    // HASH present but no vector attribute: writes fine, no row.
    put_raw_item(
        &storage,
        "VecSparse",
        json!({
            "pk": {"S": "no-vector"},
            "tenant": {"S": "acme"}
        }),
    )
    .await;
    assert_eq!(shadow_row_count(&storage, "VecSparse::vector::vix"), 0);
    assert_eq!(shadow_row_count(&storage, "VecSparse"), 2);

    // Both present: indexed, with the HASH value in its key-string encoding.
    put_raw_item(
        &storage,
        "VecSparse",
        json!({
            "pk": {"S": "both"},
            "tenant": {"S": "acme"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    assert_eq!(shadow_row_count(&storage, "VecSparse::vector::vix"), 1);
    let (hash_value, _, _) = shadow_row(&storage, "VecSparse::vector::vix");
    assert_eq!(hash_value, "S:acme");
}

#[tokio::test(flavor = "current_thread")]
async fn ttl_sweep_deindexes_expired_items() {
    use dynoxide::actions::update_time_to_live::{
        TimeToLiveSpecification, UpdateTimeToLiveRequest,
    };

    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecTtl").await;
    dynoxide::actions::update_time_to_live::execute(
        &storage,
        UpdateTimeToLiveRequest {
            table_name: "VecTtl".to_string(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "expires".to_string(),
                enabled: true,
            },
        },
    )
    .await
    .unwrap();

    // One long-expired item, one that never expires.
    put_raw_item(
        &storage,
        "VecTtl",
        json!({
            "pk": {"S": "expired"},
            "expires": {"N": "1000"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    put_raw_item(
        &storage,
        "VecTtl",
        json!({
            "pk": {"S": "alive"},
            "expires": {"N": "99999999999"},
            "embedding": {"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]}
        }),
    )
    .await;
    assert_eq!(shadow_row_count(&storage, "VecTtl::vector::vix"), 2);

    let deleted = dynoxide::ttl::sweep_expired_items(&storage).await.unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(shadow_row_count(&storage, "VecTtl"), 1);
    assert_eq!(shadow_row_count(&storage, "VecTtl::vector::vix"), 1);
    let (_, vector_json, _) = shadow_row(&storage, "VecTtl::vector::vix");
    assert_eq!(vector_json, "[0.0,1.0,0.0]");
}

// ---------------------------------------------------------------------------
// Captured write-validation errors: each rejection is wholesale, leaving the
// base table and the shadow table unchanged
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn put_wrong_dimension_count_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecDims").await;

    let err = try_put_raw_item(
        &storage,
        "VecDims",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1"}, {"N": "2"}]}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    // Captured from real DynamoDB (eu-west-2, 2026-08-11): full stop after
    // `invalid`, no stop before `IndexName`.
    assert_eq!(
        err,
        "One or more parameter values were invalid. Invalid size for parameter embedding, \
         Expected: 3, Actual: 2 IndexName: vix"
    );
    assert_eq!(shadow_row_count(&storage, "VecDims"), 0);
    assert_eq!(shadow_row_count(&storage, "VecDims::vector::vix"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn put_wrong_element_type_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecElem").await;

    let err = try_put_raw_item(
        &storage,
        "VecElem",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1"}, {"S": "x"}, {"N": "3"}]}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    // Captured from real DynamoDB (eu-west-2, 2026-08-11): the element
    // position is zero-based, and a full stop precedes `IndexName`.
    assert_eq!(
        err,
        "One or more parameter values were invalid. Invalid type for parameter embedding[1], \
         Expected: 32-bit floating point number, Actual: S. IndexName: vix"
    );
    assert_eq!(shadow_row_count(&storage, "VecElem"), 0);
    assert_eq!(shadow_row_count(&storage, "VecElem::vector::vix"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn put_non_list_vector_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecList").await;

    let err = try_put_raw_item(
        &storage,
        "VecList",
        json!({
            "pk": {"S": "a"},
            "embedding": {"S": "not-a-list"}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    // Captured from real DynamoDB (eu-west-2, 2026-08-11): no stop before
    // `IndexName` on this form.
    assert_eq!(
        err,
        "One or more parameter values were invalid. Invalid type for parameter embedding, \
         Expected: 32-bit floating point number list IndexName: vix"
    );
    assert_eq!(shadow_row_count(&storage, "VecList"), 0);
    assert_eq!(shadow_row_count(&storage, "VecList::vector::vix"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn put_out_of_range_element_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecRange").await;

    let err = try_put_raw_item(
        &storage,
        "VecRange",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1E+39"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    // Captured from real DynamoDB (eu-west-2 and us-east-1, 2026-08-12):
    // parameter path and raw value interpolated, scientific-notation bounds
    // exactly as shown.
    assert_eq!(
        err,
        "One or more parameter values were invalid. Invalid value for parameter embedding[0], \
         Value: 1E+39 is outside valid range [-3.4028235E38, 3.4028235E38]. IndexName: vix"
    );
    assert_eq!(shadow_row_count(&storage, "VecRange"), 0);
    assert_eq!(shadow_row_count(&storage, "VecRange::vector::vix"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn put_empty_string_hash_value_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_hash_schema_vector_table(&storage, "VecEmptyHash").await;

    let err = try_put_raw_item(
        &storage,
        "VecEmptyHash",
        json!({
            "pk": {"S": "a"},
            "tenant": {"S": ""},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    // The classic secondary-index empty-string message with the
    // IndexName/IndexKey suffixes (captured eu-west-2, 2026-08-11).
    assert_eq!(
        err,
        "One or more parameter values are not valid. A value specified for a secondary \
         index key is not supported. The AttributeValue for a key attribute cannot \
         contain an empty string value. IndexName: vix, IndexKey: tenant"
    );
    assert_eq!(shadow_row_count(&storage, "VecEmptyHash"), 0);
    assert_eq!(shadow_row_count(&storage, "VecEmptyHash::vector::vix"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn put_type_mismatched_hash_value_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_hash_schema_vector_table(&storage, "VecHashType").await;

    let err = try_put_raw_item(
        &storage,
        "VecHashType",
        json!({
            "pk": {"S": "a"},
            "tenant": {"N": "7"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    // Captured from real DynamoDB (eu-west-2 and us-east-1, 2026-08-12):
    // full stop after `invalid`, matching the write-path family.
    assert_eq!(
        err,
        "One or more parameter values were invalid. Attribute 'tenant' type mismatch. \
         Expected: S, Actual: N. IndexName: vix"
    );
    assert_eq!(shadow_row_count(&storage, "VecHashType"), 0);
    assert_eq!(shadow_row_count(&storage, "VecHashType::vector::vix"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn update_item_setting_invalid_vector_rejected_and_state_unchanged() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecUpdBad").await;

    put_raw_item(
        &storage,
        "VecUpdBad",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;

    let update = serde_json::from_value(json!({
        "TableName": "VecUpdBad",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET embedding = :v",
        "ExpressionAttributeValues": {
            ":v": {"L": [{"N": "1"}, {"N": "2"}]}
        }
    }))
    .unwrap();
    let err = dynoxide::actions::update_item::execute(&storage, update)
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid. Invalid size for parameter embedding, \
         Expected: 3, Actual: 2 IndexName: vix"
    );

    // The base item and the shadow row both keep their pre-update state.
    let base_json: String = storage
        .conn()
        .query_row("SELECT item_json FROM \"VecUpdBad\"", [], |r| r.get(0))
        .unwrap();
    let base: serde_json::Value = serde_json::from_str(&base_json).unwrap();
    assert_eq!(
        base["embedding"],
        json!({"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]})
    );
    let (_, vector_json, _) = shadow_row(&storage, "VecUpdBad::vector::vix");
    assert_eq!(vector_json, "[1.0,0.0,0.0]");
}

#[tokio::test(flavor = "current_thread")]
async fn backfill_skipped_item_rejected_when_re_put_while_index_exists() {
    let storage = Storage::memory().unwrap();
    create_plain_ppr_table(&storage, "VecAsym").await;

    // Written before any index exists: accepted.
    put_raw_item(
        &storage,
        "VecAsym",
        json!({
            "pk": {"S": "short"},
            "embedding": {"L": [{"N": "1"}, {"N": "2"}]}
        }),
    )
    .await;

    // Backfill sparse-skips the wrong-dimension item (captured from real
    // DynamoDB, eu-west-2 and us-east-1, 2026-08-12).
    update_table_raw(
        &storage,
        json!({
            "TableName": "VecAsym",
            "VectorIndexUpdates": [{"Create": vix_json("vix")}]
        }),
    )
    .await
    .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecAsym::vector::vix"), 0);

    // The captured asymmetry: the same item backfill skipped is rejected if
    // re-put while the index exists.
    let err = try_put_raw_item(
        &storage,
        "VecAsym",
        json!({
            "pk": {"S": "short"},
            "embedding": {"L": [{"N": "1"}, {"N": "2"}]}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid. Invalid size for parameter embedding, \
         Expected: 3, Actual: 2 IndexName: vix"
    );

    // The pre-existing item survives the rejection untouched.
    assert_eq!(shadow_row_count(&storage, "VecAsym"), 1);
    assert_eq!(shadow_row_count(&storage, "VecAsym::vector::vix"), 0);
}

// ---------------------------------------------------------------------------
// BatchWriteItem, TransactWriteItems, and multi-index behaviour
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn batch_write_item_maintains_vector_indexes_like_put_item() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecBatch").await;

    let put_batch = serde_json::from_value(json!({
        "RequestItems": {
            "VecBatch": [
                {"PutRequest": {"Item": {
                    "pk": {"S": "a"},
                    "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
                }}},
                {"PutRequest": {"Item": {
                    "pk": {"S": "b"},
                    "embedding": {"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]}
                }}}
            ]
        }
    }))
    .unwrap();
    dynoxide::actions::batch_write_item::execute(&storage, put_batch)
        .await
        .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecBatch::vector::vix"), 2);

    let delete_batch = serde_json::from_value(json!({
        "RequestItems": {
            "VecBatch": [
                {"DeleteRequest": {"Key": {"pk": {"S": "a"}}}}
            ]
        }
    }))
    .unwrap();
    dynoxide::actions::batch_write_item::execute(&storage, delete_batch)
        .await
        .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecBatch::vector::vix"), 1);
    let (_, vector_json, _) = shadow_row(&storage, "VecBatch::vector::vix");
    assert_eq!(vector_json, "[0.0,1.0,0.0]");
}

#[tokio::test(flavor = "current_thread")]
async fn batch_write_with_invalid_vector_rejects_before_any_write() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecBatchBad").await;

    let batch = serde_json::from_value(json!({
        "RequestItems": {
            "VecBatchBad": [
                {"PutRequest": {"Item": {
                    "pk": {"S": "valid"},
                    "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
                }}},
                {"PutRequest": {"Item": {
                    "pk": {"S": "invalid"},
                    "embedding": {"L": [{"N": "1"}, {"N": "2"}]}
                }}}
            ]
        }
    }))
    .unwrap();
    let err = dynoxide::actions::batch_write_item::execute(&storage, batch)
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid. Invalid size for parameter embedding, \
         Expected: 3, Actual: 2 IndexName: vix"
    );
    // Validation runs before any write, so the valid sibling landed nowhere.
    assert_eq!(shadow_row_count(&storage, "VecBatchBad"), 0);
    assert_eq!(shadow_row_count(&storage, "VecBatchBad::vector::vix"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn transact_write_items_maintains_vector_indexes_like_put_item() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecTx").await;

    let tx = serde_json::from_value(json!({
        "TransactItems": [
            {"Put": {
                "TableName": "VecTx",
                "Item": {
                    "pk": {"S": "a"},
                    "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
                }
            }},
            {"Put": {
                "TableName": "VecTx",
                "Item": {
                    "pk": {"S": "b"},
                    "embedding": {"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]}
                }
            }}
        ]
    }))
    .unwrap();
    dynoxide::actions::transact_write_items::execute(&storage, tx)
        .await
        .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecTx::vector::vix"), 2);

    let tx = serde_json::from_value(json!({
        "TransactItems": [
            {"Delete": {
                "TableName": "VecTx",
                "Key": {"pk": {"S": "a"}}
            }}
        ]
    }))
    .unwrap();
    dynoxide::actions::transact_write_items::execute(&storage, tx)
        .await
        .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecTx::vector::vix"), 1);
}

/// One vector index's arm of a `ConsumedCapacity`, in bytes.
fn vector_arm(cc: &dynoxide::types::ConsumedCapacity, name: &str) -> Option<f64> {
    cc.vector_indexes
        .as_ref()
        .and_then(|m| m.get(name))
        .map(|d| d.vector_write_request_bytes)
}

/// Vector replication is reported in bytes under `INDEXES` alone, on its own
/// axis: the figure never joins `CapacityUnits`, and `TOTAL` carries no vector
/// fields at all (captured 2026-08-11, eu-west-2).
#[tokio::test(flavor = "current_thread")]
async fn vector_write_reports_bytes_under_indexes_and_nothing_under_total() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecWCap").await;

    let put = |rcc: &str, pk: &str| {
        serde_json::from_value(json!({
            "TableName": "VecWCap",
            "Item": {
                "pk": {"S": pk},
                "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
            },
            "ReturnConsumedCapacity": rcc
        }))
        .unwrap()
    };

    let cc = dynoxide::actions::put_item::execute(&storage, put("INDEXES", "a"))
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    assert_eq!(vector_arm(&cc, "vix"), Some(1024.0));
    // The bytes stay off the unit total: one table write, nothing more.
    assert_eq!(cc.capacity_units, 1.0);

    let cc = dynoxide::actions::put_item::execute(&storage, put("TOTAL", "b"))
        .await
        .unwrap()
        .consumed_capacity
        .expect("TOTAL reports capacity");
    assert!(
        cc.vector_indexes.is_none(),
        "TOTAL carries no vector fields at all"
    );
    assert_eq!(cc.capacity_units, 1.0);
}

/// Replication is delta-based, so an overwrite that leaves the index's stored
/// view alone reports no map at all rather than a zeroed arm (captured).
#[tokio::test(flavor = "current_thread")]
async fn identical_overwrite_reports_no_vector_map_and_a_changed_vector_does() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecDelta").await;

    let put = |vector: serde_json::Value| {
        serde_json::from_value(json!({
            "TableName": "VecDelta",
            "Item": {"pk": {"S": "a"}, "embedding": vector},
            "ReturnConsumedCapacity": "INDEXES"
        }))
        .unwrap()
    };
    let original = json!({"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]});

    dynoxide::actions::put_item::execute(&storage, put(original.clone()))
        .await
        .unwrap();

    let cc = dynoxide::actions::put_item::execute(&storage, put(original))
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    assert!(
        cc.vector_indexes.is_none(),
        "an identical overwrite charges nothing, so the map is absent"
    );

    let cc = dynoxide::actions::put_item::execute(
        &storage,
        put(json!({"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]})),
    )
    .await
    .unwrap()
    .consumed_capacity
    .expect("INDEXES reports capacity");
    assert_eq!(vector_arm(&cc, "vix"), Some(1024.0));
}

/// A table with no vector index never carries the map, and neither does a
/// write to a vector-indexed table that leaves the vector attribute alone.
#[tokio::test(flavor = "current_thread")]
async fn writes_that_touch_no_vector_index_carry_no_map() {
    let storage = Storage::memory().unwrap();
    let req: CreateTableRequest = serde_json::from_value(full_house_request("VecNone")).unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();

    // Member of the GSI and the LSI, but carrying no vector attribute.
    let put = serde_json::from_value(json!({
        "TableName": "VecNone",
        "Item": {
            "pk": {"S": "a"}, "sk": {"S": "1"},
            "gsi_pk": {"S": "g"}, "lsi_sk": {"S": "l"}
        },
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let cc = dynoxide::actions::put_item::execute(&storage, put)
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    assert!(cc.vector_indexes.is_none());
    // The classic arms are unaffected by the vector index existing.
    assert_eq!(index_arm(&cc.global_secondary_indexes, "gsi1"), Some(1.0));
    assert_eq!(index_arm(&cc.local_secondary_indexes, "lsi1"), Some(1.0));
}

/// A delete charges the index the removed item was a member of, sized on the
/// row it held.
#[tokio::test(flavor = "current_thread")]
async fn delete_charges_the_vector_index_the_item_belonged_to() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecDelCap").await;

    let put = serde_json::from_value(json!({
        "TableName": "VecDelCap",
        "Item": {"pk": {"S": "a"}, "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}}
    }))
    .unwrap();
    dynoxide::actions::put_item::execute(&storage, put)
        .await
        .unwrap();

    let del = serde_json::from_value(json!({
        "TableName": "VecDelCap",
        "Key": {"pk": {"S": "a"}},
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let cc = dynoxide::actions::delete_item::execute(&storage, del)
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    assert_eq!(vector_arm(&cc, "vix"), Some(1024.0));

    // Deleting what is no longer there charges nothing.
    let del = serde_json::from_value(json!({
        "TableName": "VecDelCap",
        "Key": {"pk": {"S": "a"}},
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let cc = dynoxide::actions::delete_item::execute(&storage, del)
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    assert!(cc.vector_indexes.is_none());
}

/// An empty vector attribute name is refused at the request-model layer. The
/// API model bounds it at one character, which the AWS CLI enforces before the
/// request leaves the client. Left unbounded it created an index that reported
/// ACTIVE and could never hold a row, because no item can carry an attribute
/// with no name.
#[tokio::test(flavor = "current_thread")]
async fn empty_vector_attribute_name_rejected_at_the_request_model_layer() {
    let storage = Storage::memory().unwrap();
    let req: Result<CreateTableRequest, _> = serde_json::from_value(json!({
        "TableName": "VecEmptyAttr",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{
            "IndexName": "vix",
            "VectorAttribute": {"AttributeName": ""},
            "Projection": {"ProjectionType": "ALL"},
            "Dimensions": 3,
            "DistanceFunction": "COSINE"
        }]
    }));
    let err = match req {
        Err(e) => e.to_string(),
        Ok(r) => dynoxide::actions::create_table::execute(&storage, r)
            .await
            .expect_err("an empty vector attribute name is refused")
            .to_string(),
    };
    assert!(
        err.contains("vectorIndexes.1.member.vectorAttribute.attributeName")
            && err.contains("Member must have length greater than or equal to 1"),
        "got {err}"
    );
}

/// Member names are case sensitive on the wire, and every guard around the
/// update parser reads the wire spelling. A second spelling accepted in the
/// parser would reach it and miss the request-model collector and the
/// pre-deserialisation type checks, so a lowercase action would apply with
/// nothing validating it.
#[tokio::test(flavor = "current_thread")]
async fn a_lowercase_action_key_does_not_bypass_the_wire_guards() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "WireCase").await;

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "WireCase",
        "VectorIndexUpdates": [{"create": {
            "IndexName": "sneak",
            "VectorAttribute": {"AttributeName": ""},
            "Projection": {"ProjectionType": "ALL"},
            "Dimensions": 3,
            "DistanceFunction": "COSINE"
        }}]
    }))
    .expect("the lowercase key deserialises, carrying no recognised action");

    let err = dynoxide::actions::update_table::execute(&storage, req, &VectorIndexLifecycle::new())
        .await
        .expect_err("an unrecognised action is refused, not applied unvalidated");
    assert!(
        err.to_string().contains("must not be null"),
        "expected the no-action rejection, got {err}"
    );

    // And nothing was created behind it.
    let desc = describe_raw(&storage, "WireCase").await;
    let names: Vec<String> = desc
        .table
        .vector_indexes
        .unwrap_or_default()
        .iter()
        .map(|v| v.index_name.clone())
        .collect();
    assert!(!names.contains(&"sneak".to_string()), "got {names:?}");
}

/// Query and Scan refuse a vector index by type, as PartiQL does, rather than
/// reporting an index DescribeTable lists as missing. Captured against
/// eu-west-2 on 2026-08-19. The two messages are not symmetric: Query's ends
/// with a full stop and Scan's does not, which is AWS's own inconsistency and
/// not a transcription slip.
#[tokio::test(flavor = "current_thread")]
async fn query_and_scan_refuse_a_vector_index_by_type() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecQS").await;

    let q: dynoxide::actions::query::QueryRequest = serde_json::from_value(json!({
        "TableName": "VecQS",
        "IndexName": "vix",
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": "a"}}
    }))
    .unwrap();
    let err = dynoxide::actions::query::execute(&storage, q)
        .await
        .expect_err("a vector index is not queryable");
    assert_eq!(
        err.to_string(),
        "Query operation not supported on this index type."
    );

    let sc: dynoxide::actions::scan::ScanRequest = serde_json::from_value(json!({
        "TableName": "VecQS",
        "IndexName": "vix"
    }))
    .unwrap();
    let err = dynoxide::actions::scan::execute(&storage, sc)
        .await
        .expect_err("a vector index is not scannable");
    assert_eq!(
        err.to_string(),
        "Scan operation not supported on this index type"
    );
}

/// An index that is not any of the three kinds still reports absence, and a
/// consistent read against a vector index keeps the GSI wording. Both captured
/// the same day: AWS itself says "global secondary indexes" there, so matching
/// it means repeating a phrase that is wrong about the index type.
#[tokio::test(flavor = "current_thread")]
async fn the_vector_refusal_does_not_swallow_the_neighbouring_rejections() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecQS2").await;

    let sc: dynoxide::actions::scan::ScanRequest = serde_json::from_value(json!({
        "TableName": "VecQS2",
        "IndexName": "nosuchindex"
    }))
    .unwrap();
    let err = dynoxide::actions::scan::execute(&storage, sc)
        .await
        .expect_err("an unknown index is still unknown");
    assert!(
        err.to_string()
            .contains("does not have the specified index"),
        "got {err}"
    );

    let q: dynoxide::actions::query::QueryRequest = serde_json::from_value(json!({
        "TableName": "VecQS2",
        "IndexName": "vix",
        "ConsistentRead": true,
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": "a"}}
    }))
    .unwrap();
    let err = dynoxide::actions::query::execute(&storage, q)
        .await
        .expect_err("a consistent read on a vector index is refused");
    assert_eq!(
        err.to_string(),
        "Consistent reads are not supported on global secondary indexes",
        "the consistent-read check fires before the index-type one, as captured"
    );
}

/// The captured byte formula, pinned above the 1KB floor where it is actually
/// visible. Every figure here was observed against real DynamoDB in eu-west-2
/// on 2026-08-18:
///
///   bytes = 4 * dimensions + vector attribute name + item size of the rest of
///           the projected entry, floored at 1024
///
/// The vector is billed at its f32 width, not as the decimal text it stores as,
/// and it is counted once even when the projection also carries it.
#[tokio::test(flavor = "current_thread")]
async fn vector_write_bytes_match_the_captured_formula_above_the_floor() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecBytes").await;

    // 3 dimensions, pk "a", plus a 1500-byte blob.
    // 12 (vector) + 3 (pk) + 9 (embedding name) + 1504 (blob) = 1528
    let put = |blob_len: usize, pk: &str| {
        serde_json::from_value(json!({
            "TableName": "VecBytes",
            "Item": {
                "pk": {"S": pk},
                "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]},
                "blob": {"S": "y".repeat(blob_len)}
            },
            "ReturnConsumedCapacity": "INDEXES"
        }))
        .unwrap()
    };

    let cc = dynoxide::actions::put_item::execute(&storage, put(1500, "a"))
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert_eq!(
        vector_arm(&cc, "vix"),
        Some(1528.0),
        "3-dim + 1500-byte blob"
    );

    // 12 + 3 + 9 + 3004 = 3028
    let cc = dynoxide::actions::put_item::execute(&storage, put(3000, "b"))
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert_eq!(
        vector_arm(&cc, "vix"),
        Some(3028.0),
        "3-dim + 3000-byte blob"
    );

    // Shrinking an entry is charged on the larger of the two images, so
    // overwriting the 3000-byte item with a 1500-byte one still costs 3028 and
    // not 1528. Without this the two images are the same size everywhere else
    // in the suite, and taking the smaller of the pair passes unnoticed.
    let cc = dynoxide::actions::put_item::execute(&storage, put(1500, "b"))
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert_eq!(
        vector_arm(&cc, "vix"),
        Some(3028.0),
        "a shrinking overwrite is charged on the larger image"
    );
}

/// The same formula at 512 dimensions, across both projection types, which is
/// where billing the vector as JSON text rather than as f32 diverges most.
#[tokio::test(flavor = "current_thread")]
async fn vector_write_bytes_bill_the_vector_at_f32_width_once() {
    let storage = Storage::memory().unwrap();
    let vec512: Vec<serde_json::Value> = (0..512).map(|_| json!({"N": "0.5"})).collect();
    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": "VecWide",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [
            {"IndexName": "vall", "VectorAttribute": {"AttributeName": "embedding"},
             "Dimensions": 512, "DistanceFunction": "COSINE",
             "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "vkeys", "VectorAttribute": {"AttributeName": "embedding"},
             "Dimensions": 512, "DistanceFunction": "COSINE",
             "Projection": {"ProjectionType": "KEYS_ONLY"}}
        ]
    }))
    .unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();

    let put = |note: &str| {
        serde_json::from_value(json!({
            "TableName": "VecWide",
            "Item": {"pk": {"S": "a"}, "embedding": {"L": vec512}, "note": {"S": note}},
            "ReturnConsumedCapacity": "INDEXES"
        }))
        .unwrap()
    };

    let cc = dynoxide::actions::put_item::execute(&storage, put("x"))
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    // KEYS_ONLY: 2048 + 3 (pk) + 9 (embedding name) = 2060
    assert_eq!(vector_arm(&cc, "vkeys"), Some(2060.0));
    // ALL also projects note (4 + 1): 2065
    assert_eq!(vector_arm(&cc, "vall"), Some(2065.0));

    // Changing only the non-vector attribute charges the index that projects
    // it and leaves the one that does not entirely absent. Captured against
    // eu-west-2 with exactly this fixture.
    let cc = dynoxide::actions::put_item::execute(&storage, put("y"))
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert_eq!(vector_arm(&cc, "vall"), Some(2065.0));
    assert_eq!(
        vector_arm(&cc, "vkeys"),
        None,
        "KEYS_ONLY does not project the changed attribute, so its view is untouched"
    );

    // Changing only the vector charges both indexes, KEYS_ONLY included: the
    // shadow row stores the vector whatever the projection, so its view moves
    // either way. Captured against eu-west-2 on 2026-08-19 with this shape.
    let wide_other: Vec<serde_json::Value> = (0..512).map(|_| json!({"N": "0.25"})).collect();
    let vector_only: dynoxide::actions::put_item::PutItemRequest = serde_json::from_value(json!({
        "TableName": "VecWide",
        "Item": {"pk": {"S": "a"}, "embedding": {"L": wide_other}, "note": {"S": "y"}},
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let cc = dynoxide::actions::put_item::execute(&storage, vector_only)
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert_eq!(vector_arm(&cc, "vall"), Some(2065.0), "vector-only change");
    assert_eq!(
        vector_arm(&cc, "vkeys"),
        Some(2060.0),
        "a vector-only change moves the KEYS_ONLY view too"
    );

    // A delete is charged on the image it removes, so it clears the floor on
    // the same fixture. Every other delete assertion in this suite sits at
    // 1024 and cannot tell a correct delete charge from a floored one.
    let del: dynoxide::actions::delete_item::DeleteItemRequest = serde_json::from_value(json!({
        "TableName": "VecWide",
        "Key": {"pk": {"S": "a"}},
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let cc = dynoxide::actions::delete_item::execute(&storage, del)
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert_eq!(vector_arm(&cc, "vall"), Some(2065.0), "delete, ALL");
    assert_eq!(vector_arm(&cc, "vkeys"), Some(2060.0), "delete, KEYS_ONLY");
}

/// The batch and transactional surfaces report the vector arm too. A batch
/// sums the bytes across its items; a transaction charges the index at its
/// single-write cost, as the classic arms are (the 2x factor reaches the base
/// table arm alone).
#[tokio::test(flavor = "current_thread")]
async fn batch_and_transact_report_the_vector_arm() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecMulti").await;

    let batch = serde_json::from_value(json!({
        "RequestItems": {
            "VecMulti": [
                {"PutRequest": {"Item": {
                    "pk": {"S": "a"},
                    "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
                }}},
                {"PutRequest": {"Item": {
                    "pk": {"S": "b"},
                    "embedding": {"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]}
                }}}
            ]
        },
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let caps = dynoxide::actions::batch_write_item::execute(&storage, batch)
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    let cc = caps
        .iter()
        .find(|c| c.table_name == "VecMulti")
        .expect("the written table reports an entry");
    // Two newly indexed items, each at the billable floor.
    assert_eq!(vector_arm(cc, "vix"), Some(2048.0));

    let tx = serde_json::from_value(json!({
        "TransactItems": [
            {"Put": {
                "TableName": "VecMulti",
                "Item": {
                    "pk": {"S": "c"},
                    "embedding": {"L": [{"N": "0"}, {"N": "0"}, {"N": "1"}]}
                }
            }}
        ],
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let caps = dynoxide::actions::transact_write_items::execute(&storage, tx)
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    let cc = caps
        .iter()
        .find(|c| c.table_name == "VecMulti")
        .expect("the written table reports an entry");
    assert_eq!(vector_arm(cc, "vix"), Some(1024.0));
    // The transactional factor doubles the base table arm and leaves the
    // vector bytes alone.
    assert_eq!(cc.capacity_units, 2.0);
}

/// TransactWriteItems carries a separate vector maintenance call for each of
/// its three action kinds. Put and Delete are covered above; this pins Update,
/// which creates the item on the first call and de-indexes it on the second.
#[tokio::test(flavor = "current_thread")]
async fn transact_update_maintains_vector_indexes_like_update_item() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecTxUpd").await;

    let tx = serde_json::from_value(json!({
        "TransactItems": [
            {"Update": {
                "TableName": "VecTxUpd",
                "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "SET embedding = :v",
                "ExpressionAttributeValues": {
                    ":v": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
                }
            }}
        ]
    }))
    .unwrap();
    dynoxide::actions::transact_write_items::execute(&storage, tx)
        .await
        .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecTxUpd::vector::vix"), 1);

    // Removing the vector attribute de-indexes through the same path, leaving
    // the base item in place.
    let tx = serde_json::from_value(json!({
        "TransactItems": [
            {"Update": {
                "TableName": "VecTxUpd",
                "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "REMOVE embedding"
            }}
        ]
    }))
    .unwrap();
    dynoxide::actions::transact_write_items::execute(&storage, tx)
        .await
        .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecTxUpd::vector::vix"), 0);
    assert_eq!(shadow_row_count(&storage, "VecTxUpd"), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn transact_rollback_leaves_no_shadow_row() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecTxRb").await;

    // The first put is valid; the second fails its condition (nothing exists
    // at that key), cancelling the transaction. The first put's shadow row
    // must roll back with its base write.
    let tx = serde_json::from_value(json!({
        "TransactItems": [
            {"Put": {
                "TableName": "VecTxRb",
                "Item": {
                    "pk": {"S": "a"},
                    "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
                }
            }},
            {"Put": {
                "TableName": "VecTxRb",
                "Item": {"pk": {"S": "b"}},
                "ConditionExpression": "attribute_exists(pk)"
            }}
        ]
    }))
    .unwrap();
    dynoxide::actions::transact_write_items::execute(&storage, tx)
        .await
        .unwrap_err();
    assert_eq!(shadow_row_count(&storage, "VecTxRb"), 0);
    assert_eq!(shadow_row_count(&storage, "VecTxRb::vector::vix"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn write_valid_for_one_index_and_invalid_for_another_rejects_wholesale() {
    let storage = Storage::memory().unwrap();
    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": "VecMulti",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [
            {
                "IndexName": "vixa",
                "VectorAttribute": {"AttributeName": "veca"},
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "vixb",
                "VectorAttribute": {"AttributeName": "vecb"},
                "Dimensions": 2,
                "DistanceFunction": "EUCLIDEAN",
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    }))
    .unwrap();
    dynoxide::actions::create_table::execute(&storage, req)
        .await
        .unwrap();

    let err = try_put_raw_item(
        &storage,
        "VecMulti",
        json!({
            "pk": {"S": "a"},
            "veca": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]},
            "vecb": {"L": [{"N": "1"}]}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid. Invalid size for parameter vecb, \
         Expected: 2, Actual: 1 IndexName: vixb"
    );
    // The rejection is wholesale: nothing lands in the base table or in
    // either shadow table, including the index the write was valid for.
    assert_eq!(shadow_row_count(&storage, "VecMulti"), 0);
    assert_eq!(shadow_row_count(&storage, "VecMulti::vector::vixa"), 0);
    assert_eq!(shadow_row_count(&storage, "VecMulti::vector::vixb"), 0);
}

// ---------------------------------------------------------------------------
// PartiQL write paths
// ---------------------------------------------------------------------------

/// As [`exec_partiql`], asking for per-index capacity so the vector arm is
/// observable.
async fn exec_partiql_indexes(
    storage: &Storage,
    statement: &str,
) -> dynoxide::actions::execute_statement::ExecuteStatementResponse {
    dynoxide::actions::execute_statement::execute(
        storage,
        dynoxide::actions::execute_statement::ExecuteStatementRequest {
            statement: statement.to_string(),
            return_consumed_capacity: Some("INDEXES".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

/// Run a PartiQL statement against the storage, returning the result.
async fn exec_partiql(
    storage: &Storage,
    statement: &str,
    parameters: Vec<dynoxide::types::AttributeValue>,
) -> dynoxide::Result<dynoxide::actions::execute_statement::ExecuteStatementResponse> {
    dynoxide::actions::execute_statement::execute(
        storage,
        dynoxide::actions::execute_statement::ExecuteStatementRequest {
            statement: statement.to_string(),
            parameters: if parameters.is_empty() {
                None
            } else {
                Some(parameters)
            },
            ..Default::default()
        },
    )
    .await
}

/// A three-element vector as a parameter value.
fn vector_param(a: &str, b: &str, c: &str) -> dynoxide::types::AttributeValue {
    serde_json::from_value(json!({"L": [{"N": a}, {"N": b}, {"N": c}]})).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn partiql_insert_indexes_a_valid_vector() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecPqIns").await;

    exec_partiql(
        &storage,
        "INSERT INTO \"VecPqIns\" VALUE {'pk': 'a', 'embedding': ?}",
        vec![vector_param("1", "0", "0")],
    )
    .await
    .unwrap();

    assert_eq!(shadow_row_count(&storage, "VecPqIns::vector::vix"), 1);
    let (_, vector_json, _) = shadow_row(&storage, "VecPqIns::vector::vix");
    assert_eq!(vector_json, "[1.0,0.0,0.0]");
}

#[tokio::test(flavor = "current_thread")]
async fn partiql_update_refreshes_the_shadow_row() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecPqUpd").await;

    put_raw_item(
        &storage,
        "VecPqUpd",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;

    // A vector change through PartiQL UPDATE replaces the shadow row's copy.
    exec_partiql(
        &storage,
        "UPDATE \"VecPqUpd\" SET embedding = ? WHERE pk = 'a'",
        vec![vector_param("0", "1", "0")],
    )
    .await
    .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecPqUpd::vector::vix"), 1);
    let (_, vector_json, _) = shadow_row(&storage, "VecPqUpd::vector::vix");
    assert_eq!(vector_json, "[0.0,1.0,0.0]");

    // A non-vector change refreshes the ALL-projection item copy too.
    exec_partiql(
        &storage,
        "UPDATE \"VecPqUpd\" SET note = 'touched' WHERE pk = 'a'",
        vec![],
    )
    .await
    .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecPqUpd::vector::vix"), 1);
    let (_, vector_json, item_json) = shadow_row(&storage, "VecPqUpd::vector::vix");
    assert_eq!(vector_json, "[0.0,1.0,0.0]");
    let item: serde_json::Value = serde_json::from_str(&item_json).unwrap();
    assert_eq!(item["note"], json!({"S": "touched"}));
}

#[tokio::test(flavor = "current_thread")]
async fn partiql_delete_removes_the_shadow_row() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecPqDel").await;

    put_raw_item(
        &storage,
        "VecPqDel",
        json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    assert_eq!(shadow_row_count(&storage, "VecPqDel::vector::vix"), 1);

    exec_partiql(&storage, "DELETE FROM \"VecPqDel\" WHERE pk = 'a'", vec![])
        .await
        .unwrap();

    assert_eq!(shadow_row_count(&storage, "VecPqDel::vector::vix"), 0);
    assert_eq!(shadow_row_count(&storage, "VecPqDel"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn partiql_insert_with_invalid_vector_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecPqBad").await;

    let wrong_dims: dynoxide::types::AttributeValue =
        serde_json::from_value(json!({"L": [{"N": "1"}, {"N": "2"}]})).unwrap();
    let err = exec_partiql(
        &storage,
        "INSERT INTO \"VecPqBad\" VALUE {'pk': 'a', 'embedding': ?}",
        vec![wrong_dims],
    )
    .await
    .unwrap_err()
    .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid. Invalid size for parameter embedding, \
         Expected: 3, Actual: 2 IndexName: vix"
    );
    assert_eq!(shadow_row_count(&storage, "VecPqBad"), 0);
    assert_eq!(shadow_row_count(&storage, "VecPqBad::vector::vix"), 0);
}

// ---------------------------------------------------------------------------
// Changed-value gating, imports, and further captured validations
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn unrelated_update_leaves_backfill_skipped_item_unindexed() {
    let storage = Storage::memory().unwrap();
    create_plain_ppr_table(&storage, "VecGate").await;

    // Written before any index exists, then sparse-skipped by the backfill.
    put_raw_item(
        &storage,
        "VecGate",
        json!({
            "pk": {"S": "short"},
            "embedding": {"L": [{"N": "1"}, {"N": "2"}]}
        }),
    )
    .await;
    update_table_raw(
        &storage,
        json!({
            "TableName": "VecGate",
            "VectorIndexUpdates": [{"Create": vix_json("vix")}]
        }),
    )
    .await
    .unwrap();
    assert_eq!(shadow_row_count(&storage, "VecGate::vector::vix"), 0);

    // An update that never touches the vector attribute must not re-reject
    // the pre-existing invalid value, and the item stays unindexed.
    let update = serde_json::from_value(json!({
        "TableName": "VecGate",
        "Key": {"pk": {"S": "short"}},
        "UpdateExpression": "SET note = :n",
        "ExpressionAttributeValues": {":n": {"S": "touched"}}
    }))
    .unwrap();
    dynoxide::actions::update_item::execute(&storage, update)
        .await
        .unwrap();

    assert_eq!(shadow_row_count(&storage, "VecGate::vector::vix"), 0);
    let base_json: String = storage
        .conn()
        .query_row("SELECT item_json FROM \"VecGate\"", [], |r| r.get(0))
        .unwrap();
    let base: serde_json::Value = serde_json::from_str(&base_json).unwrap();
    assert_eq!(base["note"], json!({"S": "touched"}));
    assert_eq!(base["embedding"], json!({"L": [{"N": "1"}, {"N": "2"}]}));
}

#[tokio::test(flavor = "current_thread")]
async fn import_sparse_skips_invalid_vector_values() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecImp").await;

    // Imports are backfill-shaped: the wrong-dimension item lands in the base
    // table with no shadow row, instead of failing the whole import the way a
    // live write rejects.
    let items: Vec<dynoxide::types::Item> = vec![
        serde_json::from_value(json!({
            "pk": {"S": "good"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }))
        .unwrap(),
        serde_json::from_value(json!({
            "pk": {"S": "short"},
            "embedding": {"L": [{"N": "1"}, {"N": "2"}]}
        }))
        .unwrap(),
    ];
    let res = dynoxide::actions::import_items::execute(
        &storage,
        "VecImp",
        items,
        &dynoxide::ImportOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(res.items_imported, 2);

    assert_eq!(shadow_row_count(&storage, "VecImp"), 2);
    assert_eq!(shadow_row_count(&storage, "VecImp::vector::vix"), 1);
    let table_pk: String = storage
        .conn()
        .query_row("SELECT table_pk FROM \"VecImp::vector::vix\"", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(table_pk, "S:good");
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_import_with_intra_batch_duplicate_leaves_no_ghost_row() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecImpDup").await;

    // The fresh-import fast path skips index deletes, so without dedupe the
    // first occurrence's shadow row would survive the second occurrence's
    // overwrite of the base row. The last occurrence wins wholesale.
    let items: Vec<dynoxide::types::Item> = vec![
        serde_json::from_value(json!({
            "pk": {"S": "a"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }))
        .unwrap(),
        serde_json::from_value(json!({"pk": {"S": "a"}})).unwrap(),
    ];
    dynoxide::actions::import_items::execute_skip_gsi_deletes(
        &storage,
        "VecImpDup",
        items,
        &dynoxide::ImportOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(shadow_row_count(&storage, "VecImpDup"), 1);
    assert_eq!(shadow_row_count(&storage, "VecImpDup::vector::vix"), 0);
    let base_json: String = storage
        .conn()
        .query_row("SELECT item_json FROM \"VecImpDup\"", [], |r| r.get(0))
        .unwrap();
    let base: serde_json::Value = serde_json::from_str(&base_json).unwrap();
    assert!(
        base.get("embedding").is_none(),
        "the last occurrence (without the vector) must own the base row, got: {base}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn update_setting_empty_string_hash_value_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_hash_schema_vector_table(&storage, "VecUpdEmptyHash").await;

    put_raw_item(
        &storage,
        "VecUpdEmptyHash",
        json!({
            "pk": {"S": "a"},
            "tenant": {"S": "acme"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;

    let update = serde_json::from_value(json!({
        "TableName": "VecUpdEmptyHash",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET tenant = :t",
        "ExpressionAttributeValues": {":t": {"S": ""}}
    }))
    .unwrap();
    let err = dynoxide::actions::update_item::execute(&storage, update)
        .await
        .unwrap_err()
        .to_string();
    // The update-expression form drops the IndexName/IndexKey suffix
    // (captured eu-west-2 and us-east-1, 2026-08-13, byte-identical).
    assert_eq!(
        err,
        "One or more parameter values are not valid. The update expression attempted to \
         update a secondary index key to a value that is not supported. The AttributeValue \
         for a key attribute cannot contain an empty string value."
    );

    // The shadow row keeps its pre-update state.
    assert_eq!(
        shadow_row_count(&storage, "VecUpdEmptyHash::vector::vix"),
        1
    );
    let (hash_value, _, _) = shadow_row(&storage, "VecUpdEmptyHash::vector::vix");
    assert_eq!(hash_value, "S:acme");
}

/// As [`create_hash_schema_vector_table`], but with `tenant` declared `B` in
/// AttributeDefinitions.
async fn create_binary_hash_schema_vector_table(storage: &Storage, table: &str) {
    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": table,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "tenant", "AttributeType": "B"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{
            "IndexName": "vix",
            "VectorAttribute": {"AttributeName": "embedding"},
            "SearchSchema": [
                {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"}
            ],
            "Dimensions": 3,
            "DistanceFunction": "COSINE",
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .unwrap();
    dynoxide::actions::create_table::execute(storage, req)
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn put_empty_binary_hash_value_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_binary_hash_schema_vector_table(&storage, "VecEmptyBin").await;

    let err = try_put_raw_item(
        &storage,
        "VecEmptyBin",
        json!({
            "pk": {"S": "a"},
            "tenant": {"B": ""},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    // The binary analogue of the empty-string rejection, suffixes included
    // (captured eu-west-2 and us-east-1, 2026-08-13, byte-identical).
    assert_eq!(
        err,
        "One or more parameter values are not valid. A value specified for a secondary \
         index key is not supported. The AttributeValue for a key attribute cannot \
         contain an empty binary value. IndexName: vix, IndexKey: tenant"
    );
    assert_eq!(shadow_row_count(&storage, "VecEmptyBin"), 0);
    assert_eq!(shadow_row_count(&storage, "VecEmptyBin::vector::vix"), 0);
}

/// A table whose SearchSchema carries a HASH element (`tenant`, declared `S`)
/// and an INLINE_FILTER element (`category`, declared `S`).
async fn create_filter_schema_vector_table(storage: &Storage, table: &str) {
    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": table,
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
            "DistanceFunction": "COSINE",
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .unwrap();
    dynoxide::actions::create_table::execute(storage, req)
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn put_type_mismatched_inline_filter_value_rejected_with_captured_string() {
    let storage = Storage::memory().unwrap();
    create_filter_schema_vector_table(&storage, "VecFilterType").await;

    let err = try_put_raw_item(
        &storage,
        "VecFilterType",
        json!({
            "pk": {"S": "a"},
            "tenant": {"S": "acme"},
            "category": {"N": "5"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await
    .unwrap_err()
    .to_string();
    // INLINE_FILTER elements are type-checked at write, in the same format as
    // the HASH mismatch (captured eu-west-2 and us-east-1, 2026-08-13).
    assert_eq!(
        err,
        "One or more parameter values were invalid. Attribute 'category' type mismatch. \
         Expected: S, Actual: N. IndexName: vix"
    );
    assert_eq!(shadow_row_count(&storage, "VecFilterType"), 0);
    assert_eq!(shadow_row_count(&storage, "VecFilterType::vector::vix"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn update_table_backfill_skips_empty_binary_and_mismatched_filter_values() {
    let storage = Storage::memory().unwrap();
    create_plain_ppr_table(&storage, "VecFillSkips").await;

    put_raw_item(
        &storage,
        "VecFillSkips",
        json!({
            "pk": {"S": "scoped"},
            "tenant": {"B": "AQ=="},
            "category": {"S": "x"},
            "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}
        }),
    )
    .await;
    // An empty-binary HASH value and a type-mismatched INLINE_FILTER value
    // are both rejected by a live write once the index exists (captured
    // eu-west-2 and us-east-1, 2026-08-13), so backfill skips both rows.
    put_raw_item(
        &storage,
        "VecFillSkips",
        json!({
            "pk": {"S": "empty-bin"},
            "tenant": {"B": ""},
            "embedding": {"L": [{"N": "0"}, {"N": "1"}, {"N": "0"}]}
        }),
    )
    .await;
    put_raw_item(
        &storage,
        "VecFillSkips",
        json!({
            "pk": {"S": "bad-filter"},
            "tenant": {"B": "AQ=="},
            "category": {"N": "5"},
            "embedding": {"L": [{"N": "0"}, {"N": "0"}, {"N": "1"}]}
        }),
    )
    .await;

    update_table_raw(
        &storage,
        json!({
            "TableName": "VecFillSkips",
            "AttributeDefinitions": [
                {"AttributeName": "tenant", "AttributeType": "B"},
                {"AttributeName": "category", "AttributeType": "S"}
            ],
            "VectorIndexUpdates": [{"Create": {
                "IndexName": "vix",
                "VectorAttribute": {"AttributeName": "embedding"},
                "SearchSchema": [
                    {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"},
                    {"AttributeName": "category", "SearchSchemaElementType": "INLINE_FILTER"}
                ],
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }),
    )
    .await
    .unwrap();

    assert_eq!(shadow_row_count(&storage, "VecFillSkips::vector::vix"), 1);
    let table_pk: String = storage
        .conn()
        .query_row(
            "SELECT table_pk FROM \"VecFillSkips::vector::vix\"",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(table_pk, "S:scoped");
}

/// The item-level write surfaces, each of which threads its own old image and
/// capacity mode into the vector fan-out by hand, so a wrong variable at any
/// one of them compiles and passes every other test. The statement surfaces
/// are covered by `partiql_write_paths_report_the_vector_arm` and
/// `statement_batch_and_transaction_report_the_vector_arm`; between the three
/// every call site that can report the arm has an assertion.
#[tokio::test(flavor = "current_thread")]
async fn item_write_surfaces_report_the_vector_arm() {
    // UpdateItem: an upsert that creates the row, then a REMOVE that drops it.
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecU").await;

    let upsert = serde_json::from_value(json!({
        "TableName": "VecU",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET embedding = :v",
        "ExpressionAttributeValues": {":v": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}},
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let cc = dynoxide::actions::update_item::execute(&storage, upsert)
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert_eq!(vector_arm(&cc, "vix"), Some(1024.0), "UpdateItem creating");

    let remove = serde_json::from_value(json!({
        "TableName": "VecU",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "REMOVE embedding",
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let cc = dynoxide::actions::update_item::execute(&storage, remove)
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert_eq!(
        vector_arm(&cc, "vix"),
        Some(1024.0),
        "UpdateItem de-indexing charges the row it removed"
    );

    // BatchWriteItem delete.
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecBD").await;
    let seed = serde_json::from_value(json!({
        "TableName": "VecBD",
        "Item": {"pk": {"S": "a"}, "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}}
    }))
    .unwrap();
    dynoxide::actions::put_item::execute(&storage, seed)
        .await
        .unwrap();
    let batch = serde_json::from_value(json!({
        "RequestItems": {"VecBD": [{"DeleteRequest": {"Key": {"pk": {"S": "a"}}}}]},
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let caps = dynoxide::actions::batch_write_item::execute(&storage, batch)
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    let cc = caps.iter().find(|c| c.table_name == "VecBD").unwrap();
    assert_eq!(vector_arm(cc, "vix"), Some(1024.0), "BatchWriteItem delete");

    // TransactWriteItems update and delete.
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecTC").await;
    let tx = serde_json::from_value(json!({
        "TransactItems": [{"Update": {
            "TableName": "VecTC",
            "Key": {"pk": {"S": "a"}},
            "UpdateExpression": "SET embedding = :v",
            "ExpressionAttributeValues": {":v": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}}
        }}],
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let caps = dynoxide::actions::transact_write_items::execute(&storage, tx)
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    let cc = caps.iter().find(|c| c.table_name == "VecTC").unwrap();
    assert_eq!(vector_arm(cc, "vix"), Some(1024.0), "transacted Update");

    let tx = serde_json::from_value(json!({
        "TransactItems": [{"Delete": {"TableName": "VecTC", "Key": {"pk": {"S": "a"}}}}],
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let caps = dynoxide::actions::transact_write_items::execute(&storage, tx)
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    let cc = caps.iter().find(|c| c.table_name == "VecTC").unwrap();
    assert_eq!(vector_arm(cc, "vix"), Some(1024.0), "transacted Delete");
}

/// The two statement surfaces that reach the fan-out through
/// `per_table_capacity` rather than through their own builder. Both were
/// unobserved: `ExecuteTransaction` rebuilds a bare `WriteCapacity` for its
/// read arm and relies on a clone for the write arm to carry the vector bytes,
/// so a refactor that built the write arm the same way would drop the map with
/// nothing failing.
#[tokio::test(flavor = "current_thread")]
async fn statement_batch_and_transaction_report_the_vector_arm() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecSB").await;
    create_vector_table(&storage, "VecST").await;

    let batch: dynoxide::actions::batch_execute_statement::BatchExecuteStatementRequest =
        serde_json::from_value(json!({
            "Statements": [
                {"Statement": "INSERT INTO \"VecSB\" VALUE {'pk': 'a', 'embedding': [1, 0, 0]}"}
            ],
            "ReturnConsumedCapacity": "INDEXES"
        }))
        .unwrap();
    let caps = dynoxide::actions::batch_execute_statement::execute(&storage, batch)
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    let cc = caps
        .iter()
        .find(|c| c.table_name == "VecSB")
        .expect("the written table reports an arm");
    assert_eq!(vector_arm(cc, "vix"), Some(1024.0), "BatchExecuteStatement");

    let txn: dynoxide::actions::execute_transaction::ExecuteTransactionRequest =
        serde_json::from_value(json!({
            "TransactStatements": [
                {"Statement": "INSERT INTO \"VecST\" VALUE {'pk': 'a', 'embedding': [1, 0, 0]}"}
            ],
            "ReturnConsumedCapacity": "INDEXES"
        }))
        .unwrap();
    let caps = dynoxide::actions::execute_transaction::execute(&storage, txn)
        .await
        .unwrap()
        .consumed_capacity
        .expect("INDEXES reports capacity");
    let cc = caps
        .iter()
        .find(|c| c.table_name == "VecST")
        .expect("the written table reports an arm");
    assert_eq!(vector_arm(cc, "vix"), Some(1024.0), "ExecuteTransaction");
}

/// The three PartiQL write paths, which carry their own copy of the wiring.
#[tokio::test(flavor = "current_thread")]
async fn partiql_write_paths_report_the_vector_arm() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecPQ").await;

    let resp = exec_partiql_indexes(
        &storage,
        "INSERT INTO \"VecPQ\" VALUE {'pk': 'a', 'embedding': [1, 0, 0]}",
    )
    .await;
    let cc = resp.consumed_capacity.expect("INSERT reports capacity");
    assert_eq!(vector_arm(&cc, "vix"), Some(1024.0), "PartiQL INSERT");

    let resp = exec_partiql_indexes(
        &storage,
        "UPDATE \"VecPQ\" SET embedding = [0, 1, 0] WHERE pk = 'a'",
    )
    .await;
    let cc = resp.consumed_capacity.expect("UPDATE reports capacity");
    assert_eq!(vector_arm(&cc, "vix"), Some(1024.0), "PartiQL UPDATE");

    let resp = exec_partiql_indexes(&storage, "DELETE FROM \"VecPQ\" WHERE pk = 'a'").await;
    let cc = resp.consumed_capacity.expect("DELETE reports capacity");
    assert_eq!(vector_arm(&cc, "vix"), Some(1024.0), "PartiQL DELETE");
}

/// The wire shape, asserted as JSON rather than through Rust fields, so a
/// serde rename typo cannot pass. The search side is pinned the same way in
/// tests/search_vectors.rs.
#[tokio::test(flavor = "current_thread")]
async fn the_vector_arm_serialises_under_the_captured_key_names() {
    let storage = Storage::memory().unwrap();
    create_vector_table(&storage, "VecWire").await;

    let put = serde_json::from_value(json!({
        "TableName": "VecWire",
        "Item": {"pk": {"S": "a"}, "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}},
        "ReturnConsumedCapacity": "INDEXES"
    }))
    .unwrap();
    let resp = dynoxide::actions::put_item::execute(&storage, put)
        .await
        .unwrap();
    let body = serde_json::to_value(&resp).unwrap();
    assert_eq!(
        body["ConsumedCapacity"]["VectorIndexes"],
        json!({"vix": {"VectorWriteRequestBytes": 1024.0}})
    );
}

#[test]
fn update_table_search_schema_missing_attribute_name_rejected_at_request_model_layer() {
    // The message the MCP surface is pinned to as well, in
    // `a_create_missing_a_member_is_named_the_way_the_wire_names_it`. Both
    // spell it out in full so the two cannot drift apart quietly.
    let mut create = vix_json("vix");
    create.as_object_mut().unwrap().insert(
        "SearchSchema".to_string(),
        json!([{"SearchSchemaElementType": "HASH"}]),
    );
    let err = request_model_error(serde_json::from_value::<UpdateTableRequest>(json!({
        "TableName": "VecSchemaNoName",
        "VectorIndexUpdates": [{"Create": create}]
    })));
    assert_eq!(
        err,
        "1 validation error detected: Value null at \
         'vectorIndexUpdates.1.member.create.searchSchema.1.member.attributeName' failed to \
         satisfy constraint: Member must not be null"
    );
}

#[test]
fn create_table_negative_dimensions_reports_the_value_as_given() {
    // The request-model collector reads the raw value, so a negative count is
    // echoed as it arrived rather than as the clamp would leave it. Pinned here
    // and on the MCP surface, which had been normalising before collecting and
    // so reported the clamped value instead.
    let mut vix = vix_json("vix");
    vix.as_object_mut()
        .unwrap()
        .insert("Dimensions".to_string(), json!(-1));
    let err = request_model_error(serde_json::from_value::<CreateTableRequest>(json!({
        "TableName": "VecNegDims",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [vix]
    })));
    assert!(
        err.contains("Value '-1' at 'vectorIndexes.1.member.dimensions'"),
        "the raw value is what gets echoed: {err}"
    );
}

// ---------------------------------------------------------------------------
// The creation lifecycle of an index added to a live table
//
// Real DynamoDB puts an index added through UpdateTable through a visible
// CREATING phase, reports the base table ACTIVE beside it, and refuses to drop
// the table underneath it (captured eu-west-2, 2026-08-11 and 2026-08-21).
// These drive the clock rather than waiting on it.
// ---------------------------------------------------------------------------

/// An engine whose clock a test drives, so a creation window can be crossed
/// without waiting.
fn db_with_manual_clock(clock: &ManualClock) -> Database {
    Database::memory_with_clock(clock.arc()).unwrap()
}

/// A live table with five items and a vector index added afterwards, which
/// leaves the index inside its creation window.
fn table_with_an_index_added_afterwards(db: &Database, table: &str) {
    db.create_table(parse(json!({
        "TableName": table,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    })))
    .unwrap();
    for i in 0..5 {
        db.put_item(
            serde_json::from_value(json!({
                "TableName": table,
                "Item": {
                    "pk": {"S": format!("item-{i}")},
                    "embedding": {"L": [{"N": i.to_string()}, {"N": "1"}, {"N": "0"}]}
                }
            }))
            .unwrap(),
        )
        .unwrap();
    }
    add_vector_index(db, table, "vix");
}

fn add_vector_index(db: &Database, table: &str, index: &str) {
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": table,
        "VectorIndexUpdates": [{"Create": vix_json(index)}]
    }))
    .unwrap();
    db.update_table(req).unwrap();
}

fn delete_vector_index(db: &Database, table: &str, index: &str) -> dynoxide::Result<()> {
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": table,
        "VectorIndexUpdates": [{"Delete": {"IndexName": index}}]
    }))
    .unwrap();
    db.update_table(req).map(|_| ())
}

fn only_vix(db: &Database, table: &str) -> dynoxide::actions::VectorIndexDescription {
    describe(db, table)
        .table
        .vector_indexes
        .as_ref()
        .unwrap()
        .iter()
        .find(|v| v.index_name == "vix")
        .unwrap()
        .clone()
}

#[test]
fn the_table_reports_active_while_the_index_it_is_adding_reports_creating() {
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecLive");

    // Table and index come from one description, or a transition between two
    // calls would make the pairing meaningless.
    let desc = describe(&db, "VecLive");
    assert_eq!(desc.table.table_status, "ACTIVE");
    let vix = &desc.table.vector_indexes.as_ref().unwrap()[0];
    assert_eq!(vix.index_status, "CREATING");
    assert_eq!(vix.backfilling, Some(true));

    // Past the window the field leaves the description rather than turning
    // false, which is why a readiness check written as `Backfilling == false`
    // never fires.
    clock.tick(std::time::Duration::from_secs(60));
    let desc = describe(&db, "VecLive");
    assert_eq!(desc.table.table_status, "ACTIVE");
    let vix = &desc.table.vector_indexes.as_ref().unwrap()[0];
    assert_eq!(vix.index_status, "ACTIVE");
    assert_eq!(vix.backfilling, None);
    let body = serde_json::to_string(&desc).unwrap();
    assert!(
        !body.contains("Backfilling"),
        "Backfilling should be absent once ACTIVE, got: {body}"
    );
}

#[test]
fn an_unrelated_update_reports_the_creating_index_in_its_own_response() {
    // An UpdateTable touching something else is accepted while a vector index
    // is creating, and it returns a full table description. That description
    // has to agree with the one DescribeTable gives at the same instant.
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecOther");

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecOther",
        "DeletionProtectionEnabled": true
    }))
    .unwrap();
    let resp = db.update_table(req).unwrap();
    let vix = &resp.table_description.vector_indexes.as_ref().unwrap()[0];
    assert_eq!(vix.index_status, "CREATING");
    assert_eq!(vix.backfilling, Some(true));
    assert_eq!(only_vix(&db, "VecOther").index_status, "CREATING");
}

#[test]
fn the_table_cannot_be_deleted_while_its_index_is_creating() {
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecHold");

    let err = db
        .delete_table(DeleteTableRequest {
            table_name: "VecHold".to_string(),
        })
        .unwrap_err();
    assert!(
        matches!(err, dynoxide::DynoxideError::ResourceInUseException(_)),
        "expected a ResourceInUseException, got {err:?}"
    );
    assert_eq!(
        err.to_string(),
        "Cannot delete table while indexes are being created, updated, or deleted."
    );
}

#[test]
fn the_refusal_survives_any_number_of_describes() {
    // The documented readiness pattern polls DescribeTable until ACTIVE, which
    // is unbounded. A refusal that ran out after a fixed number of looks would
    // pass a conformance test only because the test broke on its first poll.
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecPoll");

    for _ in 0..50 {
        assert_eq!(only_vix(&db, "VecPoll").index_status, "CREATING");
        let err = db
            .delete_table(DeleteTableRequest {
                table_name: "VecPoll".to_string(),
            })
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Cannot delete table while indexes are being created, updated, or deleted."
        );
    }
}

#[test]
fn the_table_deletes_once_the_index_has_finished_creating() {
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecFree");

    clock.tick(std::time::Duration::from_secs(60));
    let resp = db
        .delete_table(DeleteTableRequest {
            table_name: "VecFree".to_string(),
        })
        .unwrap();
    assert_eq!(resp.table_description.table_status, "DELETING");
}

#[test]
fn a_table_with_no_vector_indexes_deletes_unaffected() {
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    db.create_table(parse(json!({
        "TableName": "VecNone",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    })))
    .unwrap();

    db.delete_table(DeleteTableRequest {
        table_name: "VecNone".to_string(),
    })
    .unwrap();
}

#[test]
fn a_protected_table_answers_deletion_protection_rather_than_the_creating_index() {
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecGuard");
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecGuard",
        "DeletionProtectionEnabled": true
    }))
    .unwrap();
    db.update_table(req).unwrap();

    let err = db
        .delete_table(DeleteTableRequest {
            table_name: "VecGuard".to_string(),
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("protected against deletion"),
        "expected the deletion-protection message, got {err}"
    );
}

#[test]
fn dropping_a_table_leaves_no_lifecycle_entry_behind() {
    // A table recreated under the same name, with an index of the same name
    // created alongside it, must be searchable at once: the entry the dropped
    // table left would otherwise hold it inside a window it never entered.
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecReuse");
    clock.tick(std::time::Duration::from_secs(60));
    db.delete_table(DeleteTableRequest {
        table_name: "VecReuse".to_string(),
    })
    .unwrap();

    db.create_table(parse(base_request("VecReuse", json!([vix_json("vix")]))))
        .unwrap();
    let vix = only_vix(&db, "VecReuse");
    assert_eq!(vix.index_status, "ACTIVE");
    assert_eq!(vix.backfilling, None);
}

#[test]
fn cancelling_a_creating_index_clears_its_state_and_frees_the_table() {
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecCancel");
    assert_eq!(only_vix(&db, "VecCancel").index_status, "CREATING");

    delete_vector_index(&db, "VecCancel", "vix").unwrap();

    let desc = describe(&db, "VecCancel");
    assert!(desc.table.vector_indexes.is_none());
    assert_eq!(desc.table.table_status, "ACTIVE");

    // Deletable straight away, with no wait: without the disarm the entry the
    // cancelled index left would keep refusing.
    db.delete_table(DeleteTableRequest {
        table_name: "VecCancel".to_string(),
    })
    .unwrap();
}

#[test]
fn deleting_an_index_that_has_finished_creating_still_works() {
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecSettled");
    clock.tick(std::time::Duration::from_secs(60));

    delete_vector_index(&db, "VecSettled", "vix").unwrap();
    assert!(describe(&db, "VecSettled").table.vector_indexes.is_none());
}

#[test]
fn two_creates_in_one_call_still_answer_the_one_online_action_limit() {
    let clock = ManualClock::new(1_700_000_000);
    let db = db_with_manual_clock(&clock);
    table_with_an_index_added_afterwards(&db, "VecLimit");

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "VecLimit",
        "VectorIndexUpdates": [
            {"Create": vix_json("vix2")},
            {"Create": vix_json("vix3")}
        ]
    }))
    .unwrap();
    let err = db.update_table(req).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Subscriber limit exceeded: Only 1 online index can be created or deleted \
         simultaneously per table"
    );
}
