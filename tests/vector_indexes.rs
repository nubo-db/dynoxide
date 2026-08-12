//! Vector index control-plane tests: CreateTable acceptance and validation,
//! DescribeTable reflection, and DeleteTable cleanup.
//!
//! Error strings are pinned byte-for-byte to real DynamoDB behaviour captured
//! in eu-west-2 on 2026-08-11, with a follow-up capture on 2026-08-12 that
//! was byte-identical in eu-west-2 and us-east-1.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::delete_table::DeleteTableRequest;
use dynoxide::actions::describe_table::DescribeTableRequest;
use dynoxide::storage::Storage;
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
fn request_model_error(
    result: std::result::Result<CreateTableRequest, serde_json::Error>,
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
