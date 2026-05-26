use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::delete_table::DeleteTableRequest;
use dynoxide::actions::describe_table::DescribeTableRequest;
use dynoxide::actions::update_table::UpdateTableRequest;
use dynoxide::types::*;

fn make_db() -> Database {
    Database::memory().unwrap()
}

fn basic_request(name: &str) -> CreateTableRequest {
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

#[test]
fn test_create_table_with_sse_specification() {
    let db = make_db();

    let mut req = basic_request("SseTable");
    req.sse_specification = Some(SseSpecification {
        enabled: Some(true),
        sse_type: Some("KMS".to_string()),
        kms_master_key_id: Some("arn:aws:kms:us-east-1:123456789:key/my-key".to_string()),
    });

    let resp = db.create_table(req).unwrap();
    assert_eq!(resp.table_description.table_name, "SseTable");

    // Verify via DescribeTable
    let desc = db
        .describe_table(DescribeTableRequest {
            table_name: "SseTable".to_string(),
        })
        .unwrap();
    let sse = desc
        .table
        .sse_description
        .expect("SSEDescription should be present");
    assert_eq!(sse.status, "ENABLED");
    assert_eq!(sse.sse_type.as_deref(), Some("KMS"));
}

#[test]
fn test_create_table_with_table_class() {
    let db = make_db();

    let mut req = basic_request("ClassTable");
    req.table_class = Some("STANDARD_INFREQUENT_ACCESS".to_string());

    let resp = db.create_table(req).unwrap();
    let summary = resp
        .table_description
        .table_class_summary
        .expect("TableClassSummary should be present");
    assert_eq!(summary.table_class, "STANDARD_INFREQUENT_ACCESS");
}

#[test]
fn test_create_table_with_tags() {
    let db = make_db();

    let mut req = basic_request("TaggedTable");
    req.tags = Some(vec![
        Tag {
            key: "Environment".to_string(),
            value: "Production".to_string(),
        },
        Tag {
            key: "Team".to_string(),
            value: "Backend".to_string(),
        },
    ]);

    let _resp = db.create_table(req).unwrap();

    // Verify tags via ListTagsOfResource
    let tags_resp = db
        .list_tags_of_resource(
            dynoxide::actions::list_tags_of_resource::ListTagsOfResourceRequest {
                resource_arn: Some(
                    "arn:aws:dynamodb:dynoxide:000000000000:table/TaggedTable".to_string(),
                ),
            },
        )
        .unwrap();

    assert_eq!(tags_resp.tags.len(), 2);
    let keys: Vec<&str> = tags_resp.tags.iter().map(|t| t.key.as_str()).collect();
    assert!(keys.contains(&"Environment"));
    assert!(keys.contains(&"Team"));
}

#[test]
fn test_create_table_with_deletion_protection_prevents_delete() {
    let db = make_db();

    let mut req = basic_request("ProtectedTable");
    req.deletion_protection_enabled = Some(true);

    let _resp = db.create_table(req).unwrap();

    // Attempt to delete should fail
    let result = db.delete_table(DeleteTableRequest {
        table_name: "ProtectedTable".to_string(),
    });
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("deletion protection"),
        "Expected deletion protection error, got: {err_msg}"
    );
}

#[test]
fn test_update_table_disable_deletion_protection_then_delete() {
    let db = make_db();

    let mut req = basic_request("ToggleProtection");
    req.deletion_protection_enabled = Some(true);
    let _resp = db.create_table(req).unwrap();

    // Verify deletion fails
    let result = db.delete_table(DeleteTableRequest {
        table_name: "ToggleProtection".to_string(),
    });
    assert!(result.is_err());

    // Disable deletion protection via UpdateTable
    let update_req = UpdateTableRequest {
        table_name: "ToggleProtection".to_string(),
        deletion_protection_enabled: Some(false),
        ..Default::default()
    };
    let _update_resp = db.update_table(update_req).unwrap();

    // Verify DescribeTable shows disabled
    let desc = db
        .describe_table(DescribeTableRequest {
            table_name: "ToggleProtection".to_string(),
        })
        .unwrap();
    assert_eq!(desc.table.deletion_protection_enabled, Some(false));

    // Now deletion should succeed
    let result = db.delete_table(DeleteTableRequest {
        table_name: "ToggleProtection".to_string(),
    });
    assert!(result.is_ok());
}

#[test]
fn test_create_table_with_all_optional_params() {
    let db = make_db();

    let mut req = basic_request("FullTable");
    req.sse_specification = Some(SseSpecification {
        enabled: Some(true),
        sse_type: Some("KMS".to_string()),
        kms_master_key_id: None,
    });
    req.table_class = Some("STANDARD".to_string());
    req.tags = Some(vec![Tag {
        key: "App".to_string(),
        value: "test".to_string(),
    }]);
    req.deletion_protection_enabled = Some(true);

    let resp = db.create_table(req).unwrap();
    let desc = &resp.table_description;

    assert_eq!(desc.table_name, "FullTable");
    assert_eq!(desc.sse_description.as_ref().unwrap().status, "ENABLED");
    assert_eq!(
        desc.table_class_summary.as_ref().unwrap().table_class,
        "STANDARD"
    );
    assert_eq!(desc.deletion_protection_enabled, Some(true));
}

#[test]
fn test_create_table_without_optional_params_succeeds() {
    let db = make_db();

    let req = basic_request("BasicTable");
    let resp = db.create_table(req).unwrap();

    assert_eq!(resp.table_description.table_name, "BasicTable");
    // When not specified, deletion_protection_enabled is None (matching DynamoDB)
    assert!(
        resp.table_description.deletion_protection_enabled.is_none()
            || resp.table_description.deletion_protection_enabled == Some(false)
    );
    assert!(resp.table_description.sse_description.is_none());
    assert!(resp.table_description.table_class_summary.is_none());
}

#[test]
fn test_create_table_sse_enabled_only_completes_shape() {
    // Issue #44: SSESpecification { Enabled: true } with no SSEType or
    // KMSMasterKeyId must still round-trip SSEType=KMS and a KMS key ARN,
    // matching real AWS's AWS-managed-key default.
    let db = make_db();

    let mut req = basic_request("SseDefaultKey");
    req.sse_specification = Some(SseSpecification {
        enabled: Some(true),
        sse_type: None,
        kms_master_key_id: None,
    });
    db.create_table(req).unwrap();

    let desc = db
        .describe_table(DescribeTableRequest {
            table_name: "SseDefaultKey".to_string(),
        })
        .unwrap();
    let sse = desc
        .table
        .sse_description
        .expect("SSEDescription should be present");
    assert_eq!(sse.status, "ENABLED");
    assert_eq!(sse.sse_type.as_deref(), Some("KMS"));
    assert!(
        sse.kms_master_key_arn
            .as_deref()
            .is_some_and(|arn| arn.starts_with("arn:aws:kms:")),
        "expected a KMS key ARN, got: {:?}",
        sse.kms_master_key_arn
    );
}

#[test]
fn test_create_table_on_demand_throughput_round_trips() {
    // Issue #44: OnDemandThroughput set at create time must round-trip through
    // both the CreateTable response and a DescribeTable call.
    let db = make_db();

    let mut req = basic_request("OnDemandTable");
    req.billing_mode = Some("PAY_PER_REQUEST".to_string());
    req.on_demand_throughput = Some(OnDemandThroughput {
        max_read_request_units: Some(10),
        max_write_request_units: Some(5),
    });

    let resp = db.create_table(req).unwrap();
    let created = resp
        .table_description
        .on_demand_throughput
        .as_ref()
        .expect("CreateTable response should carry OnDemandThroughput");
    assert_eq!(created.max_read_request_units, Some(10));
    assert_eq!(created.max_write_request_units, Some(5));

    let desc = db
        .describe_table(DescribeTableRequest {
            table_name: "OnDemandTable".to_string(),
        })
        .unwrap();
    let odt = desc
        .table
        .on_demand_throughput
        .expect("OnDemandThroughput should round-trip via DescribeTable");
    assert_eq!(odt.max_read_request_units, Some(10));
    assert_eq!(odt.max_write_request_units, Some(5));
}

#[test]
fn test_describe_table_omits_on_demand_throughput_when_unset() {
    // Issue #44: a table created without OnDemandThroughput must not synthesise one.
    let db = make_db();

    let resp = db.create_table(basic_request("NoOnDemand")).unwrap();
    assert!(resp.table_description.on_demand_throughput.is_none());

    let desc = db
        .describe_table(DescribeTableRequest {
            table_name: "NoOnDemand".to_string(),
        })
        .unwrap();
    assert!(desc.table.on_demand_throughput.is_none());
}

#[test]
fn test_delete_table_protected_returns_exact_aws_message() {
    // Issue #46: DeleteTable on a protected table must return the exact AWS
    // message, not the ARN-prefixed form Dynoxide used to emit.
    let db = make_db();

    let mut req = basic_request("ProtectedExactMsg");
    req.deletion_protection_enabled = Some(true);
    db.create_table(req).unwrap();

    let err = db
        .delete_table(DeleteTableRequest {
            table_name: "ProtectedExactMsg".to_string(),
        })
        .expect_err("delete of a protected table must fail");

    assert!(
        matches!(err, dynoxide::errors::DynoxideError::ValidationException(_)),
        "expected ValidationException, got: {err:?}"
    );
    assert_eq!(
        format!("{err}"),
        "Resource cannot be deleted as it is currently protected against deletion. \
         Disable deletion protection first."
    );
}
