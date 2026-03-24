use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::list_tags_of_resource::ListTagsOfResourceRequest;
use dynoxide::actions::tag_resource::TagResourceRequest;
use dynoxide::actions::untag_resource::UntagResourceRequest;
use dynoxide::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType, Tag};

fn make_db() -> Database {
    Database::memory().unwrap()
}

fn create_table(db: &Database, name: &str) {
    let req = CreateTableRequest {
        table_name: name.to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "PK".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "PK".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        ..Default::default()
    };
    db.create_table(req).unwrap();
}

fn table_arn(name: &str) -> String {
    format!("arn:aws:dynamodb:dynoxide:000000000000:table/{name}")
}

fn tag(key: &str, value: &str) -> Tag {
    Tag {
        key: key.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn test_tag_resource() {
    let db = make_db();
    create_table(&db, "TestTable");

    let req = TagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tags: vec![tag("env", "production"), tag("team", "backend")],
    };
    db.tag_resource(req).unwrap();

    let resp = db
        .list_tags_of_resource(ListTagsOfResourceRequest {
            resource_arn: Some(table_arn("TestTable")),
        })
        .unwrap();

    assert_eq!(resp.tags.len(), 2);
    assert!(
        resp.tags
            .iter()
            .any(|t| t.key == "env" && t.value == "production")
    );
    assert!(
        resp.tags
            .iter()
            .any(|t| t.key == "team" && t.value == "backend")
    );
}

#[test]
fn test_tag_resource_overwrites_existing_key() {
    let db = make_db();
    create_table(&db, "TestTable");

    db.tag_resource(TagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tags: vec![tag("env", "staging")],
    })
    .unwrap();

    // Overwrite with new value
    db.tag_resource(TagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tags: vec![tag("env", "production")],
    })
    .unwrap();

    let resp = db
        .list_tags_of_resource(ListTagsOfResourceRequest {
            resource_arn: Some(table_arn("TestTable")),
        })
        .unwrap();

    assert_eq!(resp.tags.len(), 1);
    assert_eq!(resp.tags[0].value, "production");
}

#[test]
fn test_untag_resource() {
    let db = make_db();
    create_table(&db, "TestTable");

    db.tag_resource(TagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tags: vec![
            tag("env", "prod"),
            tag("team", "backend"),
            tag("cost", "high"),
        ],
    })
    .unwrap();

    db.untag_resource(UntagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tag_keys: vec!["env".to_string(), "cost".to_string()],
    })
    .unwrap();

    let resp = db
        .list_tags_of_resource(ListTagsOfResourceRequest {
            resource_arn: Some(table_arn("TestTable")),
        })
        .unwrap();

    assert_eq!(resp.tags.len(), 1);
    assert_eq!(resp.tags[0].key, "team");
}

#[test]
fn test_untag_resource_nonexistent_key_is_noop() {
    let db = make_db();
    create_table(&db, "TestTable");

    db.tag_resource(TagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tags: vec![tag("env", "prod")],
    })
    .unwrap();

    // Remove a key that doesn't exist — should succeed
    db.untag_resource(UntagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tag_keys: vec!["nonexistent".to_string()],
    })
    .unwrap();

    let resp = db
        .list_tags_of_resource(ListTagsOfResourceRequest {
            resource_arn: Some(table_arn("TestTable")),
        })
        .unwrap();
    assert_eq!(resp.tags.len(), 1);
}

#[test]
fn test_list_tags_empty() {
    let db = make_db();
    create_table(&db, "TestTable");

    let resp = db
        .list_tags_of_resource(ListTagsOfResourceRequest {
            resource_arn: Some(table_arn("TestTable")),
        })
        .unwrap();

    assert!(resp.tags.is_empty());
}

#[test]
fn test_tag_resource_max_50() {
    let db = make_db();
    create_table(&db, "TestTable");

    // Add 50 tags — should succeed
    let tags: Vec<Tag> = (0..50)
        .map(|i| tag(&format!("key{i}"), &format!("val{i}")))
        .collect();
    db.tag_resource(TagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tags,
    })
    .unwrap();

    // Adding one more should fail
    let err = db
        .tag_resource(TagResourceRequest {
            resource_arn: Some(table_arn("TestTable")),
            tags: vec![tag("key50", "val50")],
        })
        .unwrap_err();
    assert!(err.to_string().contains("tag limit is 50"));
}

#[test]
fn test_tag_resource_invalid_arn() {
    let db = make_db();

    let err = db
        .tag_resource(TagResourceRequest {
            resource_arn: Some("not-a-valid-arn".to_string()),
            tags: vec![tag("env", "prod")],
        })
        .unwrap_err();
    assert!(err.to_string().contains("Invalid TableArn"));
}

#[test]
fn test_tag_resource_nonexistent_table() {
    let db = make_db();

    let err = db
        .tag_resource(TagResourceRequest {
            resource_arn: Some(table_arn("DoesNotExist")),
            tags: vec![tag("env", "prod")],
        })
        .unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[test]
fn test_list_tags_nonexistent_table_returns_access_denied() {
    let db = make_db();

    let err = db
        .list_tags_of_resource(ListTagsOfResourceRequest {
            resource_arn: Some(table_arn("DoesNotExist")),
        })
        .unwrap_err();
    assert!(matches!(
        err,
        dynoxide::errors::DynoxideError::AccessDeniedException(_)
    ));
    assert!(err.to_string().contains("not authorized"));
}

#[test]
fn test_tags_independent_of_describe_table() {
    let db = make_db();
    create_table(&db, "TestTable");

    db.tag_resource(TagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tags: vec![tag("env", "prod")],
    })
    .unwrap();

    // DescribeTable should not include tags (DynamoDB behaviour)
    let desc_req = serde_json::from_value(serde_json::json!({
        "TableName": "TestTable"
    }))
    .unwrap();
    let resp = db.describe_table(desc_req).unwrap();
    // TableDescription struct has no tags field — this verifies the separation
    assert_eq!(resp.table.table_name, "TestTable");
}

#[test]
fn test_delete_table_removes_tags() {
    let db = make_db();
    create_table(&db, "TestTable");

    db.tag_resource(TagResourceRequest {
        resource_arn: Some(table_arn("TestTable")),
        tags: vec![tag("env", "prod")],
    })
    .unwrap();

    // Delete the table
    let del_req = serde_json::from_value(serde_json::json!({
        "TableName": "TestTable"
    }))
    .unwrap();
    db.delete_table(del_req).unwrap();

    // Listing tags should fail since table no longer exists
    // Real DynamoDB returns AccessDeniedException for non-existent ARNs
    let err = db
        .list_tags_of_resource(ListTagsOfResourceRequest {
            resource_arn: Some(table_arn("TestTable")),
        })
        .unwrap_err();
    assert!(matches!(
        err,
        dynoxide::errors::DynoxideError::AccessDeniedException(_)
    ));
}
