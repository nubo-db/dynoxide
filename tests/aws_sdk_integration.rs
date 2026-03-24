//! Integration test: start the HTTP server and exercise it via the official AWS SDK.
//!
//! Run with: cargo test --test aws_sdk_integration --features http-server

#![cfg(feature = "http-server")]

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use dynoxide::Database;
use std::net::SocketAddr;

/// Start the dynoxide HTTP server on a random port and return the SDK client + address.
async fn setup() -> (Client, SocketAddr) {
    let db = Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });

    let creds = Credentials::new("fake", "fake", None, None, "test");
    let config = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(creds)
        .build();

    let client = Client::from_conf(config);
    (client, addr)
}

#[tokio::test]
async fn test_create_table_and_crud() {
    let (client, _addr) = setup().await;

    // Create table
    client
        .create_table()
        .table_name("Users")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .expect("CreateTable should succeed");

    // ListTables
    let tables = client
        .list_tables()
        .send()
        .await
        .expect("ListTables should succeed");
    assert!(tables.table_names().contains(&"Users".to_string()));

    // PutItem
    client
        .put_item()
        .table_name("Users")
        .item("pk", AttributeValue::S("user#1".into()))
        .item("sk", AttributeValue::S("profile".into()))
        .item("name", AttributeValue::S("Alice".into()))
        .item("age", AttributeValue::N("30".into()))
        .send()
        .await
        .expect("PutItem should succeed");

    // GetItem
    let get_resp = client
        .get_item()
        .table_name("Users")
        .key("pk", AttributeValue::S("user#1".into()))
        .key("sk", AttributeValue::S("profile".into()))
        .send()
        .await
        .expect("GetItem should succeed");

    let item = get_resp.item().expect("Item should exist");
    assert_eq!(item.get("name").unwrap().as_s().unwrap(), "Alice");
    assert_eq!(item.get("age").unwrap().as_n().unwrap(), "30");

    // UpdateItem
    client
        .update_item()
        .table_name("Users")
        .key("pk", AttributeValue::S("user#1".into()))
        .key("sk", AttributeValue::S("profile".into()))
        .update_expression("SET age = :new_age")
        .expression_attribute_values(":new_age", AttributeValue::N("31".into()))
        .send()
        .await
        .expect("UpdateItem should succeed");

    // Verify update
    let get_resp = client
        .get_item()
        .table_name("Users")
        .key("pk", AttributeValue::S("user#1".into()))
        .key("sk", AttributeValue::S("profile".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_resp.item().unwrap().get("age").unwrap().as_n().unwrap(),
        "31"
    );

    // Query
    let query_resp = client
        .query()
        .table_name("Users")
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", AttributeValue::S("user#1".into()))
        .send()
        .await
        .expect("Query should succeed");
    assert_eq!(query_resp.count(), 1);

    // Scan
    let scan_resp = client
        .scan()
        .table_name("Users")
        .send()
        .await
        .expect("Scan should succeed");
    assert_eq!(scan_resp.count(), 1);

    // DeleteItem
    client
        .delete_item()
        .table_name("Users")
        .key("pk", AttributeValue::S("user#1".into()))
        .key("sk", AttributeValue::S("profile".into()))
        .send()
        .await
        .expect("DeleteItem should succeed");

    // Verify deletion
    let get_resp = client
        .get_item()
        .table_name("Users")
        .key("pk", AttributeValue::S("user#1".into()))
        .key("sk", AttributeValue::S("profile".into()))
        .send()
        .await
        .unwrap();
    assert!(get_resp.item().is_none());

    // DescribeTable
    let describe = client
        .describe_table()
        .table_name("Users")
        .send()
        .await
        .expect("DescribeTable should succeed");
    assert_eq!(describe.table().unwrap().table_name(), Some("Users"));
    assert_eq!(
        describe.table().unwrap().table_arn(),
        Some("arn:aws:dynamodb:dynoxide:000000000000:table/Users")
    );

    // DeleteTable
    client
        .delete_table()
        .table_name("Users")
        .send()
        .await
        .expect("DeleteTable should succeed");

    // Verify table gone
    let tables = client.list_tables().send().await.unwrap();
    assert!(!tables.table_names().contains(&"Users".to_string()));
}

#[tokio::test]
async fn test_batch_operations() {
    let (client, _addr) = setup().await;

    // Create table
    client
        .create_table()
        .table_name("Items")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // BatchWriteItem — put 5 items
    use aws_sdk_dynamodb::types::{PutRequest, WriteRequest};

    let write_requests: Vec<WriteRequest> = (0..5)
        .map(|i| {
            WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .item("id", AttributeValue::S(format!("item#{i}")))
                        .item("data", AttributeValue::S(format!("value_{i}")))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect();

    client
        .batch_write_item()
        .request_items("Items", write_requests)
        .send()
        .await
        .expect("BatchWriteItem should succeed");

    // BatchGetItem — fetch 3 of them
    use aws_sdk_dynamodb::types::KeysAndAttributes;

    let keys_and_attrs = KeysAndAttributes::builder()
        .keys(std::collections::HashMap::from([(
            "id".to_string(),
            AttributeValue::S("item#0".into()),
        )]))
        .keys(std::collections::HashMap::from([(
            "id".to_string(),
            AttributeValue::S("item#2".into()),
        )]))
        .keys(std::collections::HashMap::from([(
            "id".to_string(),
            AttributeValue::S("item#4".into()),
        )]))
        .build()
        .unwrap();

    let batch_get = client
        .batch_get_item()
        .request_items("Items", keys_and_attrs)
        .send()
        .await
        .expect("BatchGetItem should succeed");

    let items = batch_get.responses().unwrap().get("Items").unwrap();
    assert_eq!(items.len(), 3);

    // Scan to verify all 5 items exist
    let scan = client.scan().table_name("Items").send().await.unwrap();
    assert_eq!(scan.count(), 5);
}

#[tokio::test]
async fn test_condition_expressions() {
    let (client, _addr) = setup().await;

    client
        .create_table()
        .table_name("Conditions")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Put with condition (should succeed — item doesn't exist yet)
    client
        .put_item()
        .table_name("Conditions")
        .item("pk", AttributeValue::S("k1".into()))
        .item("val", AttributeValue::N("1".into()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
        .expect("Conditional put should succeed");

    // Put again with same condition — should fail
    let err = client
        .put_item()
        .table_name("Conditions")
        .item("pk", AttributeValue::S("k1".into()))
        .item("val", AttributeValue::N("2".into()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;
    assert!(err.is_err(), "Conditional put should fail (item exists)");

    // Update with condition expression
    client
        .update_item()
        .table_name("Conditions")
        .key("pk", AttributeValue::S("k1".into()))
        .update_expression("SET val = val + :inc")
        .condition_expression("val = :expected")
        .expression_attribute_values(":inc", AttributeValue::N("1".into()))
        .expression_attribute_values(":expected", AttributeValue::N("1".into()))
        .send()
        .await
        .expect("Conditional update should succeed");

    // Verify val is now 2
    let item = client
        .get_item()
        .table_name("Conditions")
        .key("pk", AttributeValue::S("k1".into()))
        .send()
        .await
        .unwrap()
        .item()
        .unwrap()
        .clone();
    assert_eq!(item.get("val").unwrap().as_n().unwrap(), "2");
}
