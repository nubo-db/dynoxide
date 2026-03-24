pub mod memory_measurement;

use dynoxide::Database;
use dynoxide::actions::batch_write_item::{BatchWriteItemRequest, PutRequest, WriteRequest};
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::types::*;
use std::collections::HashMap;

/// HTTP-mode helpers (AWS SDK types).
pub mod http {
    use aws_sdk_dynamodb::Client;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, GlobalSecondaryIndex, KeySchemaElement, KeyType,
        Projection, ProjectionType, ProvisionedThroughput, ScalarAttributeType,
    };
    use std::collections::HashMap;

    pub const TABLE_NAME: &str = "BenchmarkTable";
    pub const GSI_NAME: &str = "EmailIndex";

    pub fn make_sdk_client(endpoint_url: &str) -> Client {
        let creds = Credentials::new("fake", "fake", None, None, "bench");
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint_url)
            .credentials_provider(creds)
            .build();
        Client::from_conf(config)
    }

    /// Generate a medium-sized item matching lib.rs::generate_item(Medium) schema.
    /// 10 attributes: pk, sk, name, email, age, address, tags, scores, active, metadata.
    pub fn make_sdk_item(index: usize) -> HashMap<String, AttributeValue> {
        let pk = format!("user#{:06}", index / 100);
        let sk = format!("{index}");
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk));
        item.insert("sk".to_string(), AttributeValue::N(sk));
        item.insert(
            "name".to_string(),
            AttributeValue::S(format!("User {index}")),
        );
        item.insert(
            "email".to_string(),
            AttributeValue::S(format!("user{index}@example.com")),
        );
        item.insert(
            "age".to_string(),
            AttributeValue::N(format!("{}", 20 + (index % 60))),
        );
        item.insert(
            "active".to_string(),
            AttributeValue::Bool(!index.is_multiple_of(5)),
        );
        item.insert(
            "scores".to_string(),
            AttributeValue::L(vec![
                AttributeValue::N(format!("{}", 50 + (index % 50))),
                AttributeValue::N(format!("{}", 60 + (index % 40))),
            ]),
        );
        item.insert(
            "address".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "street".to_string(),
                    AttributeValue::S(format!("{} Main St", 100 + index)),
                ),
                (
                    "city".to_string(),
                    AttributeValue::S("Springfield".to_string()),
                ),
            ])),
        );
        item.insert(
            "tags".to_string(),
            AttributeValue::Ss(vec!["active".to_string(), format!("group{}", index % 10)]),
        );
        item.insert(
            "metadata".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "created".to_string(),
                    AttributeValue::S("2026-01-15".to_string()),
                ),
                (
                    "version".to_string(),
                    AttributeValue::N(format!("{}", index % 10)),
                ),
            ])),
        );
        item
    }

    pub fn make_sdk_key(index: usize) -> HashMap<String, AttributeValue> {
        let pk = format!("user#{:06}", index / 100);
        let sk = format!("{index}");
        let mut key = HashMap::new();
        key.insert("pk".to_string(), AttributeValue::S(pk));
        key.insert("sk".to_string(), AttributeValue::N(sk));
        key
    }

    pub async fn create_sdk_table(client: &Client, table_name: &str) {
        client
            .create_table()
            .table_name(table_name)
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
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("email")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(100)
                    .write_capacity_units(100)
                    .build()
                    .unwrap(),
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name(GSI_NAME)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("email")
                            .key_type(KeyType::Hash)
                            .build()
                            .unwrap(),
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .provisioned_throughput(
                        ProvisionedThroughput::builder()
                            .read_capacity_units(100)
                            .write_capacity_units(100)
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
    }

    pub async fn warmup_jvm(client: &Client) {
        client
            .create_table()
            .table_name("_warmup")
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
            .provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(100)
                    .write_capacity_units(100)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        for i in 0..500 {
            client
                .put_item()
                .table_name("_warmup")
                .item("pk", AttributeValue::S(format!("w{i}")))
                .item("data", AttributeValue::S(format!("warmup-{i}")))
                .send()
                .await
                .unwrap();
        }

        for i in 0..100 {
            client
                .get_item()
                .table_name("_warmup")
                .key("pk", AttributeValue::S(format!("w{i}")))
                .send()
                .await
                .unwrap();
        }

        client
            .delete_table()
            .table_name("_warmup")
            .send()
            .await
            .unwrap();
    }
}

/// Statistics helpers for latency analysis.
pub mod stats {
    pub fn mean(values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.iter().sum::<f64>() / values.len() as f64
    }

    pub fn stddev(values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        let m = mean(values);
        let variance =
            values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
        variance.sqrt()
    }

    pub fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
}

/// Item size categories for benchmark workloads.
#[derive(Debug, Clone, Copy)]
pub enum ItemSize {
    /// ~200 bytes, 3 attributes
    Small,
    /// ~1KB, 10 attributes with nested maps/lists
    Medium,
    /// ~50KB, binary data + many attributes
    Large,
}

/// Standard table name used across benchmarks.
pub const BENCH_TABLE: &str = "BenchmarkTable";

/// GSI name used across benchmarks.
pub const BENCH_GSI: &str = "EmailIndex";

/// Generate a single item of the given size for a given index.
pub fn generate_item(index: usize, size: ItemSize) -> HashMap<String, AttributeValue> {
    let pk = format!("user#{:06}", index / 100);
    let sk = format!("{}", index);

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk));
    item.insert("sk".to_string(), AttributeValue::N(sk));

    match size {
        ItemSize::Small => {
            item.insert(
                "name".to_string(),
                AttributeValue::S(format!("User {index}")),
            );
        }
        ItemSize::Medium => {
            item.insert(
                "name".to_string(),
                AttributeValue::S(format!("User {index}")),
            );
            item.insert(
                "email".to_string(),
                AttributeValue::S(format!("user{index}@example.com")),
            );
            item.insert(
                "age".to_string(),
                AttributeValue::N(format!("{}", 20 + (index % 60))),
            );
            item.insert(
                "address".to_string(),
                AttributeValue::M({
                    let mut m = HashMap::new();
                    m.insert(
                        "street".to_string(),
                        AttributeValue::S(format!("{} Main St", 100 + index)),
                    );
                    m.insert(
                        "city".to_string(),
                        AttributeValue::S("Springfield".to_string()),
                    );
                    m
                }),
            );
            item.insert(
                "tags".to_string(),
                AttributeValue::SS(vec!["active".to_string(), format!("group{}", index % 10)]),
            );
            item.insert(
                "scores".to_string(),
                AttributeValue::L(vec![
                    AttributeValue::N(format!("{}", 50 + (index % 50))),
                    AttributeValue::N(format!("{}", 60 + (index % 40))),
                ]),
            );
            item.insert(
                "active".to_string(),
                AttributeValue::BOOL(!index.is_multiple_of(5)),
            );
            item.insert(
                "metadata".to_string(),
                AttributeValue::M({
                    let mut m = HashMap::new();
                    m.insert(
                        "created".to_string(),
                        AttributeValue::S("2026-01-15".to_string()),
                    );
                    m.insert(
                        "version".to_string(),
                        AttributeValue::N(format!("{}", index % 10)),
                    );
                    m
                }),
            );
        }
        ItemSize::Large => {
            // Start with medium attributes
            item.insert(
                "name".to_string(),
                AttributeValue::S(format!("User {index}")),
            );
            item.insert(
                "email".to_string(),
                AttributeValue::S(format!("user{index}@example.com")),
            );
            item.insert(
                "age".to_string(),
                AttributeValue::N(format!("{}", 20 + (index % 60))),
            );
            item.insert(
                "address".to_string(),
                AttributeValue::M({
                    let mut m = HashMap::new();
                    m.insert(
                        "street".to_string(),
                        AttributeValue::S(format!("{} Main St", 100 + index)),
                    );
                    m.insert(
                        "city".to_string(),
                        AttributeValue::S("Springfield".to_string()),
                    );
                    m
                }),
            );
            item.insert("active".to_string(), AttributeValue::BOOL(true));
            // ~50KB binary payload
            let payload = vec![0x42u8; 50_000];
            item.insert("payload".to_string(), AttributeValue::B(payload));
        }
    }

    item
}

/// Generate a vector of items for benchmark workloads.
pub fn generate_items(count: usize, size: ItemSize) -> Vec<HashMap<String, AttributeValue>> {
    (0..count).map(|i| generate_item(i, size)).collect()
}

/// Generate items with mixed sizes: 80% medium, 10% small, 10% large.
pub fn generate_mixed_items(count: usize) -> Vec<HashMap<String, AttributeValue>> {
    (0..count)
        .map(|i| {
            let size = match i % 10 {
                0 => ItemSize::Small,
                9 => ItemSize::Large,
                _ => ItemSize::Medium,
            };
            generate_item(i, size)
        })
        .collect()
}

/// Create a standard benchmark table request: pk (S) HASH, sk (N) RANGE, GSI on email (S).
pub fn create_table_request(table_name: &str) -> CreateTableRequest {
    CreateTableRequest {
        table_name: table_name.to_string(),
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
                attribute_type: ScalarAttributeType::N,
            },
            AttributeDefinition {
                attribute_name: "email".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: BENCH_GSI.to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "email".to_string(),
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
    }
}

/// Set up a database pre-populated with items for benchmarking.
/// Creates the standard benchmark table and loads `item_count` items of `item_size`.
pub fn setup_database(item_count: usize, item_size: ItemSize) -> Database {
    let db = Database::memory().unwrap();
    db.create_table(create_table_request(BENCH_TABLE)).unwrap();

    let items = generate_items(item_count, item_size);

    // Load in batches of 25 (DynamoDB batch limit)
    for chunk in items.chunks(25) {
        let write_requests: Vec<WriteRequest> = chunk
            .iter()
            .map(|item| WriteRequest {
                put_request: Some(PutRequest { item: item.clone() }),
                delete_request: None,
            })
            .collect();

        let mut request_items = HashMap::new();
        request_items.insert(BENCH_TABLE.to_string(), write_requests);

        db.batch_write_item(BatchWriteItemRequest {
            request_items,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .unwrap();
    }

    db
}

/// Extract pk and sk from an item to form a key map.
pub fn make_key(item: &HashMap<String, AttributeValue>) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    if let Some(pk) = item.get("pk") {
        key.insert("pk".to_string(), pk.clone());
    }
    if let Some(sk) = item.get("sk") {
        key.insert("sk".to_string(), sk.clone());
    }
    key
}
