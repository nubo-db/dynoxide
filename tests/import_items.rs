use dynoxide::actions::create_table::StreamSpecification;
use dynoxide::{AttributeValue, Database, ImportOptions};
use std::collections::HashMap;

fn create_test_db() -> Database {
    Database::memory().unwrap()
}

fn create_table(db: &Database, table_name: &str) {
    use dynoxide::actions::create_table::CreateTableRequest;
    use dynoxide::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};

    let request = CreateTableRequest {
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
                attribute_type: ScalarAttributeType::S,
            },
        ],
        ..Default::default()
    };
    db.create_table(request).unwrap();
}

fn create_table_with_gsi(db: &Database, table_name: &str) {
    use dynoxide::actions::create_table::CreateTableRequest;
    use dynoxide::types::{
        AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
        ProjectionType, ScalarAttributeType,
    };

    let request = CreateTableRequest {
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
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "email".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: "email-index".to_string(),
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
        ..Default::default()
    };
    db.create_table(request).unwrap();
}

fn get_item(
    db: &Database,
    table_name: &str,
    pk: &str,
    sk: &str,
) -> Option<HashMap<String, AttributeValue>> {
    use dynoxide::actions::get_item::GetItemRequest;

    let request = GetItemRequest {
        table_name: table_name.to_string(),
        key: dynoxide::item! {
            "pk" => pk,
            "sk" => sk,
        },
        ..Default::default()
    };
    db.get_item(request).unwrap().item
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn import_basic() {
    let db = create_test_db();
    create_table(&db, "Users");

    let items = vec![
        dynoxide::item! { "pk" => "user#1", "sk" => "PROFILE", "name" => "Alice" },
        dynoxide::item! { "pk" => "user#2", "sk" => "PROFILE", "name" => "Bob" },
    ];

    let result = db
        .import_items("Users", items, ImportOptions::default())
        .unwrap();

    assert_eq!(result.items_imported, 2);
    assert!(result.bytes_imported > 0);

    // Verify items are readable
    let item1 = get_item(&db, "Users", "user#1", "PROFILE");
    assert!(item1.is_some());
    assert_eq!(
        item1.unwrap()["name"],
        AttributeValue::S("Alice".to_string())
    );

    let item2 = get_item(&db, "Users", "user#2", "PROFILE");
    assert!(item2.is_some());
    assert_eq!(item2.unwrap()["name"], AttributeValue::S("Bob".to_string()));
}

#[test]
fn import_empty_vec() {
    let db = create_test_db();
    create_table(&db, "Users");

    let result = db
        .import_items("Users", vec![], ImportOptions::default())
        .unwrap();

    assert_eq!(result.items_imported, 0);
    assert_eq!(result.bytes_imported, 0);
}

#[test]
fn import_nonexistent_table() {
    let db = create_test_db();

    let result = db.import_items("NoSuchTable", vec![], ImportOptions::default());
    assert!(result.is_err());
}

#[test]
fn import_duplicate_keys_last_wins() {
    let db = create_test_db();
    create_table(&db, "Users");

    let items = vec![
        dynoxide::item! { "pk" => "user#1", "sk" => "PROFILE", "name" => "Alice" },
        dynoxide::item! { "pk" => "user#1", "sk" => "PROFILE", "name" => "Bob" },
    ];

    db.import_items("Users", items, ImportOptions::default())
        .unwrap();

    let item = get_item(&db, "Users", "user#1", "PROFILE").unwrap();
    assert_eq!(item["name"], AttributeValue::S("Bob".to_string()));
}

#[test]
fn import_maintains_gsi() {
    let db = create_test_db();
    create_table_with_gsi(&db, "Users");

    let items = vec![dynoxide::item! {
        "pk" => "user#1",
        "sk" => "PROFILE",
        "email" => "alice@example.com",
    }];

    db.import_items("Users", items, ImportOptions::default())
        .unwrap();

    // Query the GSI
    use dynoxide::actions::query::QueryRequest;
    let resp = db
        .query({
            QueryRequest {
                table_name: "Users".to_string(),
                index_name: Some("email-index".to_string()),
                key_condition_expression: Some("email = :email".to_string()),
                expression_attribute_values: Some(
                    dynoxide::item! { ":email" => "alice@example.com" },
                ),
                ..Default::default()
            }
        })
        .unwrap();

    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["pk"], AttributeValue::S("user#1".to_string()));
}

#[test]
fn import_sparse_gsi_skips_items_without_gsi_key() {
    let db = create_test_db();
    create_table_with_gsi(&db, "Users");

    // Item without the "email" attribute (GSI pk)
    let items = vec![dynoxide::item! {
        "pk" => "user#1",
        "sk" => "PROFILE",
    }];

    db.import_items("Users", items, ImportOptions::default())
        .unwrap();

    // Item exists in base table
    let item = get_item(&db, "Users", "user#1", "PROFILE");
    assert!(item.is_some());

    // But NOT in the GSI
    use dynoxide::actions::scan::ScanRequest;
    let resp = db
        .scan({
            ScanRequest {
                table_name: "Users".to_string(),
                index_name: Some("email-index".to_string()),
                ..Default::default()
            }
        })
        .unwrap();
    let gsi_items = resp.items.unwrap_or_default();
    assert_eq!(gsi_items.len(), 0);
}

/// Table with a composite GSI whose sort key is a non-key attribute.
fn create_table_with_composite_gsi(db: &Database, table_name: &str) {
    use dynoxide::actions::create_table::CreateTableRequest;
    use dynoxide::types::{
        AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
        ProjectionType, ScalarAttributeType,
    };

    let request = CreateTableRequest {
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
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sparse_attribute".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: "sparse-index".to_string(),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::HASH,
                },
                KeySchemaElement {
                    attribute_name: "sparse_attribute".to_string(),
                    key_type: KeyType::RANGE,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::ALL),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        ..Default::default()
    };
    db.create_table(request).unwrap();
}

/// Sort-key absence (not just partition-key absence) excludes an item on import.
#[test]
fn import_sparse_gsi_skips_items_without_gsi_sort_key() {
    let db = create_test_db();
    create_table_with_composite_gsi(&db, "Events");

    let items = vec![
        dynoxide::item! { "pk" => "e#1", "sk" => "A" },
        dynoxide::item! { "pk" => "e#2", "sk" => "B", "sparse_attribute" => "x" },
    ];
    db.import_items("Events", items, ImportOptions::default())
        .unwrap();

    use dynoxide::actions::scan::ScanRequest;
    let resp = db
        .scan(ScanRequest {
            table_name: "Events".to_string(),
            index_name: Some("sparse-index".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(resp.items.unwrap_or_default().len(), 1);
}

#[test]
fn import_with_cached_at() {
    let db = create_test_db();
    create_table(&db, "Users");

    let items = vec![dynoxide::item! {
        "pk" => "user#1",
        "sk" => "PROFILE",
    }];

    let opts = ImportOptions {
        set_cached_at: true,
        ..Default::default()
    };
    db.import_items("Users", items, opts).unwrap();

    // Verify cached_at is set by checking LRU items
    let lru = db.get_lru_items("Users", 10).unwrap();
    assert_eq!(lru.len(), 1);
    // pk is stored as a key string (e.g. "S:user#1")
    assert!(lru[0].0.contains("user#1"));
}

#[test]
fn import_without_cached_at_not_in_lru() {
    let db = create_test_db();
    create_table(&db, "Users");

    let items = vec![dynoxide::item! {
        "pk" => "user#1",
        "sk" => "PROFILE",
    }];

    db.import_items("Users", items, ImportOptions::default())
        .unwrap();

    // Without set_cached_at, items should NOT appear in LRU (NULL cached_at)
    let lru = db.get_lru_items("Users", 10).unwrap();
    assert_eq!(lru.len(), 0);
}

#[test]
fn import_missing_partition_key_fails_and_rolls_back() {
    let db = create_test_db();
    create_table(&db, "Users");

    let items = vec![
        dynoxide::item! { "pk" => "user#1", "sk" => "PROFILE", "name" => "Alice" },
        // Missing "pk" attribute -- should cause the whole import to fail
        dynoxide::item! { "sk" => "PROFILE", "name" => "Bob" },
    ];

    let result = db.import_items("Users", items, ImportOptions::default());
    assert!(result.is_err());

    // First item should NOT be persisted (entire transaction rolled back)
    let item = get_item(&db, "Users", "user#1", "PROFILE");
    assert!(item.is_none());
}

#[test]
fn import_missing_sort_key_fails_and_rolls_back() {
    let db = create_test_db();
    create_table(&db, "Users");

    let items = vec![
        dynoxide::item! { "pk" => "user#1", "sk" => "PROFILE", "name" => "Alice" },
        // Missing "sk" attribute
        dynoxide::item! { "pk" => "user#2", "name" => "Bob" },
    ];

    let result = db.import_items("Users", items, ImportOptions::default());
    assert!(result.is_err());

    // First item should NOT be persisted
    let item = get_item(&db, "Users", "user#1", "PROFILE");
    assert!(item.is_none());
}

#[test]
fn import_calculates_item_size() {
    let db = create_test_db();
    create_table(&db, "Users");

    let items = vec![dynoxide::item! {
        "pk" => "user#1",
        "sk" => "PROFILE",
        "name" => "Alice",
    }];

    let result = db
        .import_items("Users", items, ImportOptions::default())
        .unwrap();

    assert_eq!(result.items_imported, 1);
    assert!(result.bytes_imported > 0);
}

#[test]
fn import_large_batch() {
    let db = create_test_db();
    create_table(&db, "Users");

    let items: Vec<_> = (0..500)
        .map(|i| {
            dynoxide::item! {
                "pk" => format!("user#{i}"),
                "sk" => "PROFILE",
                "index" => i as i64,
            }
        })
        .collect();

    let result = db
        .import_items("Users", items, ImportOptions::default())
        .unwrap();

    assert_eq!(result.items_imported, 500);

    // Spot-check a few items
    assert!(get_item(&db, "Users", "user#0", "PROFILE").is_some());
    assert!(get_item(&db, "Users", "user#499", "PROFILE").is_some());
}

/// The one shard DescribeStream lists for `stream_arn`.
fn only_shard(db: &Database, stream_arn: &str) -> dynoxide::actions::describe_stream::Shard {
    use dynoxide::actions::describe_stream::DescribeStreamRequest;

    let mut shards = db
        .describe_stream(DescribeStreamRequest {
            stream_arn: stream_arn.to_string(),
            exclusive_start_shard_id: None,
            limit: None,
        })
        .unwrap()
        .stream_description
        .shards;
    assert_eq!(shards.len(), 1);
    shards.remove(0)
}

/// Import one item into a fresh table streaming `view_type`, and return the
/// record a consumer reads from the shard DescribeStream lists.
fn import_one_and_read_its_record(view_type: &str) -> dynoxide::actions::get_records::Record {
    use dynoxide::actions::create_table::CreateTableRequest;
    use dynoxide::actions::get_records::GetRecordsRequest;
    use dynoxide::actions::get_shard_iterator::GetShardIteratorRequest;
    use dynoxide::actions::list_streams::ListStreamsRequest;
    use dynoxide::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};

    let db = create_test_db();
    db.create_table(CreateTableRequest {
        table_name: "Viewed".to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(view_type.to_string()),
        }),
        ..Default::default()
    })
    .unwrap();

    let opts = ImportOptions {
        record_streams: true,
        ..Default::default()
    };
    let items = vec![dynoxide::item! { "pk" => "a", "name" => "Alice" }];
    db.import_items("Viewed", items, opts).unwrap();

    let stream_arn = db
        .list_streams(ListStreamsRequest {
            table_name: Some("Viewed".to_string()),
            exclusive_start_stream_arn: None,
            limit: None,
        })
        .unwrap()
        .streams
        .remove(0)
        .stream_arn;
    let shard = only_shard(&db, &stream_arn);
    let iterator = db
        .get_shard_iterator(GetShardIteratorRequest {
            stream_arn,
            shard_id: shard.shard_id,
            shard_iterator_type: "TRIM_HORIZON".to_string(),
            sequence_number: None,
        })
        .unwrap()
        .shard_iterator
        .unwrap();
    let mut records = db
        .get_records(GetRecordsRequest {
            shard_iterator: iterator,
            limit: None,
        })
        .unwrap()
        .records;
    assert_eq!(records.len(), 1, "{view_type}");
    records.remove(0)
}

#[test]
fn imported_stream_records_follow_the_tables_view_type() {
    let key = dynoxide::item! { "pk" => "a" };
    let full = dynoxide::item! { "pk" => "a", "name" => "Alice" };

    for (view_type, new_image) in [
        ("KEYS_ONLY", None),
        ("NEW_IMAGE", Some(&full)),
        ("OLD_IMAGE", None),
        ("NEW_AND_OLD_IMAGES", Some(&full)),
    ] {
        let record = import_one_and_read_its_record(view_type);
        assert_eq!(record.event_name, "INSERT", "{view_type}");
        assert_eq!(record.dynamodb.keys, key, "{view_type}");
        assert_eq!(record.dynamodb.new_image.as_ref(), new_image, "{view_type}");
        assert_eq!(record.dynamodb.old_image, None, "{view_type}");
        assert_eq!(record.dynamodb.stream_view_type, view_type);
    }
}

/// Every record on `table`'s one shard, from the start, as a consumer reads it.
fn all_stream_records(db: &Database, table: &str) -> Vec<dynoxide::actions::get_records::Record> {
    use dynoxide::actions::get_records::GetRecordsRequest;
    use dynoxide::actions::get_shard_iterator::GetShardIteratorRequest;
    use dynoxide::actions::list_streams::ListStreamsRequest;

    let stream_arn = db
        .list_streams(ListStreamsRequest {
            table_name: Some(table.to_string()),
            exclusive_start_stream_arn: None,
            limit: None,
        })
        .unwrap()
        .streams
        .remove(0)
        .stream_arn;
    let shard = only_shard(db, &stream_arn);
    let iterator = db
        .get_shard_iterator(GetShardIteratorRequest {
            stream_arn,
            shard_id: shard.shard_id,
            shard_iterator_type: "TRIM_HORIZON".to_string(),
            sequence_number: None,
        })
        .unwrap()
        .shard_iterator
        .unwrap();
    db.get_records(GetRecordsRequest {
        shard_iterator: iterator,
        limit: None,
    })
    .unwrap()
    .records
}

/// An import that replaces an item records a `MODIFY` carrying the item it
/// replaced, as a `PutItem` over the key does, whether the earlier item was
/// already stored or came earlier in the same import. A new key is still an
/// `INSERT`.
#[test]
fn an_import_over_an_existing_key_records_a_modify() {
    use dynoxide::actions::create_table::CreateTableRequest;
    use dynoxide::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};

    let db = create_test_db();
    db.create_table(CreateTableRequest {
        table_name: "Replaced".to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some("NEW_AND_OLD_IMAGES".to_string()),
        }),
        ..Default::default()
    })
    .unwrap();
    let opts = || ImportOptions {
        record_streams: true,
        ..Default::default()
    };

    let a1 = dynoxide::item! { "pk" => "a", "v" => "1" };
    let a2 = dynoxide::item! { "pk" => "a", "v" => "2" };
    let a3 = dynoxide::item! { "pk" => "a", "v" => "3" };
    let b1 = dynoxide::item! { "pk" => "b", "v" => "1" };
    db.import_items("Replaced", vec![a1.clone()], opts())
        .unwrap();
    db.import_items("Replaced", vec![a2.clone(), b1.clone(), a3.clone()], opts())
        .unwrap();

    let records: Vec<_> = all_stream_records(&db, "Replaced")
        .into_iter()
        .map(|r| (r.event_name, r.dynamodb.old_image, r.dynamodb.new_image))
        .collect();
    assert_eq!(
        records,
        vec![
            ("INSERT".to_string(), None, Some(a1.clone())),
            ("MODIFY".to_string(), Some(a1), Some(a2.clone())),
            ("INSERT".to_string(), None, Some(b1)),
            ("MODIFY".to_string(), Some(a2), Some(a3)),
        ]
    );
}

#[test]
fn import_with_record_streams() {
    use dynoxide::actions::create_table::CreateTableRequest;
    use dynoxide::actions::get_records::GetRecordsRequest;
    use dynoxide::actions::get_shard_iterator::GetShardIteratorRequest;
    use dynoxide::actions::list_streams::ListStreamsRequest;
    use dynoxide::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};

    let db = create_test_db();

    // Create table with streams enabled
    let request = CreateTableRequest {
        table_name: "StreamTable".to_string(),
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
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some("NEW_AND_OLD_IMAGES".to_string()),
        }),
        ..Default::default()
    };
    db.create_table(request).unwrap();

    // Import with record_streams enabled
    let items = vec![
        dynoxide::item! { "pk" => "user#1", "sk" => "PROFILE", "name" => "Alice" },
        dynoxide::item! { "pk" => "user#2", "sk" => "PROFILE", "name" => "Bob" },
    ];
    let opts = ImportOptions {
        record_streams: true,
        ..Default::default()
    };
    let result = db.import_items("StreamTable", items, opts).unwrap();
    assert_eq!(result.items_imported, 2);

    // Verify stream records were created
    let streams_resp = db
        .list_streams(ListStreamsRequest {
            table_name: Some("StreamTable".to_string()),
            exclusive_start_stream_arn: None,
            limit: None,
        })
        .unwrap();
    assert_eq!(streams_resp.streams.len(), 1);
    let stream_arn = &streams_resp.streams[0].stream_arn;

    // Follow the shard DescribeStream lists, as a stream consumer does.
    let shard = only_shard(&db, stream_arn);
    assert!(
        shard
            .sequence_number_range
            .starting_sequence_number
            .is_some(),
        "the listed shard holds the imported records"
    );

    let iter_resp = db
        .get_shard_iterator(GetShardIteratorRequest {
            stream_arn: stream_arn.clone(),
            shard_id: shard.shard_id,
            shard_iterator_type: "TRIM_HORIZON".to_string(),
            sequence_number: None,
        })
        .unwrap();

    let records_resp = db
        .get_records(GetRecordsRequest {
            shard_iterator: iter_resp.shard_iterator.unwrap(),
            limit: None,
        })
        .unwrap();

    // Should have 2 INSERT records (one per imported item)
    assert_eq!(records_resp.records.len(), 2);
    assert_eq!(records_resp.records[0].event_name, "INSERT");
    assert_eq!(records_resp.records[1].event_name, "INSERT");
}
