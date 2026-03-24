//! DynamoDB Streams integration tests.

use dynoxide::Database;
use dynoxide::types::AttributeValue;
use serde_json::json;

fn create_stream_table(db: &Database, name: &str, view_type: &str) {
    let req: serde_json::Value = json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "StreamSpecification": {
            "StreamEnabled": true,
            "StreamViewType": view_type
        }
    });
    let create_req = serde_json::from_value(req).unwrap();
    db.create_table(create_req).unwrap();
}

fn create_table_no_stream(db: &Database, name: &str) {
    let req: serde_json::Value = json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    });
    let create_req = serde_json::from_value(req).unwrap();
    db.create_table(create_req).unwrap();
}

fn put_item(db: &Database, table: &str, pk: &str, val: &str) {
    let req: serde_json::Value = json!({
        "TableName": table,
        "Item": {"pk": {"S": pk}, "val": {"S": val}}
    });
    let put_req = serde_json::from_value(req).unwrap();
    db.put_item(put_req).unwrap();
}

fn get_stream_arn(db: &Database, table: &str) -> String {
    let req: serde_json::Value = json!({"TableName": table});
    let list_req = serde_json::from_value(req).unwrap();
    let resp = db.list_streams(list_req).unwrap();
    resp.streams[0].stream_arn.clone()
}

fn get_trim_horizon_iterator(db: &Database, stream_arn: &str, shard_id: &str) -> String {
    let req: serde_json::Value = json!({
        "StreamArn": stream_arn,
        "ShardId": shard_id,
        "ShardIteratorType": "TRIM_HORIZON"
    });
    let iter_req = serde_json::from_value(req).unwrap();
    let resp = db.get_shard_iterator(iter_req).unwrap();
    resp.shard_iterator.unwrap()
}

fn get_records(
    db: &Database,
    iterator: &str,
) -> dynoxide::actions::get_records::GetRecordsResponse {
    let req: serde_json::Value = json!({"ShardIterator": iterator});
    let rec_req = serde_json::from_value(req).unwrap();
    db.get_records(rec_req).unwrap()
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[test]
fn test_stream_enabled_on_create() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "StreamTable", "NEW_AND_OLD_IMAGES");

    let req: serde_json::Value = json!({});
    let list_req = serde_json::from_value(req).unwrap();
    let resp = db.list_streams(list_req).unwrap();

    assert_eq!(resp.streams.len(), 1);
    assert_eq!(resp.streams[0].table_name, "StreamTable");
    assert!(resp.streams[0].stream_arn.contains("StreamTable"));
}

#[test]
fn test_put_item_generates_insert_record() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_AND_OLD_IMAGES");
    put_item(&db, "Table1", "key1", "value1");

    let arn = get_stream_arn(&db, "Table1");

    // Describe stream to get shard
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    assert_eq!(resp.records.len(), 1);
    assert_eq!(resp.records[0].event_name, "INSERT");
    assert!(resp.records[0].dynamodb.new_image.is_some());
    assert!(resp.records[0].dynamodb.old_image.is_none());

    let new_image = resp.records[0].dynamodb.new_image.as_ref().unwrap();
    assert!(matches!(new_image.get("val"), Some(AttributeValue::S(s)) if s == "value1"));
}

#[test]
fn test_put_item_replace_generates_modify_record() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_AND_OLD_IMAGES");
    put_item(&db, "Table1", "key1", "old_value");
    put_item(&db, "Table1", "key1", "new_value");

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    assert_eq!(resp.records.len(), 2);
    assert_eq!(resp.records[0].event_name, "INSERT");
    assert_eq!(resp.records[1].event_name, "MODIFY");

    let old_image = resp.records[1].dynamodb.old_image.as_ref().unwrap();
    assert!(matches!(old_image.get("val"), Some(AttributeValue::S(s)) if s == "old_value"));
    let new_image = resp.records[1].dynamodb.new_image.as_ref().unwrap();
    assert!(matches!(new_image.get("val"), Some(AttributeValue::S(s)) if s == "new_value"));
}

#[test]
fn test_update_item_generates_modify_record() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_AND_OLD_IMAGES");
    put_item(&db, "Table1", "key1", "before");

    // Update the item
    let req: serde_json::Value = json!({
        "TableName": "Table1",
        "Key": {"pk": {"S": "key1"}},
        "UpdateExpression": "SET val = :v",
        "ExpressionAttributeValues": {":v": {"S": "after"}}
    });
    let update_req = serde_json::from_value(req).unwrap();
    db.update_item(update_req).unwrap();

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    // INSERT from put_item + MODIFY from update_item
    assert_eq!(resp.records.len(), 2);
    assert_eq!(resp.records[1].event_name, "MODIFY");
}

#[test]
fn test_delete_item_generates_remove_record() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_AND_OLD_IMAGES");
    put_item(&db, "Table1", "key1", "doomed");

    let req: serde_json::Value = json!({
        "TableName": "Table1",
        "Key": {"pk": {"S": "key1"}}
    });
    let del_req = serde_json::from_value(req).unwrap();
    db.delete_item(del_req).unwrap();

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    assert_eq!(resp.records.len(), 2);
    assert_eq!(resp.records[1].event_name, "REMOVE");
    assert!(resp.records[1].dynamodb.old_image.is_some());
    assert!(resp.records[1].dynamodb.new_image.is_none());
}

#[test]
fn test_keys_only_view_type() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "KEYS_ONLY");
    put_item(&db, "Table1", "key1", "value1");

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    assert_eq!(resp.records.len(), 1);
    // KEYS_ONLY: no new_image or old_image
    assert!(resp.records[0].dynamodb.new_image.is_none());
    assert!(resp.records[0].dynamodb.old_image.is_none());
    // But keys are always present
    assert!(resp.records[0].dynamodb.keys.contains_key("pk"));
}

#[test]
fn test_new_image_view_type() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_IMAGE");
    put_item(&db, "Table1", "key1", "value1");

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    assert_eq!(resp.records.len(), 1);
    assert!(resp.records[0].dynamodb.new_image.is_some());
    assert!(resp.records[0].dynamodb.old_image.is_none());
}

#[test]
fn test_old_image_view_type() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "OLD_IMAGE");
    put_item(&db, "Table1", "key1", "old_val");
    put_item(&db, "Table1", "key1", "new_val");

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    // INSERT has no old image, MODIFY has old image
    assert_eq!(resp.records.len(), 2);
    assert!(resp.records[0].dynamodb.old_image.is_none()); // INSERT has no old
    assert!(resp.records[0].dynamodb.new_image.is_none()); // OLD_IMAGE doesn't include new
    assert!(resp.records[1].dynamodb.old_image.is_some()); // MODIFY has old
    assert!(resp.records[1].dynamodb.new_image.is_none()); // OLD_IMAGE doesn't include new
}

#[test]
fn test_list_streams_returns_only_stream_enabled() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "StreamTable", "NEW_IMAGE");
    create_table_no_stream(&db, "NoStreamTable");

    let req: serde_json::Value = json!({});
    let list_req = serde_json::from_value(req).unwrap();
    let resp = db.list_streams(list_req).unwrap();

    assert_eq!(resp.streams.len(), 1);
    assert_eq!(resp.streams[0].table_name, "StreamTable");
}

#[test]
fn test_list_streams_filter_by_table() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_IMAGE");
    create_stream_table(&db, "Table2", "NEW_IMAGE");

    let req: serde_json::Value = json!({"TableName": "Table1"});
    let list_req = serde_json::from_value(req).unwrap();
    let resp = db.list_streams(list_req).unwrap();

    assert_eq!(resp.streams.len(), 1);
    assert_eq!(resp.streams[0].table_name, "Table1");
}

#[test]
fn test_describe_stream_returns_correct_metadata() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_AND_OLD_IMAGES");

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();

    assert_eq!(desc.stream_description.table_name, "Table1");
    assert_eq!(desc.stream_description.stream_status, "ENABLED");
    assert_eq!(
        desc.stream_description.stream_view_type,
        "NEW_AND_OLD_IMAGES"
    );
    assert_eq!(desc.stream_description.shards.len(), 1);
}

#[test]
fn test_get_shard_iterator_trim_horizon() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_IMAGE");
    put_item(&db, "Table1", "a", "1");
    put_item(&db, "Table1", "b", "2");

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    // TRIM_HORIZON reads from beginning
    assert_eq!(resp.records.len(), 2);
}

#[test]
fn test_get_shard_iterator_latest() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_IMAGE");
    put_item(&db, "Table1", "a", "1");

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    // Get LATEST iterator (after existing records)
    let req: serde_json::Value = json!({
        "StreamArn": &arn,
        "ShardId": shard_id,
        "ShardIteratorType": "LATEST"
    });
    let iter_req = serde_json::from_value(req).unwrap();
    let iter = db
        .get_shard_iterator(iter_req)
        .unwrap()
        .shard_iterator
        .unwrap();

    // No new records since LATEST
    let resp = get_records(&db, &iter);
    assert_eq!(resp.records.len(), 0);

    // Now add a new item
    put_item(&db, "Table1", "b", "2");

    // Use the next iterator to read new records
    let resp2 = get_records(&db, &resp.next_shard_iterator.as_ref().unwrap());
    assert_eq!(resp2.records.len(), 1);
    assert_eq!(resp2.records[0].event_name, "INSERT");
}

#[test]
fn test_get_records_pagination() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_IMAGE");

    for i in 0..5 {
        put_item(&db, "Table1", &format!("key{i}"), &format!("val{i}"));
    }

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);

    // Read with limit of 2
    let req: serde_json::Value = json!({"ShardIterator": &iterator, "Limit": 2});
    let rec_req = serde_json::from_value(req).unwrap();
    let resp = db.get_records(rec_req).unwrap();
    assert_eq!(resp.records.len(), 2);

    // Use NextShardIterator to get more
    let next = resp.next_shard_iterator.unwrap();
    let req: serde_json::Value = json!({"ShardIterator": &next, "Limit": 2});
    let rec_req = serde_json::from_value(req).unwrap();
    let resp2 = db.get_records(rec_req).unwrap();
    assert_eq!(resp2.records.len(), 2);

    // Get remaining
    let next2 = resp2.next_shard_iterator.unwrap();
    let resp3 = get_records(&db, &next2);
    assert_eq!(resp3.records.len(), 1);
}

#[test]
fn test_batch_write_generates_stream_records() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_IMAGE");

    let req: serde_json::Value = json!({
        "RequestItems": {
            "Table1": [
                {"PutRequest": {"Item": {"pk": {"S": "a"}, "val": {"S": "1"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "b"}, "val": {"S": "2"}}}}
            ]
        }
    });
    let batch_req = serde_json::from_value(req).unwrap();
    db.batch_write_item(batch_req).unwrap();

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    assert_eq!(resp.records.len(), 2);
    assert!(resp.records.iter().all(|r| r.event_name == "INSERT"));
}

#[test]
fn test_transact_write_generates_stream_records() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_AND_OLD_IMAGES");
    put_item(&db, "Table1", "existing", "old_val");

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "new_item"}, "val": {"S": "created"}}}},
            {"Delete": {"TableName": "Table1", "Key": {"pk": {"S": "existing"}}}}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    db.transact_write_items(transact_req).unwrap();

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    // 1 INSERT (initial put_item) + 1 INSERT (transact put) + 1 REMOVE (transact delete)
    assert_eq!(resp.records.len(), 3);
    assert_eq!(resp.records[0].event_name, "INSERT"); // initial put
    assert_eq!(resp.records[1].event_name, "INSERT"); // transact put
    assert_eq!(resp.records[2].event_name, "REMOVE"); // transact delete
}

#[test]
fn test_no_stream_records_when_disabled() {
    let db = Database::memory().unwrap();
    create_table_no_stream(&db, "Table1");
    put_item(&db, "Table1", "a", "1");

    // No streams should be listed
    let req: serde_json::Value = json!({});
    let list_req = serde_json::from_value(req).unwrap();
    let resp = db.list_streams(list_req).unwrap();
    assert_eq!(resp.streams.len(), 0);
}

#[test]
fn test_records_ordered_by_sequence_number() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Table1", "NEW_IMAGE");

    for i in 0..10 {
        put_item(&db, "Table1", &format!("key{i}"), &format!("val{i}"));
    }

    let arn = get_stream_arn(&db, "Table1");
    let req: serde_json::Value = json!({"StreamArn": &arn});
    let desc_req = serde_json::from_value(req).unwrap();
    let desc = db.describe_stream(desc_req).unwrap();
    let shard_id = &desc.stream_description.shards[0].shard_id;

    let iterator = get_trim_horizon_iterator(&db, &arn, shard_id);
    let resp = get_records(&db, &iterator);

    assert_eq!(resp.records.len(), 10);

    // Verify monotonically increasing sequence numbers
    let mut prev_seq: i64 = 0;
    for record in &resp.records {
        let seq: i64 = record.dynamodb.sequence_number.parse().unwrap();
        assert!(
            seq > prev_seq,
            "sequence numbers should be monotonically increasing"
        );
        prev_seq = seq;
    }
}

#[test]
fn describe_table_includes_stream_specification() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "StreamTable", "NEW_AND_OLD_IMAGES");

    let req: serde_json::Value = json!({"TableName": "StreamTable"});
    let describe_req = serde_json::from_value(req).unwrap();
    let resp = db.describe_table(describe_req).unwrap();

    // Serialise the response to JSON so we can inspect the StreamSpecification field
    let resp_json = serde_json::to_value(&resp).unwrap();
    let table = &resp_json["Table"];

    let stream_spec = &table["StreamSpecification"];
    assert!(
        !stream_spec.is_null(),
        "StreamSpecification should be present for a stream-enabled table"
    );
    assert_eq!(stream_spec["StreamEnabled"], true);
    assert_eq!(stream_spec["StreamViewType"], "NEW_AND_OLD_IMAGES");
}

#[test]
fn describe_table_omits_stream_specification_when_disabled() {
    let db = Database::memory().unwrap();
    create_table_no_stream(&db, "NoStreamTable");

    let req: serde_json::Value = json!({"TableName": "NoStreamTable"});
    let describe_req = serde_json::from_value(req).unwrap();
    let resp = db.describe_table(describe_req).unwrap();

    let resp_json = serde_json::to_value(&resp).unwrap();
    let table = &resp_json["Table"];

    assert!(
        table.get("StreamSpecification").is_none() || table["StreamSpecification"].is_null(),
        "StreamSpecification should be absent for a non-stream table"
    );
}
