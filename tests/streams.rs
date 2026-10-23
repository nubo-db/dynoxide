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
    let resp2 = get_records(&db, resp.next_shard_iterator.as_ref().unwrap());
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

    // Each transact record carries its own eventID, shaped as DynamoDB's.
    for record in &resp.records[1..] {
        assert_aws_event_id(&record.event_id);
    }
    assert_ne!(resp.records[1].event_id, resp.records[2].event_id);
}

fn assert_aws_event_id(id: &str) {
    assert_eq!(id.len(), 32, "eventID {id} should be 32 hex digits");
    assert!(
        id.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')),
        "eventID {id} should be lowercase hex"
    );
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
    let mut prev_seq: u128 = 0;
    for record in &resp.records {
        let seq: u128 = record.dynamodb.sequence_number.parse().unwrap();
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

// ------------------------------------------------------------------
// eventID and SequenceNumber identity
// ------------------------------------------------------------------

fn shard_iterator(db: &Database, table: &str, kind: &str, seq: Option<&str>) -> String {
    let arn = get_stream_arn(db, table);
    let mut req = json!({
        "StreamArn": &arn,
        "ShardId": format!("shardId-00000001-{table}"),
        "ShardIteratorType": kind
    });
    if let Some(seq) = seq {
        req["SequenceNumber"] = json!(seq);
    }
    let iter_req = serde_json::from_value(req).unwrap();
    db.get_shard_iterator(iter_req)
        .unwrap()
        .shard_iterator
        .unwrap()
}

fn all_records(db: &Database, table: &str) -> Vec<dynoxide::actions::get_records::Record> {
    get_records(db, &shard_iterator(db, table, "TRIM_HORIZON", None)).records
}

fn event_ids(db: &Database, table: &str) -> Vec<String> {
    all_records(db, table)
        .into_iter()
        .map(|r| r.event_id)
        .collect()
}

fn sequence_numbers(db: &Database, table: &str) -> Vec<String> {
    all_records(db, table)
        .into_iter()
        .map(|r| r.dynamodb.sequence_number)
        .collect()
}

fn delete_item(db: &Database, table: &str, pk: &str) {
    let req = json!({"TableName": table, "Key": {"pk": {"S": pk}}});
    db.delete_item(serde_json::from_value(req).unwrap())
        .unwrap();
}

#[test]
fn event_ids_are_unique_and_aws_shaped() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Evt", "NEW_AND_OLD_IMAGES");
    for i in 0..10 {
        put_item(&db, "Evt", &format!("k{i}"), "a");
        put_item(&db, "Evt", &format!("k{i}"), "b");
        delete_item(&db, "Evt", &format!("k{i}"));
    }

    let ids = event_ids(&db, "Evt");
    assert_eq!(ids.len(), 30);
    for id in &ids {
        assert_eq!(id.len(), 32, "eventID {id} should be 32 hex digits");
        assert!(
            id.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')),
            "eventID {id} should be lowercase hex"
        );
    }
    let distinct: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(distinct.len(), ids.len(), "eventIDs should be unique");
}

#[test]
fn rereading_a_record_returns_the_same_event_id() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Evt", "NEW_IMAGE");
    for i in 0..3 {
        put_item(&db, "Evt", &format!("k{i}"), "v");
    }
    let first = event_ids(&db, "Evt");
    assert_eq!(first.len(), 3);
    assert_eq!(event_ids(&db, "Evt"), first);

    // A record read at its own sequence number carries the same id.
    let seq = sequence_numbers(&db, "Evt")[1].clone();
    let at = get_records(
        &db,
        &shard_iterator(&db, "Evt", "AT_SEQUENCE_NUMBER", Some(&seq)),
    );
    assert_eq!(at.records[0].event_id, first[1]);
}

#[test]
fn a_reopened_database_returns_the_same_event_ids() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("streams.db");
    let path = path.to_str().unwrap();
    let (ids, seqs) = {
        let db = Database::new(path).unwrap();
        create_stream_table(&db, "Evt", "NEW_IMAGE");
        put_item(&db, "Evt", "a", "1");
        put_item(&db, "Evt", "b", "2");
        (event_ids(&db, "Evt"), sequence_numbers(&db, "Evt"))
    };
    let db = Database::new(path).unwrap();
    assert_eq!(event_ids(&db, "Evt"), ids);
    assert_eq!(sequence_numbers(&db, "Evt"), seqs);
}

#[test]
fn a_fresh_database_reuses_no_event_id_or_sequence_number() {
    let session = || {
        let db = Database::memory().unwrap();
        create_stream_table(&db, "Evt", "NEW_IMAGE");
        for i in 0..3 {
            put_item(&db, "Evt", &format!("k{i}"), "v");
        }
        (event_ids(&db, "Evt"), sequence_numbers(&db, "Evt"))
    };
    let (ids1, seqs1) = session();
    let (ids2, seqs2) = session();

    assert!(
        ids2.iter().all(|id| !ids1.contains(id)),
        "a new session reissued an eventID: {ids1:?} then {ids2:?}"
    );
    let last1: u128 = seqs1.last().unwrap().parse().unwrap();
    let first2: u128 = seqs2.first().unwrap().parse().unwrap();
    assert!(
        first2 > last1,
        "a new session's sequence numbers should follow the last one's: {seqs1:?} then {seqs2:?}"
    );
}

#[test]
fn sequence_numbers_are_aws_shaped_and_drive_every_iterator_type() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Evt", "NEW_IMAGE");
    for i in 0..5 {
        put_item(&db, "Evt", &format!("k{i}"), "v");
    }

    let seqs = sequence_numbers(&db, "Evt");
    assert_eq!(seqs.len(), 5);
    for seq in &seqs {
        assert!(
            seq.len() >= 21 && seq.len() <= 40 && seq.chars().all(|c| c.is_ascii_digit()),
            "SequenceNumber {seq} should be 21 to 40 digits"
        );
    }
    let mut sorted = seqs.clone();
    sorted.sort();
    assert_eq!(sorted, seqs, "string order should match write order");

    let keys = |resp: dynoxide::actions::get_records::GetRecordsResponse| -> Vec<String> {
        resp.records
            .iter()
            .map(|r| match r.dynamodb.keys.get("pk") {
                Some(AttributeValue::S(s)) => s.clone(),
                other => panic!("unexpected key {other:?}"),
            })
            .collect()
    };
    let at = shard_iterator(&db, "Evt", "AT_SEQUENCE_NUMBER", Some(&seqs[2]));
    assert_eq!(keys(get_records(&db, &at)), ["k2", "k3", "k4"]);
    let after = shard_iterator(&db, "Evt", "AFTER_SEQUENCE_NUMBER", Some(&seqs[2]));
    assert_eq!(keys(get_records(&db, &after)), ["k3", "k4"]);
    let horizon = shard_iterator(&db, "Evt", "TRIM_HORIZON", None);
    assert_eq!(keys(get_records(&db, &horizon)).len(), 5);

    let latest = shard_iterator(&db, "Evt", "LATEST", None);
    let resp = get_records(&db, &latest);
    assert!(resp.records.is_empty());
    put_item(&db, "Evt", "k5", "v");
    let next = resp.next_shard_iterator.unwrap();
    assert_eq!(keys(get_records(&db, &next)), ["k5"]);

    // A NextShardIterator resumes after the page it ends.
    let paged = get_records(&db, &after);
    let resume = paged.next_shard_iterator.clone().unwrap();
    assert_eq!(keys(paged), ["k3", "k4", "k5"]);
    assert!(get_records(&db, &resume).records.is_empty());

    let arn = get_stream_arn(&db, "Evt");
    let desc = db
        .describe_stream(serde_json::from_value(json!({"StreamArn": &arn})).unwrap())
        .unwrap();
    let range = &desc.stream_description.shards[0].sequence_number_range;
    let seqs = sequence_numbers(&db, "Evt");
    assert_eq!(range.starting_sequence_number.as_ref(), seqs.first());
    assert_eq!(range.ending_sequence_number.as_ref(), seqs.last());
}

fn record_keys(resp: &dynoxide::actions::get_records::GetRecordsResponse) -> Vec<String> {
    resp.records
        .iter()
        .map(|r| match r.dynamodb.keys.get("pk") {
            Some(AttributeValue::S(s)) => s.clone(),
            other => panic!("unexpected key {other:?}"),
        })
        .collect()
}

/// A database file whose two stream records carry the short sequence numbers
/// 1 and 2 and predate the `event_id` column, as an earlier build wrote them.
fn write_legacy_stream_file(path: &str) {
    {
        let db = Database::new(path).unwrap();
        create_stream_table(&db, "Evt", "NEW_IMAGE");
        put_item(&db, "Evt", "k0", "v");
        put_item(&db, "Evt", "k1", "v");
    }
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(
        "UPDATE _stream_records SET sequence_number = CASE
             WHEN json_extract(keys_json, '$.pk.S') = 'k0' THEN '1' ELSE '2' END;
         ALTER TABLE _stream_records DROP COLUMN event_id;",
    )
    .unwrap();
}

#[test]
fn records_from_an_earlier_build_keep_their_ids_and_order_beside_new_ones() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    let path = path.to_str().unwrap();
    write_legacy_stream_file(path);

    let db = Database::new(path).unwrap();
    put_item(&db, "Evt", "k2", "v");
    put_item(&db, "Evt", "k3", "v");

    // Old records keep the eventID they always reported; new ones are random.
    let records = all_records(&db, "Evt");
    let ids: Vec<&str> = records.iter().map(|r| r.event_id.as_str()).collect();
    assert_eq!(&ids[..2], ["1", "2"]);
    for id in &ids[2..] {
        assert_aws_event_id(id);
    }

    // Write order holds across both kinds, as numbers and as strings.
    let horizon = get_records(&db, &shard_iterator(&db, "Evt", "TRIM_HORIZON", None));
    assert_eq!(record_keys(&horizon), ["k0", "k1", "k2", "k3"]);
    let seqs = sequence_numbers(&db, "Evt");
    assert_eq!(
        &seqs[..2],
        ["100000000000000000001", "100000000000000000002"]
    );
    let mut sorted = seqs.clone();
    sorted.sort();
    assert_eq!(sorted, seqs);

    let arn = get_stream_arn(&db, "Evt");
    let desc = db
        .describe_stream(serde_json::from_value(json!({"StreamArn": &arn})).unwrap())
        .unwrap();
    let range = &desc.stream_description.shards[0].sequence_number_range;
    assert_eq!(
        range.starting_sequence_number.as_deref(),
        Some("100000000000000000001")
    );
    assert_eq!(range.ending_sequence_number.as_ref(), seqs.last());

    let at = shard_iterator(&db, "Evt", "AT_SEQUENCE_NUMBER", Some(&seqs[1]));
    assert_eq!(record_keys(&get_records(&db, &at)), ["k1", "k2", "k3"]);
    let after = shard_iterator(&db, "Evt", "AFTER_SEQUENCE_NUMBER", Some(&seqs[0]));
    assert_eq!(record_keys(&get_records(&db, &after)), ["k1", "k2", "k3"]);
    // A short number a client kept from the earlier build is refused on length.
    assert!(shard_iterator_error(&db, "AT_SEQUENCE_NUMBER", Some("2")).contains("length"));
    let after = shard_iterator(&db, "Evt", "AFTER_SEQUENCE_NUMBER", Some(&seqs[1]));
    assert_eq!(record_keys(&get_records(&db, &after)), ["k2", "k3"]);

    let latest = get_records(&db, &shard_iterator(&db, "Evt", "LATEST", None));
    assert!(latest.records.is_empty());
    put_item(&db, "Evt", "k4", "v");
    let next = latest.next_shard_iterator.unwrap();
    assert_eq!(record_keys(&get_records(&db, &next)), ["k4"]);

    // Reopening leaves every eventID as it was.
    drop(db);
    let db = Database::new(path).unwrap();
    let reread: Vec<String> = all_records(&db, "Evt")
        .into_iter()
        .map(|r| r.event_id)
        .collect();
    assert_eq!(&reread[..4], ids);

    // A record an earlier build writes to the file later, with no eventID,
    // reports its stored sequence number, as the backfill would have stored.
    drop(db);
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch("UPDATE _stream_records SET event_id = NULL WHERE sequence_number = '2'")
        .unwrap();
    let db = Database::new(path).unwrap();
    assert_eq!(all_records(&db, "Evt")[1].event_id, "2");
}

fn shard_iterator_error(db: &Database, kind: &str, seq: Option<&str>) -> String {
    let mut req = json!({
        "StreamArn": get_stream_arn(db, "Evt"),
        "ShardId": "shardId-00000001-Evt",
        "ShardIteratorType": kind
    });
    if let Some(seq) = seq {
        req["SequenceNumber"] = json!(seq);
    }
    match db.get_shard_iterator(serde_json::from_value(req).unwrap()) {
        Err(dynoxide::DynoxideError::ValidationException(msg)) => msg,
        other => panic!("{kind} with {seq:?}: {other:?}"),
    }
}

#[test]
fn a_missing_or_invalid_sequence_number_is_refused_as_dynamodb_does() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Evt", "NEW_IMAGE");
    put_item(&db, "Evt", "k0", "v");

    let too_short = "1 validation error detected: Value at 'sequenceNumber' failed to \
                     satisfy constraint: Member must have length greater than or equal to 21";
    let too_long = "1 validation error detected: Value at 'sequenceNumber' failed to \
                    satisfy constraint: Member must have length less than or equal to 40";
    let unusable = "Invalid SequenceNumber for the shard";
    let forty_nines = "9".repeat(40);
    for kind in ["AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER"] {
        let cases = [
            (
                None,
                "One or more parameter values were invalid: SequenceNumber cannot be null",
            ),
            (Some(""), too_short),
            (Some("5"), too_short),
            (Some("12345678901234567890"), too_short),
            (Some("99999999999999999999999999999999999999999"), too_long),
            (Some("abcdefghijklmnopqrstu"), unusable),
            (Some("-100000000000000000001"), unusable),
            (Some("000000000000000000000"), unusable),
            (Some(forty_nines.as_str()), unusable),
        ];
        for (seq, expected) in cases {
            assert_eq!(
                shard_iterator_error(&db, kind, seq),
                expected,
                "{kind} {seq:?}"
            );
        }
    }

    // A leading `+` on an otherwise valid number is accepted.
    let first = format!("+{}", sequence_numbers(&db, "Evt")[0]);
    let at = shard_iterator(&db, "Evt", "AT_SEQUENCE_NUMBER", Some(&first));
    assert_eq!(record_keys(&get_records(&db, &at)), ["k0"]);
}

#[test]
fn a_sequence_number_on_trim_horizon_or_latest_is_refused() {
    let db = Database::memory().unwrap();
    create_stream_table(&db, "Evt", "NEW_IMAGE");
    put_item(&db, "Evt", "k0", "v");
    let seq = sequence_numbers(&db, "Evt")[0].clone();

    for kind in ["TRIM_HORIZON", "LATEST"] {
        assert_eq!(
            shard_iterator_error(&db, kind, Some(&seq)),
            "One or more parameter values were invalid: \
             SequenceNumber should not be passed for this iterator type"
        );
    }
}
