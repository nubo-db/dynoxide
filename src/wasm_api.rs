//! Operation-level engine API for the browser playground.
//!
//! The smoke harness ([`wasm_harness`](crate::wasm_harness)) proves the bridge
//! works but exposes no way to run an arbitrary DynamoDB operation from JS. This
//! module is that surface: a single JSON-in / JSON-out [`dispatch`] over the
//! shared `actions::*::execute` handlers, wrapped by a `#[wasm_bindgen]` engine
//! that holds one persistent [`WasmDatabase`](crate::WasmDatabase) per Worker.
//!
//! ## Contract
//!
//! `execute(op, request_json)` resolves with the serialised response on success
//! and rejects with a stable JSON error envelope on failure. Both shapes are
//! the same the native HTTP server speaks:
//!
//! - Success: the action's response struct serialised to DynamoDB JSON
//!   (`Count`, `ScannedCount`, and — when the request asks for it —
//!   `ConsumedCapacity` are all present on Query/Scan).
//! - API error: [`DynoxideError::to_json`], carrying `__type` and a message.
//! - Unknown or preview-unsupported op: an `UnsupportedOperation` envelope with
//!   the same `__type`/`message` shape, so a client never has to parse a panic.
//!
//! Positive feature detection goes through [`capabilities`], not error probing:
//! the client asks which ops exist and hides the rest. The dispatch is generic
//! over [`StorageBackend`], so the routing, deserialisation, and envelope
//! behaviour are exercised natively against rusqlite (see the tests) rather than
//! depending on a browser to verify.
//!
//! ## Versioning
//!
//! [`CONTRACT_VERSION`] stamps the envelope shape. Adding an op is non-breaking
//! and does not bump it; changing the request/response/error envelope shape
//! does. The client validates the version on boot (U2/U4) and fails loudly on
//! mismatch rather than mis-parsing a newer engine.

use crate::actions;
use crate::errors::DynoxideError;
use crate::storage_backend::StorageBackend;

/// Engine-contract version. Bump only on an envelope-shape change; adding a
/// supported op is additive and non-breaking.
pub const CONTRACT_VERSION: u32 = 1;

/// The `__type` carried by the engine's own (non-AWS) envelopes, currently just
/// the unsupported/unknown-operation case. Namespaced so a client can match it
/// without colliding with a real DynamoDB `__type`.
const UNSUPPORTED_TYPE: &str = "com.dynoxide.wasm#UnsupportedOperation";

/// Operations the wasm preview engine answers through [`dispatch`]. This is the
/// authoritative feature-detection list the client consumes via
/// [`capabilities`]; anything outside it returns an `UnsupportedOperation`
/// envelope. Kept in sync with the arms of [`dispatch`].
pub const SUPPORTED_OPS: &[&str] = &[
    "CreateTable",
    "DeleteTable",
    "DescribeTable",
    "UpdateTable",
    "ListTables",
    "PutItem",
    "GetItem",
    "DeleteItem",
    "UpdateItem",
    "Query",
    "Scan",
    "BatchGetItem",
    "BatchWriteItem",
    "TransactGetItems",
];

/// Build the `UnsupportedOperation` JSON envelope for an op the engine does not
/// serve (either preview-unsupported or genuinely unknown).
fn unsupported_envelope(op: &str) -> String {
    // `dynamo_ops::is_known_operation` is the wide "real DynamoDB op" set;
    // `SUPPORTED_OPS` is the preview's subset. A known-but-unsupported op phrases
    // differently from a genuinely unknown one.
    let message = if crate::dynamo_ops::is_known_operation(op) {
        format!("Operation '{op}' is not supported by the wasm preview engine")
    } else {
        format!("Unknown operation: '{op}'")
    };
    // Hand-built rather than via DynoxideError so the `__type` is a stable,
    // engine-specific sentinel the client feature-detects on.
    serde_json::json!({ "__type": UNSUPPORTED_TYPE, "message": message }).to_string()
}

/// Run one operation against `backend` and return its response (or error) as a
/// JSON string.
///
/// Generic over the backend so the contract is the same on native rusqlite and
/// the wasm bridge. `Ok` carries the response JSON; `Err` carries a stable error
/// envelope (`DynoxideError::to_json` for API errors,
/// [`unsupported_envelope`] for unknown/unsupported ops). Deserialisation of a
/// malformed `request_json` is itself an API error (`SerializationException`),
/// never a panic.
pub async fn dispatch<S: StorageBackend>(
    backend: &S,
    op: &str,
    request_json: &str,
) -> std::result::Result<String, String> {
    // Each arm: deserialise the request (a parse failure is a
    // SerializationException), run the shared handler (an API error becomes its
    // own envelope), then serialise the response.
    macro_rules! run {
        ($module:ident) => {{
            let request = serde_json::from_str(request_json)
                .map_err(|e| DynoxideError::SerializationException(e.to_string()).to_json())?;
            let response = actions::$module::execute(backend, request)
                .await
                .map_err(|e| e.to_json())?;
            serde_json::to_string(&response)
                .map_err(|e| DynoxideError::InternalServerError(e.to_string()).to_json())
        }};
    }

    match op {
        "CreateTable" => run!(create_table),
        "DeleteTable" => run!(delete_table),
        "DescribeTable" => run!(describe_table),
        "UpdateTable" => run!(update_table),
        "ListTables" => run!(list_tables),
        "PutItem" => run!(put_item),
        "GetItem" => run!(get_item),
        "DeleteItem" => run!(delete_item),
        "UpdateItem" => run!(update_item),
        "Query" => run!(query),
        "Scan" => run!(scan),
        "BatchGetItem" => run!(batch_get_item),
        "BatchWriteItem" => run!(batch_write_item),
        "TransactGetItems" => run!(transact_get_items),
        other => Err(unsupported_envelope(other)),
    }
}

// ---------------------------------------------------------------------------
// wasm-bindgen engine surface
//
// One persistent engine per Worker. the wasm engine is single-threaded, so a
// thread-local holding the opened database is sufficient and avoids exporting a
// generic type across the wasm boundary. `WasmDatabase` is `Clone` (only `Arc`s
// move), so each call clones the handle out of the cell before awaiting — the
// `RefCell` borrow never spans an await point.
// ---------------------------------------------------------------------------

#[cfg(feature = "wasm-sqlite")]
mod engine {
    use super::{CONTRACT_VERSION, SUPPORTED_OPS, dispatch};
    use crate::WasmDatabase;
    use std::cell::RefCell;
    use wasm_bindgen::prelude::*;

    thread_local! {
        static ENGINE: RefCell<Option<WasmDatabase>> = const { RefCell::new(None) };
    }

    /// The boot descriptor `open` resolves with: the static contract plus the
    /// persistence mode this session actually got (`opfs` or `memory`), so the
    /// client can validate the version, learn the op set, and warn when a
    /// session will not persist - all in one round trip.
    fn boot_descriptor(persistence_mode: &str) -> String {
        serde_json::json!({
            "contractVersion": CONTRACT_VERSION,
            "capabilities": SUPPORTED_OPS,
            "persistenceMode": persistence_mode,
        })
        .to_string()
    }

    fn not_opened_envelope() -> String {
        serde_json::json!({
            "__type": "com.dynoxide.wasm#EngineNotOpened",
            "message": "execute called before open(); call open(name) first",
        })
        .to_string()
    }

    /// Open (or reopen) the engine's database under `name`, persisted to OPFS
    /// where available. When `ephemeral` is true, force an in-memory session
    /// that does not persist. Replaces any previously opened database in this
    /// Worker. Resolves with the boot descriptor
    /// (`{ contractVersion, capabilities, persistenceMode }`).
    #[wasm_bindgen]
    pub async fn open(name: String, ephemeral: bool) -> Result<String, String> {
        // Open the new database before tearing down the old one, so a failed
        // open (e.g. a busy OPFS lock) leaves the previous session intact. Once
        // the new one is live, swap it in and close the old connection; the
        // bridge's close frees the old pool's OPFS handles for another tab.
        // Best-effort: a close failure must not fail the re-open.
        let db = WasmDatabase::open_with(&name, ephemeral)
            .await
            .map_err(|e| e.to_json())?;
        let persistence_mode = db.persistence_mode().await;
        let previous = ENGINE.with(|cell| cell.borrow_mut().replace(db));
        if let Some(previous) = previous {
            let _ = previous.close().await;
        }
        Ok(boot_descriptor(&persistence_mode))
    }

    /// Run one DynamoDB operation. Resolves with the response JSON; rejects with
    /// a stable JSON error envelope (see the module docs). Holds the backend
    /// lock for the whole operation so a transaction is atomic and concurrent
    /// callers queue rather than interleave.
    #[wasm_bindgen]
    pub async fn execute(op: String, request_json: String) -> Result<String, String> {
        let db = ENGINE.with(|cell| cell.borrow().clone());
        let Some(db) = db else {
            return Err(not_opened_envelope());
        };
        let backend = db.backend().await;
        dispatch(&*backend, &op, &request_json).await
    }

    /// The supported-operation list, as a JSON array of op names. The client's
    /// positive feature-detection path — it hides anything not listed rather
    /// than probing for `UnsupportedOperation` errors.
    #[wasm_bindgen]
    pub fn capabilities() -> String {
        serde_json::to_string(SUPPORTED_OPS).unwrap_or_else(|_| "[]".to_string())
    }

    /// The engine-contract version the client validates on boot.
    #[wasm_bindgen]
    pub fn contract_version() -> u32 {
        CONTRACT_VERSION
    }
}

#[cfg(all(test, feature = "native-sqlite"))]
mod tests {
    use super::*;
    use crate::storage::Storage;

    /// Drive one operation against a fresh in-memory native backend. The
    /// dispatch is backend-generic, so this exercises the same routing,
    /// deserialisation, and envelope code the wasm engine runs.
    fn run(backend: &Storage, op: &str, json: &str) -> std::result::Result<String, String> {
        pollster::block_on(dispatch(backend, op, json))
    }

    const CREATE_MUSIC: &str = r#"{
        "TableName": "Music",
        "KeySchema": [
            {"AttributeName": "artist", "KeyType": "HASH"},
            {"AttributeName": "song", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "artist", "AttributeType": "S"},
            {"AttributeName": "song", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST"
    }"#;

    fn seed_music(backend: &Storage) {
        run(backend, "CreateTable", CREATE_MUSIC).expect("create table");
        for (song, genre) in [("s1", "rock"), ("s2", "jazz"), ("s3", "rock")] {
            let put = format!(
                r#"{{"TableName":"Music","Item":{{"artist":{{"S":"a"}},"song":{{"S":"{song}"}},"genre":{{"S":"{genre}"}}}}}}"#
            );
            run(backend, "PutItem", &put).expect("put item");
        }
    }

    #[test]
    fn create_put_get_roundtrip() {
        let backend = Storage::memory().unwrap();
        run(&backend, "CreateTable", CREATE_MUSIC).unwrap();

        let put = r#"{"TableName":"Music","Item":{"artist":{"S":"a"},"song":{"S":"s1"},"msg":{"S":"hi"}}}"#;
        run(&backend, "PutItem", put).unwrap();

        let get = r#"{"TableName":"Music","Key":{"artist":{"S":"a"},"song":{"S":"s1"}}}"#;
        let resp = run(&backend, "GetItem", get).unwrap();
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["Item"]["msg"]["S"], "hi");
    }

    #[test]
    fn query_returns_count_and_items() {
        let backend = Storage::memory().unwrap();
        seed_music(&backend);

        let query = r#"{"TableName":"Music","KeyConditionExpression":"artist = :a","ExpressionAttributeValues":{":a":{"S":"a"}}}"#;
        let resp = run(&backend, "Query", query).unwrap();
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["Count"], 3);
        assert_eq!(v["Items"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn scan_with_filter_scans_more_than_it_counts() {
        let backend = Storage::memory().unwrap();
        seed_music(&backend);

        let scan = r#"{"TableName":"Music","FilterExpression":"genre = :g","ExpressionAttributeValues":{":g":{"S":"rock"}}}"#;
        let resp = run(&backend, "Scan", scan).unwrap();
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        // Two of three rows are rock, but all three are scanned: the cost lesson.
        assert_eq!(v["Count"], 2);
        assert_eq!(v["ScannedCount"], 3);
        assert!(v["ScannedCount"].as_u64() > v["Count"].as_u64());
    }

    #[test]
    fn newly_wrapped_update_item_roundtrips() {
        let backend = Storage::memory().unwrap();
        seed_music(&backend);

        let update = r#"{
            "TableName": "Music",
            "Key": {"artist": {"S": "a"}, "song": {"S": "s1"}},
            "UpdateExpression": "SET plays = :p",
            "ExpressionAttributeValues": {":p": {"N": "5"}},
            "ReturnValues": "ALL_NEW"
        }"#;
        let resp = run(&backend, "UpdateItem", update).unwrap();
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["Attributes"]["plays"]["N"], "5");
    }

    #[test]
    fn batch_get_item_returns_seeded_items() {
        let backend = Storage::memory().unwrap();
        seed_music(&backend);

        let batch_get = r#"{
            "RequestItems": {
                "Music": {
                    "Keys": [
                        {"artist": {"S": "a"}, "song": {"S": "s1"}},
                        {"artist": {"S": "a"}, "song": {"S": "s3"}}
                    ]
                }
            }
        }"#;
        let resp = run(&backend, "BatchGetItem", batch_get).unwrap();
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();

        let items = v["Responses"]["Music"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        // Exactly the two requested rows come back, distinct (not s1 twice) and not
        // the unrequested s2. Keys order is not preserved, so sort before comparing.
        let mut songs: Vec<&str> = items
            .iter()
            .map(|item| item["song"]["S"].as_str().unwrap())
            .collect();
        songs.sort_unstable();
        assert_eq!(songs, ["s1", "s3"]);
        assert!(v["UnprocessedKeys"].as_object().unwrap().is_empty());
    }

    #[test]
    fn batch_write_item_puts_and_deletes_persist() {
        let backend = Storage::memory().unwrap();
        seed_music(&backend);

        // One batch: delete an existing row, insert a new one.
        let batch_write = r#"{
            "RequestItems": {
                "Music": [
                    {"DeleteRequest": {"Key": {"artist": {"S": "a"}, "song": {"S": "s2"}}}},
                    {"PutRequest": {"Item": {"artist": {"S": "a"}, "song": {"S": "s4"}, "genre": {"S": "pop"}}}}
                ]
            }
        }"#;
        let resp = run(&backend, "BatchWriteItem", batch_write).unwrap();
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert!(v["UnprocessedItems"].as_object().unwrap().is_empty());

        // Read back through dispatch: the write actually mutated the backend.
        let get_s2 = r#"{"TableName":"Music","Key":{"artist":{"S":"a"},"song":{"S":"s2"}}}"#;
        let s2: serde_json::Value =
            serde_json::from_str(&run(&backend, "GetItem", get_s2).unwrap()).unwrap();
        assert!(s2.get("Item").is_none(), "s2 should have been deleted");

        let get_s4 = r#"{"TableName":"Music","Key":{"artist":{"S":"a"},"song":{"S":"s4"}}}"#;
        let s4: serde_json::Value =
            serde_json::from_str(&run(&backend, "GetItem", get_s4).unwrap()).unwrap();
        assert_eq!(s4["Item"]["genre"]["S"], "pop");
    }

    #[test]
    fn transact_get_items_preserves_position_for_present_and_missing() {
        let backend = Storage::memory().unwrap();
        seed_music(&backend);

        // One present key, one absent key: the response must keep both slots in order.
        let transact_get = r#"{
            "TransactItems": [
                {"Get": {"TableName": "Music", "Key": {"artist": {"S": "a"}, "song": {"S": "s1"}}}},
                {"Get": {"TableName": "Music", "Key": {"artist": {"S": "a"}, "song": {"S": "nope"}}}}
            ]
        }"#;
        let resp = run(&backend, "TransactGetItems", transact_get).unwrap();
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();

        let responses = v["Responses"].as_array().unwrap();
        assert_eq!(responses.len(), 2);
        // Position 0 is the present row; position 1 is the miss, serialised as {}.
        assert_eq!(responses[0]["Item"]["genre"]["S"], "rock");
        assert!(responses[1].get("Item").is_none());
    }

    #[test]
    fn unknown_op_returns_envelope_not_panic() {
        let backend = Storage::memory().unwrap();
        let err = run(&backend, "FlyToTheMoon", "{}").unwrap_err();
        let v: serde_json::Value = serde_json::from_str(&err).unwrap();
        assert_eq!(v["__type"], "com.dynoxide.wasm#UnsupportedOperation");
        assert!(v["message"].as_str().unwrap().contains("Unknown operation"));
    }

    #[test]
    fn unsupported_preview_op_returns_envelope() {
        let backend = Storage::memory().unwrap();
        let err = run(&backend, "UpdateTimeToLive", "{}").unwrap_err();
        let v: serde_json::Value = serde_json::from_str(&err).unwrap();
        assert_eq!(v["__type"], "com.dynoxide.wasm#UnsupportedOperation");
        assert!(v["message"].as_str().unwrap().contains("not supported"));
    }

    #[test]
    fn conditional_check_failure_surfaces_in_envelope() {
        let backend = Storage::memory().unwrap();
        seed_music(&backend);

        // attribute_not_exists on an existing key must fail the condition.
        let put = r#"{
            "TableName": "Music",
            "Item": {"artist": {"S": "a"}, "song": {"S": "s1"}},
            "ConditionExpression": "attribute_not_exists(artist)"
        }"#;
        let err = run(&backend, "PutItem", put).unwrap_err();
        let v: serde_json::Value = serde_json::from_str(&err).unwrap();
        assert!(
            v["__type"]
                .as_str()
                .unwrap()
                .contains("ConditionalCheckFailedException")
        );
    }

    #[test]
    fn malformed_request_json_is_a_serialization_error() {
        let backend = Storage::memory().unwrap();
        let err = run(&backend, "PutItem", "{ this is not json").unwrap_err();
        let v: serde_json::Value = serde_json::from_str(&err).unwrap();
        assert!(
            v["__type"]
                .as_str()
                .unwrap()
                .contains("SerializationException")
        );
    }

    #[test]
    fn contract_advertises_a_version_and_the_supported_ops() {
        assert_eq!(CONTRACT_VERSION, 1);
        assert!(SUPPORTED_OPS.contains(&"Query"));
        assert!(SUPPORTED_OPS.contains(&"Scan"));
    }

    #[test]
    fn update_table_adds_a_gsi_and_backfills_through_dispatch() {
        let backend = Storage::memory().unwrap();
        seed_music(&backend);

        // UpdateTable is a dispatched op: adding a GSI on the genre attribute the
        // seeded rows carry exercises the dispatch arm and the add-GSI handler
        // path (create index, backfill existing rows, update metadata).
        let update = r#"{
            "TableName": "Music",
            "AttributeDefinitions": [
                {"AttributeName": "artist", "AttributeType": "S"},
                {"AttributeName": "song", "AttributeType": "S"},
                {"AttributeName": "genre", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexUpdates": [
                {"Create": {
                    "IndexName": "GenreIndex",
                    "KeySchema": [{"AttributeName": "genre", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }}
            ]
        }"#;
        let resp = run(&backend, "UpdateTable", update).unwrap();
        assert!(
            resp.contains("GenreIndex"),
            "the response should describe the new GSI"
        );

        // The pre-existing rows were backfilled: a query on the new index returns
        // the two rock rows.
        let q = r#"{"TableName":"Music","IndexName":"GenreIndex","KeyConditionExpression":"genre = :g","ExpressionAttributeValues":{":g":{"S":"rock"}}}"#;
        let qv: serde_json::Value =
            serde_json::from_str(&run(&backend, "Query", q).unwrap()).unwrap();
        assert_eq!(qv["Count"], 2);

        assert!(SUPPORTED_OPS.contains(&"UpdateTable"));
    }
}
