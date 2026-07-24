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

// ---------------------------------------------------------------------------
// HTTP envelope
//
// The wasm engine is fronted by a transport shim (a headless browser driven by
// `js/wasm-http-bridge.mjs`) so the conformance suite can reach it like any
// other target. Everything the native server decides *before* an operation is
// resolved lives here rather than in that shim, so there is exactly one
// implementation of the wire envelope and the shim stays a dumb pipe. The
// sequence below mirrors `src/server/mod.rs::handle_request` step for step; if
// that changes, this changes with it.
// ---------------------------------------------------------------------------

/// The `X-Amz-Target` prefixes the native server accepts. Kept in step with
/// `server::TARGET_PREFIX` / `server::STREAMS_TARGET_PREFIX`.
const TARGET_PREFIX: &str = "DynamoDB_20120810.";
const STREAMS_TARGET_PREFIX: &str = "DynamoDBStreams_20120810.";

/// `SerializationException` with no message, for a body that is not JSON.
/// Byte-identical to `server::serialization_exception_bare`.
const SERIALIZATION_EXCEPTION_BARE: &str =
    r#"{"__type":"com.amazon.coral.service#SerializationException"}"#;

/// `UnknownOperationException` with no message, for a missing or unrecognised
/// target. Byte-identical to `server::unknown_operation_response`.
const UNKNOWN_OPERATION_BARE: &str =
    r#"{"__type":"com.amazon.coral.service#UnknownOperationException"}"#;

/// One HTTP response: the status the transport must write, and the body.
pub struct HttpOutcome {
    pub status: u16,
    pub body: String,
}

impl HttpOutcome {
    fn new(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }
}

/// Resolve one DynamoDB HTTP request against `backend`, returning the status
/// and body the transport should write verbatim.
///
/// `target` is the raw `X-Amz-Target` header value, or `None` when absent.
/// The ordering matters and matches the native server: body-is-JSON first, then
/// target resolution, then the operation itself. DynamoDB reports a missing
/// target even when the body is also empty, which is why the empty-body check
/// sits after target resolution rather than with the JSON check.
///
/// **Divergence from the native server:** auth material is not validated.
/// `server::auth::validate_auth` mirrors DynamoDB's presence/completeness
/// checks on SigV4 headers (never signature verification, which dynoxide has
/// never done). The suite drives every target through the AWS SDK, which always
/// emits well-formed auth, and carries no auth-malformation tests, so
/// replicating it here would be untested code. Add it if that stops being true.
pub async fn dispatch_http<S: StorageBackend>(
    backend: &S,
    target: Option<&str>,
    body: &str,
) -> HttpOutcome {
    // Non-JSON body → bare SerializationException, before the target is read.
    if !body.is_empty() && serde_json::from_str::<serde_json::Value>(body).is_err() {
        return HttpOutcome::new(400, SERIALIZATION_EXCEPTION_BARE);
    }

    let Some(target) = target else {
        return HttpOutcome::new(400, UNKNOWN_OPERATION_BARE);
    };

    let operation = target
        .strip_prefix(TARGET_PREFIX)
        .or_else(|| target.strip_prefix(STREAMS_TARGET_PREFIX));

    let Some(operation) = operation.filter(|op| crate::dynamo_ops::is_known_operation(op)) else {
        return HttpOutcome::new(400, UNKNOWN_OPERATION_BARE);
    };

    // A valid target with an empty body is a serialisation failure, not a
    // validation error: DynamoDB requires a JSON body on every operation.
    if body.is_empty() {
        return HttpOutcome::new(400, SERIALIZATION_EXCEPTION_BARE);
    }

    // A known DynamoDB operation the preview does not implement. 501 is what
    // makes this land as a skip rather than a failure: the conformance suite's
    // `isUnsupportedFault` accepts the status outright, so the classification
    // does not depend on the message surviving the SDK's error parsing.
    if !SUPPORTED_OPS.contains(&operation) {
        return HttpOutcome::new(501, unsupported_envelope(operation));
    }

    // `route` keeps the error typed, so the status comes straight from
    // `DynoxideError::status_code` rather than being re-derived from the
    // serialised envelope. Same source of truth as the native server.
    match route(backend, operation, body).await {
        Ok(json) => HttpOutcome::new(200, json),
        Err(e) => HttpOutcome::new(e.status_code(), e.to_json()),
    }
}

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
/// [`unsupported_envelope`] for unknown/unsupported ops). Request
/// deserialisation goes through the shared decoder, so a malformed or invalid
/// `request_json` classifies exactly as it does over HTTP, never a panic.
pub async fn dispatch<S: StorageBackend>(
    backend: &S,
    op: &str,
    request_json: &str,
) -> std::result::Result<String, String> {
    if !SUPPORTED_OPS.contains(&op) {
        return Err(unsupported_envelope(op));
    }
    route(backend, op, request_json)
        .await
        .map_err(|e| e.to_json())
}

/// Route one supported operation to its handler, keeping the error typed so a
/// caller that needs an HTTP status (see [`dispatch_http`]) can read it before
/// the envelope is serialised. Callers check [`SUPPORTED_OPS`] first; an
/// unlisted op reaching here is a bug, not a user error.
async fn route<S: StorageBackend>(
    backend: &S,
    op: &str,
    request_json: &str,
) -> crate::Result<String> {
    // Each arm: deserialise the request through the shared decoder (so a parse
    // failure classifies exactly as it does over HTTP), run the shared handler,
    // then serialise the response.
    macro_rules! run {
        ($module:ident) => {{
            match crate::serde_errors::deserialize(request_json) {
                Ok(request) => match actions::$module::execute(backend, request).await {
                    Ok(response) => serde_json::to_string(&response)
                        .map_err(|e| DynoxideError::InternalServerError(e.to_string())),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            }
        }};
    }

    let result: crate::Result<String> = match op {
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
        other => Err(DynoxideError::InternalServerError(format!(
            "route reached with operation '{other}', which is absent from SUPPORTED_OPS"
        ))),
    };

    // Same seam as the HTTP dispatch: resolve the wire-invisible
    // EnvelopedValidation tag for the operation before serialising.
    result.map_err(|e| crate::validation::resolve_request_validation_tag(op, e))
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

    /// Resolve one DynamoDB HTTP request end to end, returning
    /// `{"status": <u16>, "body": "<json>"}` for the transport to write
    /// verbatim.
    ///
    /// `target` is the raw `X-Amz-Target` header, or `null` when the request
    /// carried none. Unlike [`execute`], a protocol-level rejection is not a
    /// promise rejection: an unknown target or a bad body is a status and an
    /// envelope, exactly as it is on the wire. Only calling before `open`
    /// rejects, because that is a caller bug rather than a request outcome.
    #[wasm_bindgen(js_name = dispatchHttp)]
    pub async fn dispatch_http_js(target: Option<String>, body: String) -> Result<String, String> {
        let db = ENGINE.with(|cell| cell.borrow().clone());
        let Some(db) = db else {
            return Err(not_opened_envelope());
        };
        let backend = db.backend().await;
        let outcome = super::dispatch_http(&*backend, target.as_deref(), &body).await;
        Ok(serde_json::json!({
            "status": outcome.status,
            "body": outcome.body,
        })
        .to_string())
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

    // -----------------------------------------------------------------------
    // Request-validation classification parity with the HTTP surface
    // -----------------------------------------------------------------------

    /// The enveloped rejection PutItem and UpdateItem return for
    /// {"NULL": false}, shared verbatim with tests/http_server.rs.
    const NULL_FALSE_ENVELOPED: &str = "1 validation error detected: \
     One or more parameter values were invalid: \
     Null attribute value types must have the value of true";

    /// The bare rejection every other operation returns for {"NULL": false},
    /// shared verbatim with tests/http_server.rs.
    const NULL_FALSE_BARE: &str = "One or more parameter values were invalid: \
     Null attribute value types must have the value of true";

    /// Assert a ValidationException payload with an exact message, and that no
    /// internal marker or serde position suffix leaked into it.
    fn assert_validation_payload(err: &str, expected_message: &str) {
        assert!(
            !err.contains("VALIDATION") && !err.contains(" at line "),
            "internal marker or serde position leaked: {err}"
        );
        let v: serde_json::Value = serde_json::from_str(err).unwrap();
        assert!(
            v["__type"]
                .as_str()
                .unwrap()
                .ends_with("ValidationException"),
            "unexpected __type: {}",
            v["__type"]
        );
        assert_eq!(v["message"].as_str().unwrap(), expected_message);
    }

    #[test]
    fn put_item_null_false_in_item_is_enveloped_validation() {
        let backend = Storage::memory().unwrap();
        run(&backend, "CreateTable", CREATE_MUSIC).unwrap();

        let put = r#"{"TableName":"Music","Item":{"artist":{"S":"a"},"song":{"S":"s1"},"flag":{"NULL":false}}}"#;
        let err = run(&backend, "PutItem", put).unwrap_err();
        assert_validation_payload(&err, NULL_FALSE_ENVELOPED);
    }

    #[test]
    fn get_item_null_false_in_key_is_bare_validation() {
        // The marker-tagged serde failure classifies as a ValidationException
        // (not a SerializationException), reported bare outside
        // PutItem/UpdateItem, exactly as it does over HTTP.
        let backend = Storage::memory().unwrap();
        run(&backend, "CreateTable", CREATE_MUSIC).unwrap();

        let get = r#"{"TableName":"Music","Key":{"artist":{"NULL":false},"song":{"S":"s1"}}}"#;
        let err = run(&backend, "GetItem", get).unwrap_err();
        assert_validation_payload(&err, NULL_FALSE_BARE);
    }

    #[test]
    fn missing_field_classifies_as_validation_like_http() {
        // BatchGetItem without RequestItems is a plain serde "missing field"
        // failure. The shared decoder classifies it as a ValidationException,
        // matching the HTTP surface (previously a SerializationException on
        // this surface).
        let backend = Storage::memory().unwrap();
        let err = run(&backend, "BatchGetItem", "{}").unwrap_err();
        let v: serde_json::Value = serde_json::from_str(&err).unwrap();
        assert_eq!(v["__type"], "com.amazon.coral.validate#ValidationException");
    }

    #[test]
    fn error_payloads_never_leak_markers_or_positions() {
        // Sweep the request-validation family across the dispatch: no error
        // payload may carry the internal VALIDATION markers or serde's
        // position suffix.
        let backend = Storage::memory().unwrap();
        run(&backend, "CreateTable", CREATE_MUSIC).unwrap();

        let cases: &[(&str, &str)] = &[
            (
                "PutItem",
                r#"{"TableName":"Music","Item":{"artist":{"S":"a"},"song":{"S":"s"},"flag":{"NULL":false}}}"#,
            ),
            (
                "UpdateItem",
                r#"{"TableName":"Music","Key":{"artist":{"NULL":false},"song":{"S":"s"}},"UpdateExpression":"SET x = :v","ExpressionAttributeValues":{":v":{"S":"v"}}}"#,
            ),
            (
                "GetItem",
                r#"{"TableName":"Music","Key":{"artist":{"NULL":false},"song":{"S":"s"}}}"#,
            ),
            (
                "DeleteItem",
                r#"{"TableName":"Music","Key":{"artist":{"NULL":false},"song":{"S":"s"}}}"#,
            ),
            (
                "Query",
                r#"{"TableName":"Music","KeyConditionExpression":"artist = :a","ExpressionAttributeValues":{":a":{"NULL":false}}}"#,
            ),
            ("DeleteTable", "{}"),
        ];
        for (op, body) in cases {
            let err = run(&backend, op, body).unwrap_err();
            assert!(
                !err.contains("VALIDATION") && !err.contains(" at line "),
                "{op}: internal marker or serde position leaked: {err}"
            );
        }
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

    // --- HTTP envelope -----------------------------------------------------
    //
    // These pin the wire behaviour the conformance suite sees through the
    // bridge. The suite's `isUnsupportedFault` (src/infra.ts) classifies an op
    // as unimplemented on any of: name `UnknownOperationException`, a message
    // matching /unknown operation|not implemented|unsupported operation|is not
    // supported/i, or HTTP 501. Anything else counts as a conformance failure,
    // so the preview's unimplemented surface landing as skips depends on these.

    /// The suite's classifier, transcribed. Kept here so a change to the
    /// envelope that would break skip-classification fails locally rather than
    /// surfacing as a mysterious drop in the published row.
    fn is_unsupported_fault(status: u16, body: &str) -> bool {
        let v: serde_json::Value = serde_json::from_str(body).unwrap_or(serde_json::Value::Null);
        let type_tail = v["__type"]
            .as_str()
            .unwrap_or("")
            .rsplit('#')
            .next()
            .unwrap_or("")
            .to_owned();
        let message = v["message"].as_str().unwrap_or("").to_lowercase();
        status == 501
            || type_tail == "UnknownOperationException"
            || [
                "unknown operation",
                "not implemented",
                "unsupported operation",
                "is not supported",
            ]
            .iter()
            .any(|needle| message.contains(needle))
    }

    fn http(backend: &Storage, target: Option<&str>, body: &str) -> HttpOutcome {
        pollster::block_on(dispatch_http(backend, target, body))
    }

    #[test]
    fn http_roundtrips_a_supported_operation() {
        let backend = Storage::memory().unwrap();
        let out = http(
            &backend,
            Some("DynamoDB_20120810.CreateTable"),
            CREATE_MUSIC,
        );
        assert_eq!(out.status, 200);

        let out = http(&backend, Some("DynamoDB_20120810.ListTables"), "{}");
        assert_eq!(out.status, 200);
        let v: serde_json::Value = serde_json::from_str(&out.body).unwrap();
        assert_eq!(v["TableNames"][0], "Music");
    }

    #[test]
    fn http_rejects_a_non_json_body_as_bare_serialization_exception() {
        let backend = Storage::memory().unwrap();
        let out = http(&backend, Some("DynamoDB_20120810.ListTables"), "not json");
        assert_eq!(out.status, 400);
        assert_eq!(out.body, SERIALIZATION_EXCEPTION_BARE);
    }

    #[test]
    fn http_reports_a_missing_target_before_an_empty_body() {
        // DynamoDB resolves the target first, so no-target-and-no-body is an
        // UnknownOperationException, not a SerializationException.
        let backend = Storage::memory().unwrap();
        let out = http(&backend, None, "");
        assert_eq!(out.status, 400);
        assert_eq!(out.body, UNKNOWN_OPERATION_BARE);
    }

    #[test]
    fn http_rejects_an_empty_body_on_a_valid_target() {
        let backend = Storage::memory().unwrap();
        let out = http(&backend, Some("DynamoDB_20120810.ListTables"), "");
        assert_eq!(out.status, 400);
        assert_eq!(out.body, SERIALIZATION_EXCEPTION_BARE);
    }

    #[test]
    fn http_rejects_an_unrecognised_target_prefix() {
        let backend = Storage::memory().unwrap();
        let out = http(&backend, Some("Wrong_20120810.ListTables"), "{}");
        assert_eq!(out.status, 400);
        assert_eq!(out.body, UNKNOWN_OPERATION_BARE);
    }

    #[test]
    fn http_accepts_the_streams_target_prefix() {
        // The streams ops are not in SUPPORTED_OPS, so this resolves the target
        // and then reports the op unsupported rather than rejecting the prefix.
        let backend = Storage::memory().unwrap();
        let out = http(&backend, Some("DynamoDBStreams_20120810.ListStreams"), "{}");
        assert_eq!(out.status, 501);
        assert!(is_unsupported_fault(out.status, &out.body));
    }

    #[test]
    fn http_classifies_an_unknown_operation_as_a_skip() {
        let backend = Storage::memory().unwrap();
        let out = http(&backend, Some("DynamoDB_20120810.NoSuchOp"), "{}");
        assert_eq!(out.status, 400);
        assert!(is_unsupported_fault(out.status, &out.body));
    }

    /// Every real DynamoDB operation the preview does not implement must reach
    /// the suite as a skip. This is the case the plan calls load-bearing: if
    /// these land in the failed column the published row misrepresents the
    /// preview.
    #[test]
    fn http_classifies_every_unimplemented_operation_as_a_skip() {
        let backend = Storage::memory().unwrap();
        // The unimplemented surface the plan enumerates, plus the streams ops.
        for op in [
            "UpdateTimeToLive",
            "DescribeTimeToLive",
            "TransactWriteItems",
            "TagResource",
            "UntagResource",
            "ListTagsOfResource",
            "DescribeLimits",
            "ExecuteStatement",
            "ListStreams",
        ] {
            let target = format!("DynamoDB_20120810.{op}");
            let out = http(&backend, Some(&target), "{}");
            assert!(
                is_unsupported_fault(out.status, &out.body),
                "{op} would be scored as a conformance failure: {} {}",
                out.status,
                out.body
            );
        }
    }

    #[test]
    fn http_surfaces_an_api_error_with_its_own_status_and_envelope() {
        // A real validation error must stay a 400 with its DynamoDB envelope,
        // and must not be mistaken for an unimplemented operation.
        let backend = Storage::memory().unwrap();
        let out = http(
            &backend,
            Some("DynamoDB_20120810.DescribeTable"),
            r#"{"TableName":"Absent"}"#,
        );
        assert_eq!(out.status, 400);
        let v: serde_json::Value = serde_json::from_str(&out.body).unwrap();
        assert!(
            v["__type"]
                .as_str()
                .unwrap()
                .contains("ResourceNotFoundException"),
            "unexpected envelope: {}",
            out.body
        );
        assert!(!is_unsupported_fault(out.status, &out.body));
    }

    #[test]
    fn supported_ops_matches_the_routing_table() {
        // `dispatch` gates on SUPPORTED_OPS before routing, so a drift between
        // the two would silently report a routed op as unsupported. Every
        // listed op must reach its handler. An empty request is enough to
        // prove that: whether it succeeds (ListTables) or fails validation
        // (everything else), what it must never be is UnsupportedOperation.
        let backend = Storage::memory().unwrap();
        for op in SUPPORTED_OPS {
            if let Err(err) = run(&backend, op, "{}") {
                assert!(
                    !err.contains(UNSUPPORTED_TYPE),
                    "{op} is in SUPPORTED_OPS but did not route: {err}"
                );
            }
        }
    }
}
