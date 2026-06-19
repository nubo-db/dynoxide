//! Browser smoke-test entry point for the wasm-sqlite backend (preview).
//!
//! Gated behind the `wasm-harness` feature so this scaffolding stays out of the
//! production `wasm-sqlite` build. [`smoke_test`] runs a create/put/get
//! round-trip against [`WasmDatabase`](crate::WasmDatabase) (SQLite + OPFS)
//! and returns a JSON summary, letting a browser page confirm the bridge and
//! the async boundary work end to end without any JS-facing API.

use std::collections::HashMap;

use wasm_bindgen::prelude::*;

use crate::WasmDatabase;
use crate::actions;
use crate::storage::{CreateTableMetadata, QueryParams, ScanParams};
use crate::storage_backend::{StorageBackend, WasmBridgeBackend};
use crate::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};

/// Run a CRUD round-trip against a fresh SQLite database and resolve with a
/// JSON summary string. Rejects with the error message on failure.
#[wasm_bindgen]
pub async fn smoke_test() -> Result<JsValue, JsValue> {
    run()
        .await
        .map(|s| JsValue::from_str(&s))
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

async fn run() -> crate::Result<String> {
    let db = WasmDatabase::open("dynoxide-smoke.db").await?;

    // OPFS persists across runs, so clear any table left by a prior run.
    let _ = db
        .delete_table(actions::delete_table::DeleteTableRequest {
            table_name: "SmokeTable".to_string(),
        })
        .await;

    db.create_table(actions::create_table::CreateTableRequest {
        table_name: "SmokeTable".to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        ..Default::default()
    })
    .await?;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("smoke-1".to_string()));
    item.insert(
        "msg".to_string(),
        AttributeValue::S("hello wasm".to_string()),
    );
    db.put_item(actions::put_item::PutItemRequest {
        table_name: "SmokeTable".to_string(),
        item,
        ..Default::default()
    })
    .await?;

    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("smoke-1".to_string()));
    let got = db
        .get_item(actions::get_item::GetItemRequest {
            table_name: "SmokeTable".to_string(),
            key,
            ..Default::default()
        })
        .await?;

    let fetched = got
        .item
        .as_ref()
        .and_then(|m| m.get("msg"))
        .and_then(|v| match v {
            AttributeValue::S(s) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("<missing>");

    let tables = db
        .list_tables(actions::list_tables::ListTablesRequest::default())
        .await?;

    Ok(format!(
        "{{\"created\":true,\"put\":true,\"got_msg\":\"{}\",\"table_count\":{}}}",
        fetched,
        tables.table_names.len()
    ))
}

/// Backend-level index/scan round-trip: exercises GSI write, query, and
/// parallel scan (which drives `fnv1a_hash`) directly through
/// [`WasmBridgeBackend`] on a separate OPFS database. The two scan segments
/// must together cover every GSI row exactly once.
#[wasm_bindgen]
pub async fn index_scan_test() -> Result<JsValue, JsValue> {
    run_index()
        .await
        .map(|s| JsValue::from_str(&s))
        .map_err(|e| JsValue::from_str(&e))
}

async fn run_index() -> std::result::Result<String, String> {
    let be = WasmBridgeBackend::open("dynoxide-index.db")
        .await
        .map_err(|e| e.to_string())?;

    // OPFS persists across runs, so drop anything left by a prior run.
    let _ = be.drop_gsi_table("IdxT", "gsi1").await;
    let _ = be.drop_data_table("IdxT").await;
    let _ = be.delete_table_metadata("IdxT").await;

    be.insert_table_metadata(&CreateTableMetadata {
        table_name: "IdxT",
        key_schema: "[]",
        attribute_definitions: "[]",
        created_at: 0,
        ..Default::default()
    })
    .await
    .map_err(|e| e.to_string())?;
    be.create_data_table("IdxT")
        .await
        .map_err(|e| e.to_string())?;

    let keys = [("a", "1"), ("b", "1"), ("c", "1"), ("d", "1")];
    for (pk, sk) in keys {
        be.put_item("IdxT", pk, sk, &format!("{{\"pk\":\"{pk}\"}}"), 10)
            .await
            .map_err(|e| e.to_string())?;
    }

    let base_scan = be
        .scan_items("IdxT", &ScanParams::default())
        .await
        .map_err(|e| e.to_string())?;

    be.create_gsi_table("IdxT", "gsi1")
        .await
        .map_err(|e| e.to_string())?;
    for (pk, sk) in keys {
        be.insert_gsi_item(
            "IdxT",
            "gsi1",
            "G",
            sk,
            pk,
            sk,
            &format!("{{\"pk\":\"{pk}\"}}"),
        )
        .await
        .map_err(|e| e.to_string())?;
    }

    let gsi_query = be
        .query_gsi_items(
            "IdxT",
            "gsi1",
            "G",
            &QueryParams {
                forward: true,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.to_string())?;

    // Two-segment parallel scan exercises fnv1a_hash; the union covers all rows.
    let seg0 = be
        .scan_gsi_items(
            "IdxT",
            "gsi1",
            &ScanParams {
                segment: Some(0),
                total_segments: Some(2),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    let seg1 = be
        .scan_gsi_items(
            "IdxT",
            "gsi1",
            &ScanParams {
                segment: Some(1),
                total_segments: Some(2),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.to_string())?;

    Ok(format!(
        "{{\"base_scan\":{},\"gsi_query\":{},\"seg0\":{},\"seg1\":{},\"seg_union\":{}}}",
        base_scan.len(),
        gsi_query.len(),
        seg0.len(),
        seg1.len(),
        seg0.len() + seg1.len()
    ))
}

/// Error-envelope fidelity: a client-facing error raised by the shared action
/// handlers must surface the same AWS envelope on the wasm backend as on
/// native. Returns the `__type` and HTTP status of a GetItem against a missing
/// table (expected: ResourceNotFoundException, 400).
#[wasm_bindgen]
pub async fn error_fidelity_test() -> Result<JsValue, JsValue> {
    run_errors()
        .await
        .map(|s| JsValue::from_str(&s))
        .map_err(|e| JsValue::from_str(&e))
}

async fn run_errors() -> std::result::Result<String, String> {
    let db = WasmDatabase::open("dynoxide-smoke.db")
        .await
        .map_err(|e| e.to_string())?;

    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("x".to_string()));
    let missing = db
        .get_item(actions::get_item::GetItemRequest {
            table_name: "NoSuchTable".to_string(),
            key,
            ..Default::default()
        })
        .await;

    let (etype, status) = match missing {
        Ok(_) => ("<no error>".to_string(), 0),
        Err(e) => (e.error_type().to_string(), e.status_code()),
    };

    Ok(format!(
        "{{\"missing_table_type\":\"{etype}\",\"missing_table_status\":{status}}}"
    ))
}
