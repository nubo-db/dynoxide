//! Browser smoke-test entry point for the wasm-sqlite backend (preview).
//!
//! Gated behind the `wasm-harness` feature so this scaffolding stays out of the
//! production `wasm-sqlite` build. [`smoke_test`] runs a create/put/get
//! round-trip against [`WasmDatabase`](crate::WasmDatabase) (wa-sqlite + OPFS)
//! and returns a JSON summary, letting a browser page confirm the bridge and
//! the async boundary work end to end without any JS-facing API.

use std::collections::HashMap;

use wasm_bindgen::prelude::*;

use crate::WasmDatabase;
use crate::actions;
use crate::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};

/// Run a CRUD round-trip against a fresh wa-sqlite database and resolve with a
/// JSON summary string. Rejects with the error message on failure.
#[wasm_bindgen]
pub async fn smoke_test() -> Result<JsValue, JsValue> {
    run().await
        .map(|s| JsValue::from_str(&s))
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

async fn run() -> crate::Result<String> {
    let db = WasmDatabase::open("dynoxide-smoke.db").await?;

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
    item.insert("msg".to_string(), AttributeValue::S("hello wasm".to_string()));
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
