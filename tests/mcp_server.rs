//! Integration tests for the MCP server.
//!
//! These tests exercise the MCP server by spawning `dynoxide mcp` as a
//! subprocess and communicating over its stdin/stdout JSON-RPC transport.

#![cfg(feature = "mcp-server")]

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

/// Spawn `dynoxide mcp` and return the child process.
fn spawn_mcp() -> std::process::Child {
    spawn_mcp_with_args(&[])
}

/// Spawn `dynoxide mcp` with additional arguments.
fn spawn_mcp_with_args(extra_args: &[&str]) -> std::process::Child {
    let binary = env!("CARGO_BIN_EXE_dynoxide");
    Command::new(binary)
        .arg("mcp")
        .args(extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn dynoxide mcp")
}

/// Send a JSON-RPC message to the MCP server and read the response.
fn send(child: &mut std::process::Child, msg: &Value) -> Value {
    let stdin = child.stdin.as_mut().unwrap();
    let line = serde_json::to_string(msg).unwrap();
    writeln!(stdin, "{line}").unwrap();
    stdin.flush().unwrap();

    let stdout = child.stdout.as_mut().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    serde_json::from_str(&response).expect("invalid JSON response from MCP server")
}

/// Send a notification (no response expected).
fn notify(child: &mut std::process::Child, msg: &Value) {
    let stdin = child.stdin.as_mut().unwrap();
    let line = serde_json::to_string(msg).unwrap();
    writeln!(stdin, "{line}").unwrap();
    stdin.flush().unwrap();
}

/// Initialize the MCP server and send the `initialized` notification.
fn init_mcp(child: &mut std::process::Child) -> Value {
    let resp = send(
        child,
        &json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1.0"}
            }
        }),
    );
    notify(
        child,
        &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    );
    resp
}

/// Call an MCP tool and return the result.
fn call_tool(child: &mut std::process::Child, id: u64, name: &str, args: Value) -> Value {
    send(
        child,
        &json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {"name": name, "arguments": args}
        }),
    )
}

/// Extract the text content from a tool result, parsed as JSON.
fn tool_content(resp: &Value) -> Value {
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

/// Check if the tool result is an error.
fn is_tool_error(resp: &Value) -> bool {
    resp["result"]["isError"].as_bool().unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_initialize() {
    let mut child = spawn_mcp();
    let resp = init_mcp(&mut child);

    assert_eq!(resp["result"]["serverInfo"]["name"], "dynoxide");
    assert!(resp["result"]["capabilities"]["tools"].is_object());
    assert!(
        resp["result"]["instructions"]
            .as_str()
            .unwrap()
            .contains("DynamoDB")
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_tools_list() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = send(
        &mut child,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    );
    let tools = resp["result"]["tools"].as_array().unwrap();
    let tool_names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();

    // Core DynamoDB tools
    assert!(tool_names.contains(&"list_tables"));
    assert!(tool_names.contains(&"describe_table"));
    assert!(tool_names.contains(&"create_table"));
    assert!(tool_names.contains(&"delete_table"));
    assert!(tool_names.contains(&"update_table"));
    assert!(tool_names.contains(&"put_item"));
    assert!(tool_names.contains(&"get_item"));
    assert!(tool_names.contains(&"update_item"));
    assert!(tool_names.contains(&"delete_item"));
    assert!(tool_names.contains(&"batch_write_item"));
    assert!(tool_names.contains(&"batch_get_item"));
    assert!(tool_names.contains(&"query"));
    assert!(tool_names.contains(&"scan"));
    // Transactions
    assert!(tool_names.contains(&"transact_write_items"));
    assert!(tool_names.contains(&"transact_get_items"));
    // PartiQL
    assert!(tool_names.contains(&"execute_partiql"));
    assert!(tool_names.contains(&"batch_execute_partiql"));
    // TTL
    assert!(tool_names.contains(&"update_time_to_live"));
    assert!(tool_names.contains(&"describe_time_to_live"));
    // Tags
    assert!(tool_names.contains(&"tag_resource"));
    assert!(tool_names.contains(&"untag_resource"));
    assert!(tool_names.contains(&"list_tags_of_resource"));
    // Streams
    assert!(tool_names.contains(&"list_streams"));
    assert!(tool_names.contains(&"describe_stream"));
    assert!(tool_names.contains(&"get_shard_iterator"));
    assert!(tool_names.contains(&"get_records"));
    // Introspection & snapshots
    assert!(tool_names.contains(&"get_database_info"));
    assert!(tool_names.contains(&"create_snapshot"));
    assert!(tool_names.contains(&"restore_snapshot"));
    assert!(tool_names.contains(&"list_snapshots"));
    assert!(tool_names.contains(&"delete_snapshot"));
    assert!(tool_names.contains(&"sweep_ttl"));
    // Bulk operations
    assert!(tool_names.contains(&"bulk_put_items"));
    assert_eq!(tool_names.len(), 34);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_get_database_info_empty() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = call_tool(&mut child, 1, "get_database_info", json!({}));
    let content = tool_content(&resp);

    assert_eq!(content["storage_mode"], "in-memory");
    assert!(content["path"].is_null());
    assert_eq!(content["table_count"], 0);
    assert!(content["tables"].as_array().unwrap().is_empty());
    assert!(content["size_bytes"].as_u64().unwrap() > 0);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_create_table_and_describe() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create table with snake_case params
    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Users",
            "key_schema": [
                {"attribute_name": "pk", "key_type": "HASH"},
                {"attribute_name": "sk", "key_type": "RANGE"}
            ],
            "attribute_definitions": [
                {"attribute_name": "pk", "attribute_type": "S"},
                {"attribute_name": "sk", "attribute_type": "S"}
            ]
        }),
    );
    assert!(!is_tool_error(&resp));

    // Describe table (agent-friendly format)
    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "Users"}),
    );
    let content = tool_content(&resp);
    assert_eq!(content["table_name"], "Users");
    assert_eq!(content["status"], "ACTIVE");
    assert_eq!(content["partition_key"]["name"], "pk");
    assert_eq!(content["partition_key"]["type"], "S");
    assert_eq!(content["sort_key"]["name"], "sk");
    assert_eq!(content["sort_key"]["type"], "S");
    assert_eq!(content["stream_enabled"], false);

    // Describe table (raw DynamoDB format)
    let resp = call_tool(
        &mut child,
        3,
        "describe_table",
        json!({"table_name": "Users", "raw": true}),
    );
    let content = tool_content(&resp);
    assert!(content["Table"]["KeySchema"].is_array());
    assert_eq!(content["Table"]["TableName"], "Users");

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_put_get_delete_item() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create table
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Items",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );

    // Put item
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({
            "table_name": "Items",
            "item": {
                "pk": {"S": "item#1"},
                "name": {"S": "Widget"},
                "price": {"N": "9.99"}
            }
        }),
    );
    assert!(!is_tool_error(&resp));

    // Get item
    let resp = call_tool(
        &mut child,
        3,
        "get_item",
        json!({
            "table_name": "Items",
            "key": {"pk": {"S": "item#1"}}
        }),
    );
    let content = tool_content(&resp);
    assert_eq!(content["Item"]["name"]["S"], "Widget");
    assert_eq!(content["Item"]["price"]["N"], "9.99");

    // Delete item with ALL_OLD
    let resp = call_tool(
        &mut child,
        4,
        "delete_item",
        json!({
            "table_name": "Items",
            "key": {"pk": {"S": "item#1"}},
            "return_values": "ALL_OLD"
        }),
    );
    let content = tool_content(&resp);
    assert_eq!(content["Attributes"]["name"]["S"], "Widget");

    // Get item again — should be empty
    let resp = call_tool(
        &mut child,
        5,
        "get_item",
        json!({
            "table_name": "Items",
            "key": {"pk": {"S": "item#1"}}
        }),
    );
    let content = tool_content(&resp);
    assert!(content.get("Item").is_none() || content["Item"].is_null());

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_query_and_scan() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create table
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Events",
            "key_schema": [
                {"attribute_name": "pk", "key_type": "HASH"},
                {"attribute_name": "sk", "key_type": "RANGE"}
            ],
            "attribute_definitions": [
                {"attribute_name": "pk", "attribute_type": "S"},
                {"attribute_name": "sk", "attribute_type": "S"}
            ]
        }),
    );

    // Put 3 items
    for i in 1..=3 {
        call_tool(
            &mut child,
            10 + i,
            "put_item",
            json!({
                "table_name": "Events",
                "item": {
                    "pk": {"S": "user#1"},
                    "sk": {"S": format!("event#{i}")},
                    "data": {"S": format!("data-{i}")}
                }
            }),
        );
    }

    // Query by pk
    let resp = call_tool(
        &mut child,
        20,
        "query",
        json!({
            "table_name": "Events",
            "key_condition_expression": "pk = :pk",
            "expression_attribute_values": {":pk": {"S": "user#1"}}
        }),
    );
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 3);
    assert_eq!(content["Items"].as_array().unwrap().len(), 3);

    // Query with limit
    let resp = call_tool(
        &mut child,
        21,
        "query",
        json!({
            "table_name": "Events",
            "key_condition_expression": "pk = :pk",
            "expression_attribute_values": {":pk": {"S": "user#1"}},
            "limit": 1
        }),
    );
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 1);
    assert!(content["LastEvaluatedKey"].is_object());

    // Scan
    let resp = call_tool(&mut child, 22, "scan", json!({"table_name": "Events"}));
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 3);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_list_tables() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Empty at start
    let resp = call_tool(&mut child, 1, "list_tables", json!({}));
    let content = tool_content(&resp);
    assert!(content["TableNames"].as_array().unwrap().is_empty());

    // Create two tables
    call_tool(
        &mut child,
        2,
        "create_table",
        json!({
            "table_name": "Alpha",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    call_tool(
        &mut child,
        3,
        "create_table",
        json!({
            "table_name": "Beta",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );

    let resp = call_tool(&mut child, 4, "list_tables", json!({}));
    let content = tool_content(&resp);
    let names = content["TableNames"].as_array().unwrap();
    assert_eq!(names.len(), 2);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_error_handling() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Table not found
    let resp = call_tool(
        &mut child,
        1,
        "get_item",
        json!({
            "table_name": "NonExistent",
            "key": {"pk": {"S": "x"}}
        }),
    );
    assert!(is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "ResourceNotFoundException");
    assert_eq!(content["retryable"], false);

    // Create table, then try to create again (resource in use)
    call_tool(
        &mut child,
        2,
        "create_table",
        json!({
            "table_name": "Dup",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    let resp = call_tool(
        &mut child,
        3,
        "create_table",
        json!({
            "table_name": "Dup",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    assert!(is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "ResourceInUseException");

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_create_table_with_pascal_case() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // DynamoDB PascalCase format should also work
    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Legacy",
            "key_schema": [
                {"AttributeName": "pk", "KeyType": "HASH"}
            ],
            "attribute_definitions": [
                {"AttributeName": "pk", "AttributeType": "S"}
            ]
        }),
    );
    assert!(!is_tool_error(&resp));

    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "Legacy"}),
    );
    let content = tool_content(&resp);
    assert_eq!(content["table_name"], "Legacy");
    assert_eq!(content["partition_key"]["name"], "pk");

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_item() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create table
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "UpdateTest",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );

    // Put an item
    call_tool(
        &mut child,
        2,
        "put_item",
        json!({
            "table_name": "UpdateTest",
            "item": {"pk": {"S": "item#1"}, "count": {"N": "0"}, "name": {"S": "Original"}}
        }),
    );

    // Update the item
    let resp = call_tool(
        &mut child,
        3,
        "update_item",
        json!({
            "table_name": "UpdateTest",
            "key": {"pk": {"S": "item#1"}},
            "update_expression": "SET #n = :newname, #c = #c + :inc",
            "expression_attribute_names": {"#n": "name", "#c": "count"},
            "expression_attribute_values": {":newname": {"S": "Updated"}, ":inc": {"N": "5"}},
            "return_values": "ALL_NEW"
        }),
    );
    assert!(!is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["Attributes"]["name"]["S"], "Updated");
    assert_eq!(content["Attributes"]["count"]["N"], "5");

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_delete_table_with_auto_snapshot() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create and populate a table
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Ephemeral",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "Ephemeral", "item": {"pk": {"S": "x"}}}),
    );

    // Delete the table — should auto-snapshot
    let resp = call_tool(
        &mut child,
        3,
        "delete_table",
        json!({"table_name": "Ephemeral"}),
    );
    assert!(!is_tool_error(&resp));

    // The response should include auto-snapshot info
    let content = tool_content(&resp);
    assert!(content["_auto_snapshot"].is_object());
    assert!(content["_auto_snapshot"]["message"].is_string());

    // Table should be gone
    let resp = call_tool(
        &mut child,
        4,
        "describe_table",
        json!({"table_name": "Ephemeral"}),
    );
    assert!(is_tool_error(&resp));

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_batch_write_and_batch_get() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create table
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Batch",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );

    // Batch write 3 items
    let resp = call_tool(
        &mut child,
        2,
        "batch_write_item",
        json!({
            "request_items": {
                "Batch": [
                    {"put_request": {"item": {"pk": {"S": "a"}, "val": {"N": "1"}}}},
                    {"put_request": {"item": {"pk": {"S": "b"}, "val": {"N": "2"}}}},
                    {"put_request": {"item": {"pk": {"S": "c"}, "val": {"N": "3"}}}}
                ]
            }
        }),
    );
    assert!(!is_tool_error(&resp));

    // Batch get 2 of them
    let resp = call_tool(
        &mut child,
        3,
        "batch_get_item",
        json!({
            "request_items": {
                "Batch": {
                    "keys": [
                        {"pk": {"S": "a"}},
                        {"pk": {"S": "c"}}
                    ]
                }
            }
        }),
    );
    assert!(!is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["Responses"]["Batch"].as_array().unwrap().len(), 2);

    // Batch write with a delete
    let resp = call_tool(
        &mut child,
        4,
        "batch_write_item",
        json!({
            "request_items": {
                "Batch": [
                    {"delete_request": {"key": {"pk": {"S": "b"}}}}
                ]
            }
        }),
    );
    assert!(!is_tool_error(&resp));

    // Verify b is deleted
    let resp = call_tool(&mut child, 5, "scan", json!({"table_name": "Batch"}));
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 2);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_execute_partiql() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create table and insert data
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "PQL",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "PQL", "item": {"pk": {"S": "x"}, "val": {"S": "hello"}}}),
    );

    // SELECT via PartiQL
    let resp = call_tool(
        &mut child,
        3,
        "execute_partiql",
        json!({"statement": "SELECT * FROM PQL WHERE pk = 'x'"}),
    );
    assert!(!is_tool_error(&resp));
    let content = tool_content(&resp);
    let items = content["Items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["val"]["S"], "hello");

    // INSERT via PartiQL
    let resp = call_tool(
        &mut child,
        4,
        "execute_partiql",
        json!({"statement": "INSERT INTO PQL VALUE {'pk': 'y', 'val': 'world'}"}),
    );
    assert!(!is_tool_error(&resp));

    // Verify insert
    let resp = call_tool(&mut child, 5, "scan", json!({"table_name": "PQL"}));
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 2);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_snapshot_lifecycle() {
    // Clean up any leftover snapshots from previous runs
    let _ = std::fs::remove_file(".dynoxide/snapshots/test-snap.db");

    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create a table with data
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "SnapTest",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "SnapTest", "item": {"pk": {"S": "a"}, "val": {"S": "original"}}}),
    );

    // Create a snapshot
    let resp = call_tool(
        &mut child,
        3,
        "create_snapshot",
        json!({"name": "test-snap"}),
    );
    assert!(!is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["name"].as_str().unwrap(), "test-snap");
    assert!(content["size_bytes"].as_u64().unwrap() > 0);

    // List snapshots — should have at least 1
    let resp = call_tool(&mut child, 4, "list_snapshots", json!({}));
    assert!(!is_tool_error(&resp));
    let content = tool_content(&resp);
    assert!(content["count"].as_u64().unwrap() >= 1);

    // Modify data
    call_tool(
        &mut child,
        5,
        "put_item",
        json!({"table_name": "SnapTest", "item": {"pk": {"S": "b"}, "val": {"S": "new"}}}),
    );

    // Verify 2 items
    let resp = call_tool(&mut child, 6, "scan", json!({"table_name": "SnapTest"}));
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 2);

    // Restore from snapshot (uses name, not path)
    let resp = call_tool(
        &mut child,
        7,
        "restore_snapshot",
        json!({"name": "test-snap"}),
    );
    assert!(!is_tool_error(&resp));

    // Verify data is back to 1 item
    let resp = call_tool(&mut child, 8, "scan", json!({"table_name": "SnapTest"}));
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 1);

    // Delete the snapshot via the MCP tool
    let resp = call_tool(
        &mut child,
        9,
        "delete_snapshot",
        json!({"name": "test-snap"}),
    );
    assert!(!is_tool_error(&resp));

    drop(child.stdin.take());
    let _ = child.wait();

    // Clean up any leftover snapshot directory
    let _ = std::fs::remove_dir_all(".dynoxide");
}

// ---------------------------------------------------------------------------
// Read-only mode tests
// ---------------------------------------------------------------------------

#[test]
fn test_read_only_rejects_writes() {
    let mut child = spawn_mcp_with_args(&["--read-only"]);
    init_mcp(&mut child);

    // Reads should work
    let resp = call_tool(&mut child, 1, "list_tables", json!({}));
    assert!(!is_tool_error(&resp));

    let resp = call_tool(&mut child, 2, "get_database_info", json!({}));
    assert!(!is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["read_only"], true);

    // Write tools should be rejected
    let resp = call_tool(
        &mut child,
        3,
        "create_table",
        json!({
            "table_name": "Nope",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    assert!(is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "AccessDeniedException");
    assert!(content["message"].as_str().unwrap().contains("read-only"));

    // put_item should be rejected
    let resp = call_tool(
        &mut child,
        4,
        "put_item",
        json!({"table_name": "X", "item": {"pk": {"S": "a"}}}),
    );
    assert!(is_tool_error(&resp));

    // update_item should be rejected
    let resp = call_tool(
        &mut child,
        5,
        "update_item",
        json!({"table_name": "X", "key": {"pk": {"S": "a"}}}),
    );
    assert!(is_tool_error(&resp));

    // delete_item should be rejected
    let resp = call_tool(
        &mut child,
        6,
        "delete_item",
        json!({"table_name": "X", "key": {"pk": {"S": "a"}}}),
    );
    assert!(is_tool_error(&resp));

    // delete_table should be rejected
    let resp = call_tool(&mut child, 7, "delete_table", json!({"table_name": "X"}));
    assert!(is_tool_error(&resp));

    // batch_write_item should be rejected
    let resp = call_tool(
        &mut child,
        8,
        "batch_write_item",
        json!({"request_items": {}}),
    );
    assert!(is_tool_error(&resp));

    // restore_snapshot should be rejected
    let resp = call_tool(&mut child, 9, "restore_snapshot", json!({"name": "nope"}));
    assert!(is_tool_error(&resp));

    // execute_partiql INSERT should be rejected
    let resp = call_tool(
        &mut child,
        10,
        "execute_partiql",
        json!({"statement": "INSERT INTO X VALUE {'pk': 'a'}"}),
    );
    assert!(is_tool_error(&resp));

    // create_snapshot should be rejected (writes to filesystem)
    let resp = call_tool(&mut child, 11, "create_snapshot", json!({"name": "nope"}));
    assert!(is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "AccessDeniedException");

    // list_snapshots should still work (read-only)
    let resp = call_tool(&mut child, 12, "list_snapshots", json!({}));
    assert!(!is_tool_error(&resp));

    drop(child.stdin.take());
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// Max items limit tests
// ---------------------------------------------------------------------------

#[test]
fn test_max_items_limit() {
    let mut child = spawn_mcp_with_args(&["--max-items", "2"]);
    init_mcp(&mut child);

    // Create table and insert 5 items
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Limited",
            "key_schema": [
                {"attribute_name": "pk", "key_type": "HASH"},
                {"attribute_name": "sk", "key_type": "RANGE"}
            ],
            "attribute_definitions": [
                {"attribute_name": "pk", "attribute_type": "S"},
                {"attribute_name": "sk", "attribute_type": "S"}
            ]
        }),
    );

    for i in 1..=5 {
        call_tool(
            &mut child,
            10 + i,
            "put_item",
            json!({
                "table_name": "Limited",
                "item": {
                    "pk": {"S": "user#1"},
                    "sk": {"S": format!("item#{i}")},
                    "data": {"S": format!("value-{i}")}
                }
            }),
        );
    }

    // Scan without explicit limit — should be capped at 2
    let resp = call_tool(&mut child, 20, "scan", json!({"table_name": "Limited"}));
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 2);
    assert!(content["LastEvaluatedKey"].is_object());

    // Query without explicit limit — should be capped at 2
    let resp = call_tool(
        &mut child,
        21,
        "query",
        json!({
            "table_name": "Limited",
            "key_condition_expression": "pk = :pk",
            "expression_attribute_values": {":pk": {"S": "user#1"}}
        }),
    );
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 2);
    assert!(content["LastEvaluatedKey"].is_object());

    // Explicit limit of 1 should still work (smaller than max)
    let resp = call_tool(
        &mut child,
        22,
        "scan",
        json!({"table_name": "Limited", "limit": 1}),
    );
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 1);

    // Explicit limit of 10 should be capped at 2
    let resp = call_tool(
        &mut child,
        23,
        "scan",
        json!({"table_name": "Limited", "limit": 10}),
    );
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 2);

    // get_database_info should report the limit
    let resp = call_tool(&mut child, 30, "get_database_info", json!({}));
    let content = tool_content(&resp);
    assert_eq!(content["max_items"], 2);

    drop(child.stdin.take());
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// Max size bytes limit tests
// ---------------------------------------------------------------------------

#[test]
fn test_max_size_bytes_limit() {
    // Set a very small size limit (100 bytes) so even a small response triggers it
    let mut child = spawn_mcp_with_args(&["--max-size-bytes", "100"]);
    init_mcp(&mut child);

    // Create table and insert items
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "SizeLimited",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );

    for i in 1..=5 {
        call_tool(
            &mut child,
            10 + i,
            "put_item",
            json!({
                "table_name": "SizeLimited",
                "item": {
                    "pk": {"S": format!("item#{i}")},
                    "description": {"S": "This is a reasonably long description string for testing size limits"}
                }
            }),
        );
    }

    // Scan should fail due to response size
    let resp = call_tool(&mut child, 20, "scan", json!({"table_name": "SizeLimited"}));
    assert!(is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "ResponseSizeLimitExceeded");

    // Query should also fail
    let _resp = call_tool(
        &mut child,
        21,
        "query",
        json!({
            "table_name": "SizeLimited",
            "key_condition_expression": "pk = :pk",
            "expression_attribute_values": {":pk": {"S": "item#1"}}
        }),
    );
    // A single item query might or might not exceed 100 bytes
    // The response includes Count, ScannedCount, Items — might be close
    // Let's not assert on this one since it depends on serialization size

    // get_database_info should report the limit
    let resp = call_tool(&mut child, 30, "get_database_info", json!({}));
    let content = tool_content(&resp);
    assert_eq!(content["max_size_bytes"], 100);

    drop(child.stdin.take());
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// HTTP transport test
// ---------------------------------------------------------------------------

#[test]
fn test_http_transport() {
    use std::io::Read;

    let binary = env!("CARGO_BIN_EXE_dynoxide");

    // Find a free port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    // Start the MCP server in HTTP mode
    let mut child = Command::new(binary)
        .args(["mcp", "--http", "--port", &port.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn dynoxide mcp --http");

    // Wait for the server to be ready by polling the endpoint
    let _url = format!("http://127.0.0.1:{port}/mcp");
    let mut ready = false;
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            ready = true;
            break;
        }
    }
    assert!(ready, "MCP HTTP server did not start within 5 seconds");

    // Send an initialize request via HTTP POST
    let init_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "1.0"}
        }
    });

    // Use a raw TCP connection + HTTP to avoid needing reqwest in a sync test
    let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let body = serde_json::to_string(&init_body).unwrap();
    let request = format!(
        "POST /mcp HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Content-Type: application/json\r\n\
         Accept: application/json, text/event-stream\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();

    // Read the full response
    let mut response_buf = Vec::new();
    let _ = stream.read_to_end(&mut response_buf);
    let response_str = String::from_utf8_lossy(&response_buf);

    // The response should contain a JSON-RPC result with serverInfo
    assert!(
        response_str.contains("dynoxide"),
        "Response should contain server name 'dynoxide', got: {response_str}"
    );

    // Kill the server
    let _ = child.kill();
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// Data model (--data-model) tests
// ---------------------------------------------------------------------------

#[test]
fn test_data_model_in_get_database_info() {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/onetable-test-schema.json"
    );
    let mut child = spawn_mcp_with_args(&["--data-model", fixture]);
    init_mcp(&mut child);

    let resp = call_tool(&mut child, 1, "get_database_info", json!({}));
    let content = tool_content(&resp);

    // data_model should be present
    let dm = &content["data_model"];
    assert_eq!(dm["schema_format"], "onetable:1.1.0");
    assert_eq!(dm["type_attribute"], "_type");

    let entities = dm["entities"].as_array().unwrap();
    assert_eq!(entities.len(), 4);

    // Entities sorted alphabetically
    assert_eq!(entities[0]["name"], "Account");
    assert_eq!(entities[1]["name"], "Project");
    assert_eq!(entities[2]["name"], "Task");
    assert_eq!(entities[3]["name"], "User");

    // Account has no GSIs
    assert!(entities[0]["gsi_mappings"].as_array().unwrap().is_empty());

    // User has GSI1
    let user_gsis = entities[3]["gsi_mappings"].as_array().unwrap();
    assert_eq!(user_gsis.len(), 1);
    assert_eq!(user_gsis[0]["index_name"], "GSI1");
    assert_eq!(user_gsis[0]["pk_template"], "user#${email}");

    // Project has GSI1 + GSI2
    let project_gsis = entities[1]["gsi_mappings"].as_array().unwrap();
    assert_eq!(project_gsis.len(), 2);
    assert_eq!(project_gsis[0]["index_name"], "GSI1");
    assert_eq!(project_gsis[1]["index_name"], "GSI2");

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_data_model_instructions_include_summary() {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/onetable-test-schema.json"
    );
    let mut child = spawn_mcp_with_args(&["--data-model", fixture]);
    let resp = init_mcp(&mut child);

    let instructions = resp["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions.contains("Data model"),
        "instructions should contain data model section"
    );
    assert!(instructions.contains("onetable:1.1.0"));
    assert!(instructions.contains("4 entities"));
    assert!(instructions.contains("Account"));
    assert!(instructions.contains("GSI1"));
    assert!(instructions.contains("get_database_info"));

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_no_data_model_instructions_unchanged() {
    let mut child = spawn_mcp();
    let resp = init_mcp(&mut child);

    let instructions = resp["result"]["instructions"].as_str().unwrap();
    assert!(
        !instructions.contains("Data model"),
        "instructions should not contain data model section"
    );
    assert!(!instructions.contains("onetable"));

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_no_data_model_omitted_from_get_database_info() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = call_tool(&mut child, 1, "get_database_info", json!({}));
    let content = tool_content(&resp);

    assert!(
        content.get("data_model").is_none(),
        "data_model should be absent when no schema loaded"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_data_model_summary_limit_zero_suppresses() {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/onetable-test-schema.json"
    );
    let mut child =
        spawn_mcp_with_args(&["--data-model", fixture, "--data-model-summary-limit", "0"]);
    let resp = init_mcp(&mut child);

    let instructions = resp["result"]["instructions"].as_str().unwrap();
    assert!(
        !instructions.contains("Data model"),
        "summary should be suppressed with limit 0"
    );

    // But data_model should still be in get_database_info
    let resp = call_tool(&mut child, 1, "get_database_info", json!({}));
    let content = tool_content(&resp);
    assert!(
        content.get("data_model").is_some(),
        "data_model should still be in get_database_info"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_data_model_summary_limit_truncates() {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/onetable-test-schema.json"
    );
    let mut child =
        spawn_mcp_with_args(&["--data-model", fixture, "--data-model-summary-limit", "2"]);
    let resp = init_mcp(&mut child);

    let instructions = resp["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions.contains("...and 2 more"),
        "should show truncation message"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_invalid_schema_file_exits_with_error() {
    let binary = env!("CARGO_BIN_EXE_dynoxide");
    let output = std::process::Command::new(binary)
        .args(["mcp", "--data-model", "/nonexistent/schema.json"])
        .output()
        .expect("failed to run dynoxide");

    assert!(
        !output.status.success(),
        "should exit with error for invalid schema file"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("data model file not found"),
        "error message should mention file not found, got: {stderr}"
    );
}

#[test]
fn test_index_name_resolution_uses_name_field() {
    // This test verifies end-to-end that index names are resolved from the
    // OneTable `name` field, not the shorthand key
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/onetable-test-schema.json"
    );
    let mut child = spawn_mcp_with_args(&["--data-model", fixture]);
    init_mcp(&mut child);

    let resp = call_tool(&mut child, 1, "get_database_info", json!({}));
    let content = tool_content(&resp);
    let entities = content["data_model"]["entities"].as_array().unwrap();

    // Find User entity — should have GSI1, not gs1
    let user = entities.iter().find(|e| e["name"] == "User").unwrap();
    let gsis = user["gsi_mappings"].as_array().unwrap();
    assert_eq!(
        gsis[0]["index_name"], "GSI1",
        "should use DynamoDB name from index definition, not OneTable shorthand"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_bulk_put_items() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create table first
    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "BulkTest",
            "key_schema": [
                {"attribute_name": "pk", "key_type": "HASH"},
                {"attribute_name": "sk", "key_type": "RANGE"}
            ],
            "attribute_definitions": [
                {"attribute_name": "pk", "attribute_type": "S"},
                {"attribute_name": "sk", "attribute_type": "S"}
            ]
        }),
    );
    assert!(!is_tool_error(&resp));

    // Bulk insert items
    let resp = call_tool(
        &mut child,
        2,
        "bulk_put_items",
        json!({
            "table_name": "BulkTest",
            "items": [
                {"pk": {"S": "USER#1"}, "sk": {"S": "PROFILE"}, "name": {"S": "Alice"}},
                {"pk": {"S": "USER#2"}, "sk": {"S": "PROFILE"}, "name": {"S": "Bob"}},
                {"pk": {"S": "USER#3"}, "sk": {"S": "PROFILE"}, "name": {"S": "Charlie"}}
            ]
        }),
    );
    assert!(!is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(content["items_imported"], 3);
    assert!(content["bytes_imported"].as_u64().unwrap() > 0);

    // Verify items are queryable
    let resp = call_tool(&mut child, 3, "scan", json!({"table_name": "BulkTest"}));
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 3);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_bulk_put_items_read_only_rejected() {
    let mut child = spawn_mcp_with_args(&["--read-only"]);
    init_mcp(&mut child);

    let resp = call_tool(
        &mut child,
        1,
        "bulk_put_items",
        json!({
            "table_name": "Test",
            "items": [{"pk": {"S": "1"}, "sk": {"S": "2"}}]
        }),
    );
    assert!(is_tool_error(&resp));
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("read-only"));

    drop(child.stdin.take());
    let _ = child.wait();
}
