//! Integration tests for the MCP server.
//!
//! These tests exercise the MCP server by spawning `dynoxide mcp` as a
//! subprocess and communicating over its stdin/stdout JSON-RPC transport.

#![cfg(all(feature = "mcp-server", feature = "cli"))]

mod common;

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
    assert_eq!(
        resp["result"]["serverInfo"]["version"],
        dynoxide::PRODUCT_VERSION,
        "MCP reports the product version, not the crate version"
    );
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
    // Vector search
    assert!(tool_names.contains(&"search_vectors"));
    assert_eq!(tool_names.len(), 35);

    drop(child.stdin.take());
    let _ = child.wait();
}

/// The tools that wrap an operation DynamoDB reports consumed capacity for, and
/// so take `return_consumed_capacity`. Nothing else should.
const CAPACITY_TOOLS: [&str; 14] = [
    "batch_execute_partiql",
    "batch_get_item",
    "batch_write_item",
    "delete_item",
    "execute_partiql",
    "execute_transaction_partiql",
    "get_item",
    "put_item",
    "query",
    "scan",
    "search_vectors",
    "transact_get_items",
    "transact_write_items",
    "update_item",
];

#[test]
fn exactly_the_capacity_bearing_tools_take_return_consumed_capacity() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = send(
        &mut child,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    );
    let tools = resp["result"]["tools"].as_array().unwrap();
    let mut taking: Vec<&str> = tools
        .iter()
        .filter(|t| !t["inputSchema"]["properties"]["return_consumed_capacity"].is_null())
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    taking.sort_unstable();
    assert_eq!(taking, CAPACITY_TOOLS, "out of {} tools", tools.len());

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn the_capacity_parameter_lists_its_values_in_the_schema() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = send(
        &mut child,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    );
    let tools = resp["result"]["tools"].as_array().unwrap();
    for name in CAPACITY_TOOLS {
        let tool = tools.iter().find(|t| t["name"] == name).unwrap();
        let property = &tool["inputSchema"]["properties"]["return_consumed_capacity"];
        assert_eq!(
            property["enum"],
            json!(["INDEXES", "TOTAL", "NONE", null]),
            "{name}: {property}"
        );
        assert_eq!(property["type"], json!(["string", "null"]), "{name}");
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn a_tool_returns_the_capacity_block_and_refuses_an_unknown_mode() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Capacity",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );

    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({
            "table_name": "Capacity",
            "item": {"pk": {"S": "a"}},
            "return_consumed_capacity": "TOTAL"
        }),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    assert_eq!(
        tool_content(&resp)["ConsumedCapacity"],
        json!({"TableName": "Capacity", "CapacityUnits": 1.0})
    );

    let resp = call_tool(
        &mut child,
        4,
        "put_item",
        json!({
            "table_name": "Capacity",
            "item": {"pk": {"S": "b"}},
            "return_consumed_capacity": "INDEXES"
        }),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    assert_eq!(
        tool_content(&resp)["ConsumedCapacity"],
        json!({
            "TableName": "Capacity",
            "CapacityUnits": 1.0,
            "Table": {"CapacityUnits": 1.0}
        })
    );

    let resp = call_tool(
        &mut child,
        3,
        "get_item",
        json!({
            "table_name": "Capacity",
            "key": {"pk": {"S": "a"}},
            "return_consumed_capacity": "BOGUS"
        }),
    );
    assert!(is_tool_error(&resp), "{resp}");
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "ValidationException");
    assert_eq!(
        content["message"],
        "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to \
         satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// The item tools read a `BOOL` or `NULL` member as DynamoDB does on the wire:
/// the strings it reads as booleans, at the top of an item or nested, and a
/// false NULL or an unreadable BOOL refused in its words (captured eu-west-2
/// and us-east-1, 2026-10-10, over HTTP; a nested value reading as true is
/// inferred from the one decoder that reads every value).
#[test]
fn an_item_tool_reads_bool_and_null_strings_as_dynamodb_does() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Values",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({
            "table_name": "Values",
            "item": {
                "pk": {"S": "a"},
                "b": {"BOOL": "yes"},
                "n": {"NULL": "T"},
                "l": {"L": [{"BOOL": "no"}]},
                "m": {"M": {"k": {"BOOL": "1"}}}
            }
        }),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    let resp = call_tool(
        &mut child,
        3,
        "get_item",
        json!({"table_name": "Values", "key": {"pk": {"S": "a"}}}),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    let item = &tool_content(&resp)["Item"];
    assert_eq!(item["b"], json!({"BOOL": true}));
    assert_eq!(item["n"], json!({"NULL": true}));
    assert_eq!(item["l"], json!({"L": [{"BOOL": false}]}));
    assert_eq!(item["m"], json!({"M": {"k": {"BOOL": true}}}));

    let null_false = "One or more parameter values were invalid: Null attribute value types \
                      must have the value of true";
    for (id, value, error_type, message) in [
        (
            4,
            json!({"NULL": "false"}),
            "ValidationException",
            null_false.to_string(),
        ),
        (
            5,
            json!({"L": [{"NULL": "no"}]}),
            "ValidationException",
            null_false.to_string(),
        ),
        (
            6,
            json!({"NULL": false}),
            "ValidationException",
            format!("1 validation error detected: {null_false}"),
        ),
        (
            7,
            json!({"BOOL": "abc"}),
            "SerializationException",
            "Unexpected token received from parser".to_string(),
        ),
    ] {
        let resp = call_tool(
            &mut child,
            id,
            "put_item",
            json!({"table_name": "Values", "item": {"pk": {"S": "x"}, "v": value}}),
        );
        assert!(is_tool_error(&resp), "{value}: {resp}");
        let content = tool_content(&resp);
        assert_eq!(content["error_type"], error_type, "{value}: {content}");
        assert_eq!(content["message"], message, "{value}: {content}");
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `return_values_on_condition_check_failure` on `execute_partiql` is
/// ExecuteStatement's `ReturnValuesOnConditionCheckFailure`: `ALL_OLD` hands
/// back the item an UPDATE or DELETE failed its condition on, `NONE` doesn't,
/// and anything else is refused in DynamoDB's words (captured eu-west-2 and
/// us-east-1, 2026-10-10). The item tools' `ALL_OLD` hands it back the same
/// way.
#[test]
fn a_failed_condition_hands_back_the_item_when_asked() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Guarded",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    let stored = json!({"pk": {"S": "a"}, "v": {"N": "1"}});
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "Guarded", "item": stored}),
    );
    assert!(!is_tool_error(&resp), "{resp}");

    let update = r#"UPDATE "Guarded" SET w = 1 WHERE pk = 'a' AND v = 99"#;
    let resp = call_tool(
        &mut child,
        3,
        "execute_partiql",
        json!({"statement": update, "return_values_on_condition_check_failure": "ALL_OLD"}),
    );
    assert!(is_tool_error(&resp), "{resp}");
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "ConditionalCheckFailedException");
    assert_eq!(content["message"], "The conditional request failed");
    assert_eq!(content["item"], stored);

    for (id, mode) in [(4, json!("NONE")), (5, Value::Null)] {
        let mut args = json!({"statement": update});
        if !mode.is_null() {
            args["return_values_on_condition_check_failure"] = mode.clone();
        }
        let resp = call_tool(&mut child, id, "execute_partiql", args);
        assert!(is_tool_error(&resp), "{resp}");
        let content = tool_content(&resp);
        assert_eq!(content["error_type"], "ConditionalCheckFailedException");
        assert!(content.get("item").is_none(), "{mode}: {content}");
    }

    let resp = call_tool(
        &mut child,
        6,
        "execute_partiql",
        json!({"statement": update, "return_values_on_condition_check_failure": "BOGUS"}),
    );
    assert!(is_tool_error(&resp), "{resp}");
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "ValidationException");
    assert_eq!(
        content["message"],
        "1 validation error detected: Value 'BOGUS' at \
         'returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must \
         satisfy enum value set: [ALL_OLD, NONE]"
    );

    let resp = call_tool(
        &mut child,
        7,
        "put_item",
        json!({
            "table_name": "Guarded",
            "item": {"pk": {"S": "a"}, "v": {"N": "2"}},
            "condition_expression": "attribute_not_exists(pk)",
            "return_values_on_condition_check_failure": "ALL_OLD"
        }),
    );
    assert!(is_tool_error(&resp), "{resp}");
    assert_eq!(tool_content(&resp)["item"], stored);

    drop(child.stdin.take());
    let _ = child.wait();
}

/// The item a failed condition hands back writes a number set in the order
/// the HTTP API writes it for the same operation: ExecuteStatement in hash
/// order, PutItem in numeric order (both as the HTTP API answers on this
/// build, which follows DynamoDB's captures).
#[test]
fn a_failed_condition_writes_number_sets_in_the_operations_order() {
    let mut child = mcp_with_table("Ordered");
    let stored = json!({"pk": {"S": "a"}, "ns": {"NS": ["3", "1", "2"]}, "v": {"N": "1"}});
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "Ordered", "item": stored}),
    );
    assert!(!is_tool_error(&resp), "{resp}");

    let resp = call_tool(
        &mut child,
        3,
        "execute_partiql",
        json!({
            "statement": r#"UPDATE "Ordered" SET v = 2 WHERE pk = 'a' AND v = 9"#,
            "return_values_on_condition_check_failure": "ALL_OLD"
        }),
    );
    assert_eq!(tool_error(&resp).0, "ConditionalCheckFailedException");
    assert_eq!(
        tool_content(&resp)["item"]["ns"],
        json!({"NS": ["3", "2", "1"]})
    );

    let resp = call_tool(
        &mut child,
        4,
        "put_item",
        json!({
            "table_name": "Ordered",
            "item": {"pk": {"S": "a"}},
            "condition_expression": "attribute_not_exists(pk)",
            "return_values_on_condition_check_failure": "ALL_OLD"
        }),
    );
    assert_eq!(tool_error(&resp).0, "ConditionalCheckFailedException");
    assert_eq!(
        tool_content(&resp)["item"]["ns"],
        json!({"NS": ["1", "2", "3"]})
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `ALL_OLD` through the batch and transaction tools hands back the item a
/// failed condition was evaluated against where the HTTP API does: on the
/// member's `Error` for `batch_execute_partiql`, and on the statement's or
/// action's cancellation reason for `execute_transaction_partiql` and
/// `transact_write_items`, one reason per entry with `Code: None` for those
/// that would have succeeded. Each expectation is the HTTP API's answer for
/// the same request on this build.
#[test]
fn all_old_reaches_the_batch_and_transaction_tools_as_over_http() {
    let mut child = mcp_with_table("Guarded");
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({
            "table_name": "Guarded",
            "item": {"pk": {"S": "a"}, "ns": {"NS": ["3", "1", "2"]}, "v": {"N": "1"}}
        }),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    // Hash order, as every one of these writes the item over HTTP.
    let item = json!({"pk": {"S": "a"}, "ns": {"NS": ["3", "2", "1"]}, "v": {"N": "1"}});
    let failed = json!({
        "Code": "ConditionalCheckFailed",
        "Message": "The conditional request failed",
        "Item": item
    });
    let update = r#"UPDATE "Guarded" SET v = 2 WHERE pk = 'a' AND v = 9"#;

    let resp = call_tool(
        &mut child,
        3,
        "batch_execute_partiql",
        json!({"statements": [
            {"Statement": update, "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}
        ]}),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    assert_eq!(
        tool_content(&resp),
        json!({"Responses": [{"Error": failed, "TableName": "Guarded"}]})
    );

    let resp = call_tool(
        &mut child,
        4,
        "execute_transaction_partiql",
        json!({"transact_statements": [
            {"Statement": update, "ReturnValuesOnConditionCheckFailure": "ALL_OLD"},
            {"Statement": r#"INSERT INTO "Guarded" VALUE {'pk': 'b'}"#}
        ]}),
    );
    assert_eq!(
        tool_error(&resp),
        (
            "TransactionCanceledException".to_string(),
            "Transaction cancelled, please refer cancellation reasons for specific reasons \
             [ConditionalCheckFailed, None]"
                .to_string()
        )
    );
    assert_eq!(
        tool_content(&resp)["cancellation_reasons"],
        json!([failed, {"Code": "None"}])
    );

    let resp = call_tool(
        &mut child,
        5,
        "transact_write_items",
        json!({"transact_items": [
            {"Put": {"TableName": "Guarded", "Item": {"pk": {"S": "c"}}}},
            {"ConditionCheck": {
                "TableName": "Guarded", "Key": {"pk": {"S": "a"}},
                "ConditionExpression": "v = :n",
                "ExpressionAttributeValues": {":n": {"N": "9"}},
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }}
        ]}),
    );
    assert_eq!(
        tool_error(&resp),
        (
            "TransactionCanceledException".to_string(),
            "Transaction cancelled, please refer cancellation reasons for specific reasons \
             [None, ConditionalCheckFailed]"
                .to_string()
        )
    );
    assert_eq!(
        tool_content(&resp)["cancellation_reasons"],
        json!([{"Code": "None"}, failed])
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn the_instructions_say_capacity_can_be_requested() {
    let mut child = spawn_mcp();
    let resp = init_mcp(&mut child);
    let instructions = resp["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions.contains("`return_consumed_capacity`"),
        "{instructions}"
    );
    assert!(
        instructions.contains("Vector search: search_vectors"),
        "{instructions}"
    );

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
fn test_create_table_billing_mode_pay_per_request() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "OnDemand",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST"
        }),
    );
    assert!(!is_tool_error(&resp), "create_table failed: {resp}");

    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "OnDemand", "raw": true}),
    );
    let content = tool_content(&resp);
    assert_eq!(
        content["Table"]["BillingModeSummary"]["BillingMode"],
        "PAY_PER_REQUEST"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_create_table_invalid_billing_mode_rejected() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "BadMode",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "ON_DEMAND"
        }),
    );
    assert_validation_tool_error(
        &resp,
        "1 validation error detected: Value 'ON_DEMAND' at 'billingMode' failed to satisfy \
         constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]",
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_create_table_provisioned_with_throughput() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Provisioned",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PROVISIONED",
            "provisioned_throughput": {"read_capacity_units": 7, "write_capacity_units": 3}
        }),
    );
    assert!(!is_tool_error(&resp), "create_table failed: {resp}");

    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "Provisioned", "raw": true}),
    );
    let content = tool_content(&resp);
    assert_eq!(
        content["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"],
        7
    );
    assert_eq!(
        content["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"],
        3
    );

    // The default agent-friendly view surfaces the capacity settings too.
    let resp = call_tool(
        &mut child,
        3,
        "describe_table",
        json!({"table_name": "Provisioned"}),
    );
    let flat = tool_content(&resp);
    assert_eq!(flat["billing_mode"], "PROVISIONED");
    assert_eq!(flat["provisioned_throughput"]["read_capacity_units"], 7);
    assert_eq!(flat["provisioned_throughput"]["write_capacity_units"], 3);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_table_billing_mode_round_trip() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Flip",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );

    // Flip to on-demand.
    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({"table_name": "Flip", "billing_mode": "PAY_PER_REQUEST"}),
    );
    assert!(
        !is_tool_error(&resp),
        "flip to PAY_PER_REQUEST failed: {resp}"
    );

    let resp = call_tool(
        &mut child,
        3,
        "describe_table",
        json!({"table_name": "Flip", "raw": true}),
    );
    let content = tool_content(&resp);
    assert_eq!(
        content["Table"]["BillingModeSummary"]["BillingMode"],
        "PAY_PER_REQUEST"
    );

    // Flip back to provisioned with snake_case throughput keys; the values
    // must land, not silently read as zero.
    let resp = call_tool(
        &mut child,
        4,
        "update_table",
        json!({
            "table_name": "Flip",
            "billing_mode": "PROVISIONED",
            "provisioned_throughput": {"read_capacity_units": 5, "write_capacity_units": 4}
        }),
    );
    assert!(!is_tool_error(&resp), "flip to PROVISIONED failed: {resp}");

    let resp = call_tool(
        &mut child,
        5,
        "describe_table",
        json!({"table_name": "Flip", "raw": true}),
    );
    let content = tool_content(&resp);
    assert_eq!(
        content["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"],
        5
    );
    assert_eq!(
        content["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"],
        4
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_table_table_class() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Classy",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );

    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({"table_name": "Classy", "table_class": "STANDARD_INFREQUENT_ACCESS"}),
    );
    assert!(!is_tool_error(&resp), "table class update failed: {resp}");

    let resp = call_tool(
        &mut child,
        3,
        "describe_table",
        json!({"table_name": "Classy", "raw": true}),
    );
    let content = tool_content(&resp);
    assert_eq!(
        content["Table"]["TableClassSummary"]["TableClass"],
        "STANDARD_INFREQUENT_ACCESS"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_on_demand_throughput_create_and_update() {
    // Ceilings set at creation survive to describe_table, and an update
    // replaces them. Snake_case keys on create, PascalCase on update, so
    // both spellings are pinned.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Ceilings",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST",
            "on_demand_throughput": {"max_read_request_units": 20, "max_write_request_units": 15}
        }),
    );
    assert!(!is_tool_error(&resp), "create failed: {resp}");

    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "Ceilings", "raw": true}),
    );
    let content = tool_content(&resp);
    assert_eq!(
        content["Table"]["OnDemandThroughput"]["MaxReadRequestUnits"],
        20
    );
    assert_eq!(
        content["Table"]["OnDemandThroughput"]["MaxWriteRequestUnits"],
        15
    );

    let resp = call_tool(
        &mut child,
        3,
        "update_table",
        json!({
            "table_name": "Ceilings",
            "on_demand_throughput": {"MaxReadRequestUnits": 40, "MaxWriteRequestUnits": 30}
        }),
    );
    assert!(!is_tool_error(&resp), "update failed: {resp}");

    let resp = call_tool(
        &mut child,
        4,
        "describe_table",
        json!({"table_name": "Ceilings", "raw": true}),
    );
    let content = tool_content(&resp);
    assert_eq!(
        content["Table"]["OnDemandThroughput"]["MaxReadRequestUnits"],
        40
    );
    assert_eq!(
        content["Table"]["OnDemandThroughput"]["MaxWriteRequestUnits"],
        30
    );

    // The default agent-friendly view surfaces the config fields too.
    let resp = call_tool(
        &mut child,
        5,
        "describe_table",
        json!({"table_name": "Ceilings"}),
    );
    let content = tool_content(&resp);
    assert_eq!(content["billing_mode"], "PAY_PER_REQUEST");
    assert_eq!(content["table_class"], "STANDARD");
    assert_eq!(
        content["on_demand_throughput"]["max_read_request_units"],
        40
    );
    assert_eq!(
        content["on_demand_throughput"]["max_write_request_units"],
        30
    );
    assert!(
        content.get("provisioned_throughput").is_none(),
        "zeroed provisioned throughput should not appear for an on-demand table: {content}"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_on_demand_throughput_semantics_over_mcp() {
    // Wire-level pin of the engine semantics captured from real DynamoDB
    // (eu-west-2, 2026-07-24): the billing gate on both tools with each
    // operation's exact wording, member-wise merge, and -1 removal with the
    // response echoing the -1 while describe shows the post-removal state.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // Create-side gate, on the default (PROVISIONED) billing mode.
    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Gated",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "on_demand_throughput": {"max_read_request_units": 10}
        }),
    );
    assert_validation_tool_error(
        &resp,
        "One or more parameter values were invalid: MaxReadRequestUnits for \
         OnDemandThroughput cannot be specified when table BillingMode is PROVISIONED.",
    );

    // Update-side gate, with its own wording.
    call_tool(
        &mut child,
        2,
        "create_table",
        json!({
            "table_name": "Gated",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    let resp = call_tool(
        &mut child,
        3,
        "update_table",
        json!({"table_name": "Gated", "on_demand_throughput": {"max_write_request_units": 10}}),
    );
    assert_validation_tool_error(
        &resp,
        "One or more parameter values were invalid: MaxWriteRequestUnits for \
         OnDemandThroughput cannot be specified when the table BillingMode is PROVISIONED",
    );

    // Merge and -1 removal on an on-demand table.
    call_tool(
        &mut child,
        4,
        "update_table",
        json!({
            "table_name": "Gated",
            "billing_mode": "PAY_PER_REQUEST",
            "on_demand_throughput": {"max_read_request_units": 20, "max_write_request_units": 15}
        }),
    );
    let resp = call_tool(
        &mut child,
        5,
        "update_table",
        json!({"table_name": "Gated", "on_demand_throughput": {"max_read_request_units": 40}}),
    );
    assert!(!is_tool_error(&resp), "merge update failed: {resp}");

    let resp = call_tool(
        &mut child,
        6,
        "update_table",
        json!({"table_name": "Gated", "on_demand_throughput": {"max_read_request_units": -1}}),
    );
    assert!(!is_tool_error(&resp), "-1 removal failed: {resp}");
    let content = tool_content(&resp);
    assert_eq!(
        content["TableDescription"]["OnDemandThroughput"]["MaxReadRequestUnits"],
        -1
    );
    assert_eq!(
        content["TableDescription"]["OnDemandThroughput"]["MaxWriteRequestUnits"],
        15
    );

    let resp = call_tool(
        &mut child,
        7,
        "describe_table",
        json!({"table_name": "Gated", "raw": true}),
    );
    let content = tool_content(&resp);
    assert!(content["Table"]["OnDemandThroughput"]["MaxReadRequestUnits"].is_null());
    assert_eq!(
        content["Table"]["OnDemandThroughput"]["MaxWriteRequestUnits"],
        15
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_malformed_on_demand_throughput_rejected() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // create_table branch
    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "BadCeilings",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "on_demand_throughput": 5
        }),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "InvalidOnDemandThroughput");
    assert!(
        content["message"]
            .as_str()
            .unwrap()
            .contains("Invalid on_demand_throughput"),
        "unexpected message: {content}"
    );

    // update_table branch, against a table that does exist
    call_tool(
        &mut child,
        2,
        "create_table",
        json!({
            "table_name": "GoodCeilings",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    let resp = call_tool(
        &mut child,
        3,
        "update_table",
        json!({"table_name": "GoodCeilings", "on_demand_throughput": "high"}),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "InvalidOnDemandThroughput");
    assert!(
        content["message"]
            .as_str()
            .unwrap()
            .contains("Invalid on_demand_throughput"),
        "unexpected message: {content}"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_table_throughput_interaction_rules() {
    // The shared action's billing/throughput consistency rules, reached
    // through the MCP wiring: PT on an on-demand table is rejected, a switch
    // to PROVISIONED needs PT, PascalCase PT keys land, and a same-values
    // update is rejected.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Rules",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({"table_name": "Rules", "billing_mode": "PAY_PER_REQUEST"}),
    );
    assert!(
        !is_tool_error(&resp),
        "flip to PAY_PER_REQUEST failed: {resp}"
    );

    let resp = call_tool(
        &mut child,
        3,
        "update_table",
        json!({
            "table_name": "Rules",
            "provisioned_throughput": {"read_capacity_units": 5, "write_capacity_units": 5}
        }),
    );
    assert_validation_tool_error(
        &resp,
        "One or more parameter values were invalid: Neither ReadCapacityUnits nor \
         WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST",
    );

    let resp = call_tool(
        &mut child,
        4,
        "update_table",
        json!({"table_name": "Rules", "billing_mode": "PROVISIONED"}),
    );
    assert_validation_tool_error(
        &resp,
        "One or more parameter values were invalid: \
         ProvisionedThroughput must be specified when BillingMode is PROVISIONED",
    );

    // PascalCase throughput keys are accepted too, and the values land.
    let resp = call_tool(
        &mut child,
        5,
        "update_table",
        json!({
            "table_name": "Rules",
            "billing_mode": "PROVISIONED",
            "provisioned_throughput": {"ReadCapacityUnits": 6, "WriteCapacityUnits": 2}
        }),
    );
    assert!(!is_tool_error(&resp), "flip to PROVISIONED failed: {resp}");
    let resp = call_tool(
        &mut child,
        6,
        "describe_table",
        json!({"table_name": "Rules", "raw": true}),
    );
    let content = tool_content(&resp);
    assert_eq!(
        content["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"],
        6
    );
    assert_eq!(
        content["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"],
        2
    );

    let resp = call_tool(
        &mut child,
        7,
        "update_table",
        json!({
            "table_name": "Rules",
            "provisioned_throughput": {"read_capacity_units": 6, "write_capacity_units": 2}
        }),
    );
    assert!(
        is_tool_error(&resp),
        "same-values update should fail: {resp}"
    );
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "ValidationException");
    assert!(
        content["message"]
            .as_str()
            .unwrap()
            .contains("The provisioned throughput for the table will not change"),
        "unexpected message: {content}"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_table_malformed_provisioned_throughput_rejected() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Malformed",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({"table_name": "Malformed", "provisioned_throughput": 5}),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "InvalidProvisionedThroughput");
    assert!(
        content["message"]
            .as_str()
            .unwrap()
            .contains("Invalid provisioned_throughput"),
        "unexpected message: {content}"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_table_partial_provisioned_throughput_rejected() {
    // A partial throughput object normalises the missing member to an
    // explicit null, and the shared action rejects it as a null member.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Partial",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({
            "table_name": "Partial",
            "provisioned_throughput": {"read_capacity_units": 9}
        }),
    );
    assert_validation_tool_error(
        &resp,
        "1 validation error detected: Value null at \
         'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: \
         Member must not be null",
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_table_invalid_table_class_rejected() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "BadClass",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({"table_name": "BadClass", "table_class": "EXPRESS"}),
    );
    assert_validation_tool_error(
        &resp,
        "1 validation error detected: Value 'EXPRESS' at 'tableClass' failed to satisfy \
         constraint: Member must satisfy enum value set: \
         [STANDARD, STANDARD_INFREQUENT_ACCESS]",
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_table_invalid_billing_mode_rejected() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "BadFlip",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );

    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({"table_name": "BadFlip", "billing_mode": "ON_DEMAND"}),
    );
    assert_validation_tool_error(
        &resp,
        "1 validation error detected: Value 'ON_DEMAND' at 'billingMode' \
         failed to satisfy constraint: Member must satisfy enum value set: \
         [PROVISIONED, PAY_PER_REQUEST]",
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_create_table_provisioned_without_throughput_rejected() {
    // Explicit PROVISIONED requires throughput, exactly as on real DynamoDB.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "NoThroughput",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PROVISIONED"
        }),
    );
    assert_validation_tool_error(
        &resp,
        "One or more parameter values were invalid: ReadCapacityUnits and \
         WriteCapacityUnits must both be specified when BillingMode is PROVISIONED",
    );

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

    // Get item again: should be empty
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
fn vector_index_round_trips_through_mcp_alone() {
    // An agent must be able to do the whole thing without a wire client: create
    // the index, write to it, and search it. Before this the create tool had no
    // vector field at all, so a request carrying one was silently dropped and
    // the table came back without the index.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "AgentVecs",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST",
            "vector_indexes": [{
                "index_name": "vix",
                "vector_attribute": {"attribute_name": "emb"},
                "projection": {"projection_type": "ALL"},
                "dimensions": 3,
                "distance_function": "COSINE"
            }]
        }),
    );

    // The control plane reflects it, so the drop is really gone.
    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "AgentVecs"}),
    );
    let described = tool_content(&resp);
    let vixs = described["vector_indexes"]
        .as_array()
        .expect("the agent-facing view reports the index");
    assert_eq!(vixs.len(), 1);
    assert_eq!(vixs[0]["index_name"], "vix");
    assert_eq!(vixs[0]["vector_attribute"]["attribute_name"], "emb");
    assert_eq!(vixs[0]["dimensions"], 3);
    assert_eq!(vixs[0]["distance_function"], "COSINE");

    for (pk, emb) in [("a", ["1", "0", "0"]), ("b", ["0", "1", "0"])] {
        call_tool(
            &mut child,
            10,
            "put_item",
            json!({
                "table_name": "AgentVecs",
                "item": {
                    "pk": {"S": pk},
                    "emb": {"L": emb.iter().map(|n| json!({"N": n})).collect::<Vec<_>>()}
                }
            }),
        );
    }

    let resp = call_tool(
        &mut child,
        20,
        "search_vectors",
        json!({
            "table_name": "AgentVecs",
            "index_name": "vix",
            "search_vector": [{"N": "1"}, {"N": "0"}, {"N": "0"}],
            "top_k": 1
        }),
    );
    let content = tool_content(&resp);
    let results = content["SearchResults"].as_array().expect("SearchResults");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["Item"]["pk"]["S"], "a");
    // COSINE is a distance, so the self match scores 0.
    assert_eq!(results[0]["Score"], 0.0);
    // The vector attribute is excluded from the projection by default.
    assert!(results[0]["Item"].get("emb").is_none());
}

#[test]
fn a_fractional_dimension_count_truncates_through_mcp_as_it_does_on_the_wire() {
    // The wire path truncates and clamps Dimensions before the typed parse.
    // The MCP surface names the field in snake_case, so the normalisation has
    // to recognise both spellings or an agent is refused a value a wire client
    // gets accepted.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "AgentFrac",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST",
            "vector_indexes": [{
                "index_name": "vix",
                "vector_attribute": {"attribute_name": "emb"},
                "projection": {"projection_type": "ALL"},
                "dimensions": 3.7,
                "distance_function": "COSINE"
            }]
        }),
    );

    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "AgentFrac"}),
    );
    let described = tool_content(&resp);
    let vixs = described["vector_indexes"]
        .as_array()
        .expect("index created");
    assert_eq!(vixs.len(), 1);
    assert_eq!(
        vixs[0]["dimensions"], 3,
        "3.7 truncates to 3, as on the wire"
    );
}

#[test]
fn an_empty_vector_attribute_name_is_refused_through_mcp_too() {
    // The request-model constraints are enforced by the raw request's
    // Deserialize, which the MCP surface does not go through, so deserialising
    // alone let an agent create an index a wire client could not: an empty
    // vector attribute name reaching ACTIVE and never holding a row.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "McpEmptyAttr",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST",
            "vector_indexes": [{
                "index_name": "vix",
                "vector_attribute": {"attribute_name": ""},
                "projection": {"projection_type": "ALL"},
                "dimensions": 3,
                "distance_function": "COSINE"
            }]
        }),
    );
    let body = serde_json::to_string(&resp).unwrap();
    assert!(
        body.contains("vectorAttribute.attributeName")
            && body.contains("length greater than or equal to 1"),
        "expected the wire request-model error: {body}"
    );

    // And the table is not created.
    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "McpEmptyAttr"}),
    );
    let body = serde_json::to_string(&resp).unwrap();
    assert!(
        body.contains("not found") || body.contains("ResourceNotFound"),
        "got {body}"
    );
}

#[test]
fn vector_index_updates_work_in_the_shape_the_tool_documents() {
    // The tool description names snake_case fields, as the rest of the surface
    // does, but the parser read the wire spelling alone, so every documented
    // shape parsed into an empty update and was refused for carrying no action.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "McpVecUpd",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST"
        }),
    );

    // Add one in the documented shape.
    call_tool(
        &mut child,
        2,
        "update_table",
        json!({
            "table_name": "McpVecUpd",
            "vector_index_updates": [{"create": {
                "index_name": "vix",
                "vector_attribute": {"attribute_name": "emb"},
                "projection": {"projection_type": "ALL"},
                "dimensions": 3,
                "distance_function": "COSINE"
            }}]
        }),
    );
    let resp = call_tool(
        &mut child,
        3,
        "describe_table",
        json!({"table_name": "McpVecUpd"}),
    );
    let vixs = tool_content(&resp)["vector_indexes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(vixs.len(), 1, "create through the documented shape");
    assert_eq!(vixs[0]["index_name"], "vix");

    // Remove it in the documented shape. The index is still allocating, so the
    // engine answers the phase refusal rather than removing it, and that answer
    // is what proves the shape parsed: a delete that carried no recognised
    // action is refused for having no action at all, naming neither the table
    // nor the index. Removal itself is pinned in the wire tests, where the
    // clock can be driven past the phase instead of waited out.
    let resp = call_tool(
        &mut child,
        4,
        "update_table",
        json!({
            "table_name": "McpVecUpd",
            "vector_index_updates": [{"delete": {"index_name": "vix"}}]
        }),
    );
    assert!(is_tool_error(&resp), "{resp}");
    let body = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        body.contains("Table: McpVecUpd Index: vix"),
        "the delete shape should have parsed and reached the phase check, got {body}"
    );
}

#[test]
fn an_index_added_through_mcp_reports_the_wire_s_creation_lifecycle() {
    // An agent sees the same window a wire client does, because both surfaces
    // run the same actions against the same engine. Only the refusing half is
    // asserted here: the server holds a real clock, so the far side of the
    // window belongs to the in-process tests that can drive one.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "McpVecLife",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST"
        }),
    );
    call_tool(
        &mut child,
        2,
        "put_item",
        json!({
            "table_name": "McpVecLife",
            "item": {"pk": {"S": "a"}, "emb": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}}
        }),
    );
    call_tool(
        &mut child,
        3,
        "update_table",
        json!({
            "table_name": "McpVecLife",
            "vector_index_updates": [{"create": {
                "index_name": "vix",
                "vector_attribute": {"attribute_name": "emb"},
                "projection": {"projection_type": "ALL"},
                "dimensions": 3,
                "distance_function": "COSINE"
            }}]
        }),
    );

    let resp = call_tool(
        &mut child,
        4,
        "describe_table",
        json!({"table_name": "McpVecLife"}),
    );
    let described = tool_content(&resp);
    let vixs = described["vector_indexes"].as_array().cloned().unwrap();
    assert_eq!(vixs[0]["index_status"], "CREATING");
    // Present, either way round: which side of the allocation boundary this
    // lands on is a real clock's business, and both halves are pinned against a
    // driven one in the wire tests. The field being there at all is the parity
    // claim, since the flattened view used to omit it entirely.
    assert!(
        vixs[0]["backfilling"].is_boolean(),
        "expected Backfilling reported while CREATING, got {}",
        described
    );

    let resp = call_tool(
        &mut child,
        5,
        "search_vectors",
        json!({
            "table_name": "McpVecLife",
            "index_name": "vix",
            "search_vector": [{"N": "1"}, {"N": "0"}, {"N": "0"}],
            "top_k": 5
        }),
    );
    assert!(is_tool_error(&resp), "{resp}");
    let body = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        body.contains("Cannot search backfilling vector index: vix"),
        "expected the wire's own refusal, got {body}"
    );

    // And the table cannot be dropped underneath it, with the wire's own
    // envelope. Cancelling and the far side of the window need the clock
    // driven, so they stay with the wire tests rather than being waited out
    // here.
    let resp = call_tool(
        &mut child,
        6,
        "delete_table",
        json!({"table_name": "McpVecLife"}),
    );
    assert!(is_tool_error(&resp), "{resp}");
    let body = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        body.contains("Cannot delete table while indexes are being created, updated, or deleted."),
        "expected the wire's own refusal, got {body}"
    );
}

#[test]
fn an_index_read_back_can_be_fed_straight_to_create_table() {
    // Cloning a table is the obvious thing an agent does with describe_table,
    // so what it reports has to be what create_table accepts. A flattened
    // vector_attribute read back as a bare string failed to deserialise.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let create = json!({
        "table_name": "McpClone",
        "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
        "attribute_definitions": [
            {"attribute_name": "pk", "attribute_type": "S"},
            {"attribute_name": "tenant", "attribute_type": "S"}
        ],
        "billing_mode": "PAY_PER_REQUEST",
        "vector_indexes": [{
            "index_name": "vix",
            "vector_attribute": {"attribute_name": "emb"},
            "search_schema": [
                {"attribute_name": "tenant", "search_schema_element_type": "HASH"}
            ],
            "projection": {"projection_type": "ALL"},
            "dimensions": 3,
            "distance_function": "COSINE"
        }]
    });
    call_tool(&mut child, 1, "create_table", create);

    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "McpClone"}),
    );
    let described = tool_content(&resp);
    let vixs = described["vector_indexes"].as_array().unwrap().clone();

    // Fed back verbatim. Rebuilding it by hand here is what let the projection
    // and search schema shapes drift without the test noticing.
    let cloned = vixs.clone();
    let resp = call_tool(
        &mut child,
        3,
        "create_table",
        json!({
            "table_name": "McpCloneCopy",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [
                {"attribute_name": "pk", "attribute_type": "S"},
                {"attribute_name": "tenant", "attribute_type": "S"}
            ],
            "billing_mode": "PAY_PER_REQUEST",
            "vector_indexes": cloned
        }),
    );
    let body = serde_json::to_string(&resp).unwrap();
    assert!(
        !body.contains("Invalid vector_indexes"),
        "clone was refused: {body}"
    );

    let resp = call_tool(
        &mut child,
        4,
        "describe_table",
        json!({"table_name": "McpCloneCopy"}),
    );
    let copy = tool_content(&resp)["vector_indexes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(copy.len(), 1, "the clone carries the index");
    assert_eq!(copy[0]["vector_attribute"]["attribute_name"], "emb");
    assert_eq!(copy[0]["dimensions"], 3);
}

#[test]
fn vector_errors_through_mcp_are_the_wire_errors() {
    // The tools are thin wrappers over the same facade the wire surface uses,
    // so an agent sees the captured strings rather than an MCP paraphrase.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    // A vector index on a provisioned table is refused, as on the wire.
    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "AgentVecBad",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PROVISIONED",
            "provisioned_throughput": {"read_capacity_units": 1, "write_capacity_units": 1},
            "vector_indexes": [{
                "index_name": "vix",
                "vector_attribute": {"attribute_name": "emb"},
                "projection": {"projection_type": "ALL"},
                "dimensions": 3,
                "distance_function": "COSINE"
            }]
        }),
    );
    let body = serde_json::to_string(&resp).unwrap();
    assert!(
        body.contains("PAY_PER_REQUEST"),
        "a provisioned table must refuse a vector index: {body}"
    );

    call_tool(
        &mut child,
        2,
        "create_table",
        json!({
            "table_name": "AgentPlain",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST"
        }),
    );

    // Searching a table with no vector index reports the index missing.
    let resp = call_tool(
        &mut child,
        3,
        "search_vectors",
        json!({
            "table_name": "AgentPlain",
            "index_name": "nope",
            "search_vector": [{"N": "1"}],
            "top_k": 1
        }),
    );
    let body = serde_json::to_string(&resp).unwrap();
    assert!(
        body.contains("does not have the specified index"),
        "expected the captured missing-index string: {body}"
    );
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

// ---------------------------------------------------------------------------
// Request-validation parity with the HTTP surface
// ---------------------------------------------------------------------------

/// The enveloped rejection PutItem and UpdateItem return for {"NULL": false},
/// shared verbatim with tests/http_server.rs.
const NULL_FALSE_ENVELOPED: &str = "1 validation error detected: \
     One or more parameter values were invalid: \
     Null attribute value types must have the value of true";

/// The bare rejection every other operation returns for {"NULL": false},
/// shared verbatim with tests/http_server.rs.
const NULL_FALSE_BARE: &str = "One or more parameter values were invalid: \
     Null attribute value types must have the value of true";

/// The enveloped rejection UpdateItem returns for a duplicate string set in
/// ExpressionAttributeValues, shared verbatim with tests/http_server.rs.
// UpdateItem refuses it in the current layer's words, with no
// ExpressionAttributeValues wrapper (eu-west-2, us-east-1, eu-west-1 and us-west-2, 2026-10-09).
const DUPLICATE_SET_IN_EAV_ENVELOPED: &str = "1 validation error detected: \
     One or more parameter values were invalid: \
     Input collection [\"a\", \"b\", \"a\"] contains duplicates.";

/// The bare rejection UpdateItem returns for a SET on a key attribute, shared
/// verbatim with tests/http_server.rs.
const CANNOT_UPDATE_KEY_BARE: &str = "One or more parameter values were invalid: \
     Cannot update attribute pk. This attribute is part of the key";

/// Create a hash-only table for the validation-parity tests below.
fn create_parity_table(child: &mut std::process::Child) {
    let resp = call_tool(
        child,
        1,
        "create_table",
        json!({
            "table_name": "Parity",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    assert!(!is_tool_error(&resp));
}

/// Assert a ValidationException tool error with an exact message, and that no
/// internal marker or serde position suffix leaked into the payload.
fn assert_validation_tool_error(resp: &Value, expected_message: &str) {
    assert!(is_tool_error(resp), "expected a tool error: {resp}");
    let raw = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        !raw.contains("VALIDATION") && !raw.contains(" at line "),
        "internal marker or serde position leaked: {raw}"
    );
    let content = tool_content(resp);
    assert_eq!(content["error_type"], "ValidationException");
    assert_eq!(content["message"], expected_message);
}

#[test]
fn test_put_item_null_false_enveloped() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    create_parity_table(&mut child);

    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({
            "table_name": "Parity",
            "item": {"pk": {"S": "k1"}, "flag": {"NULL": false}}
        }),
    );
    assert_validation_tool_error(&resp, NULL_FALSE_ENVELOPED);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_get_item_null_false_in_key_bare() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    create_parity_table(&mut child);

    let resp = call_tool(
        &mut child,
        2,
        "get_item",
        json!({
            "table_name": "Parity",
            "key": {"pk": {"NULL": false}}
        }),
    );
    assert_validation_tool_error(&resp, NULL_FALSE_BARE);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_batch_write_item_null_false_bare() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    create_parity_table(&mut child);

    let resp = call_tool(
        &mut child,
        2,
        "batch_write_item",
        json!({
            "request_items": {
                "Parity": [
                    {"PutRequest": {"Item": {"pk": {"S": "k1"}, "flag": {"NULL": false}}}}
                ]
            }
        }),
    );
    assert_validation_tool_error(&resp, NULL_FALSE_BARE);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_transact_write_items_null_false_bare() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    create_parity_table(&mut child);

    let resp = call_tool(
        &mut child,
        2,
        "transact_write_items",
        json!({
            "transact_items": [
                {"Put": {"TableName": "Parity", "Item": {"pk": {"S": "k1"}, "flag": {"NULL": false}}}}
            ]
        }),
    );
    assert_validation_tool_error(&resp, NULL_FALSE_BARE);

    drop(child.stdin.take());
    let _ = child.wait();
}

/// A transaction write with every kind of action, expression names and values,
/// a token and a capacity mode goes through the tool, and a second call with
/// the same token succeeds without writing again.
#[test]
fn the_transaction_write_tool_runs_every_action_and_replays_its_token() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    create_parity_table(&mut child);

    for (id, pk) in (2..).zip(["update", "delete", "check"]) {
        let resp = call_tool(
            &mut child,
            id,
            "put_item",
            json!({"table_name": "Parity", "item": {"pk": {"S": pk}, "n": {"N": "1"}}}),
        );
        assert!(!is_tool_error(&resp), "{resp}");
    }

    let args = json!({
        "transact_items": [
            {"Put": {
                "TableName": "Parity",
                "Item": {"pk": {"S": "put"}, "n": {"N": "7"}},
                "ConditionExpression": "attribute_not_exists(#p)",
                "ExpressionAttributeNames": {"#p": "pk"}
            }},
            {"Update": {
                "TableName": "Parity",
                "Key": {"pk": {"S": "update"}},
                "UpdateExpression": "SET #n = #n + :one",
                "ConditionExpression": "#n = :was",
                "ExpressionAttributeNames": {"#n": "n"},
                "ExpressionAttributeValues": {":one": {"N": "1"}, ":was": {"N": "1"}}
            }},
            {"Delete": {
                "TableName": "Parity",
                "Key": {"pk": {"S": "delete"}},
                "ConditionExpression": "#n = :was",
                "ExpressionAttributeNames": {"#n": "n"},
                "ExpressionAttributeValues": {":was": {"N": "1"}}
            }},
            {"ConditionCheck": {
                "TableName": "Parity",
                "Key": {"pk": {"S": "check"}},
                "ConditionExpression": "attribute_exists(#p) AND #n = :was",
                "ExpressionAttributeNames": {"#p": "pk", "#n": "n"},
                "ExpressionAttributeValues": {":was": {"N": "1"}}
            }}
        ],
        "client_request_token": "round-trip-token",
        "return_consumed_capacity": "TOTAL"
    });
    let first = call_tool(&mut child, 10, "transact_write_items", args.clone());
    assert!(!is_tool_error(&first), "{first}");
    let first = tool_content(&first);
    assert_eq!(
        first["ConsumedCapacity"],
        json!([{"TableName": "Parity", "CapacityUnits": 8.0, "WriteCapacityUnits": 8.0}]),
        "{first}"
    );

    let replay = call_tool(&mut child, 11, "transact_write_items", args);
    assert!(!is_tool_error(&replay), "{replay}");
    // The replay is billed as a transactional read of the same items.
    assert_eq!(
        tool_content(&replay)["ConsumedCapacity"],
        json!([{"TableName": "Parity", "CapacityUnits": 8.0, "ReadCapacityUnits": 8.0}])
    );

    let stored = |child: &mut std::process::Child, id: u64, pk: &str| {
        let resp = call_tool(
            child,
            id,
            "get_item",
            json!({"table_name": "Parity", "key": {"pk": {"S": pk}}, "consistent_read": true}),
        );
        assert!(!is_tool_error(&resp), "{resp}");
        tool_content(&resp)["Item"].clone()
    };
    assert_eq!(
        stored(&mut child, 12, "put"),
        json!({"pk": {"S": "put"}, "n": {"N": "7"}})
    );
    assert_eq!(
        stored(&mut child, 13, "update"),
        json!({"pk": {"S": "update"}, "n": {"N": "2"}})
    );
    assert_eq!(stored(&mut child, 14, "delete"), Value::Null);
    assert_eq!(
        stored(&mut child, 15, "check"),
        json!({"pk": {"S": "check"}, "n": {"N": "1"}})
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// The transaction write tool answers its member checks as the HTTP API does:
/// a table name left out, an empty token ahead of a number refused in an item
/// and ahead of a bad capacity mode, and an item naming no action beside a
/// bad capacity mode (captured eu-west-2 and us-east-1, 2026-11-11).
#[test]
fn the_transaction_write_tool_checks_its_members_as_the_http_api_does() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    create_parity_table(&mut child);

    let cases = [
        (
            json!({"transact_items": [{"Put": {"Item": {"pk": {"S": "a"}}}}]}),
            "1 validation error detected: Value null at 'transactItems.1.member.put.tableName' \
             failed to satisfy constraint: Member must not be null",
        ),
        (
            json!({
                "transact_items": [{"Put": {"TableName": "Parity", "Item": {"pk": {"N": "abc"}}}}],
                "client_request_token": ""
            }),
            "1 validation error detected: Value at 'ClientRequestToken' failed to satisfy \
             constraint: Member must have length greater than or equal to 1",
        ),
        (
            json!({
                "transact_items": [{"Put": {"TableName": "Parity", "Item": {"pk": {"N": "abc"}}}}],
                "client_request_token": "",
                "return_consumed_capacity": "BAD"
            }),
            "1 validation error detected: Value at 'ClientRequestToken' failed to satisfy \
             constraint: Member must have length greater than or equal to 1",
        ),
        (
            json!({
                "transact_items": [{}, {"Put": {"TableName": "x", "Item": {"pk": {"S": "a"}}}}],
                "return_consumed_capacity": "BAD"
            }),
            "2 validation errors detected: Value '' at 'transactItems' failed to satisfy \
             constraint: TransactWriteRequest should contain Delete or Put or Update or \
             ConditionCheck; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy \
             constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
        ),
    ];
    for (id, (args, expected)) in (2..).zip(cases) {
        let resp = call_tool(&mut child, id, "transact_write_items", args);
        assert_validation_tool_error(&resp, expected);
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_item_duplicate_set_in_eav_enveloped() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    create_parity_table(&mut child);

    let resp = call_tool(
        &mut child,
        2,
        "update_item",
        json!({
            "table_name": "Parity",
            "key": {"pk": {"S": "k1"}},
            "update_expression": "SET tags = :t",
            "expression_attribute_values": {":t": {"SS": ["a", "b", "a"]}}
        }),
    );
    assert_validation_tool_error(&resp, DUPLICATE_SET_IN_EAV_ENVELOPED);

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn test_update_item_cannot_update_key_bare() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    create_parity_table(&mut child);

    let resp = call_tool(
        &mut child,
        2,
        "update_item",
        json!({
            "table_name": "Parity",
            "key": {"pk": {"S": "k1"}},
            "update_expression": "SET pk = :v",
            "expression_attribute_values": {":v": {"S": "k2"}}
        }),
    );
    assert_validation_tool_error(&resp, CANNOT_UPDATE_KEY_BARE);

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

    // Delete the table: should auto-snapshot
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

    // List snapshots: should have at least 1
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

    // Scan without explicit limit: should be capped at 2
    let resp = call_tool(&mut child, 20, "scan", json!({"table_name": "Limited"}));
    let content = tool_content(&resp);
    assert_eq!(content["Count"], 2);
    assert!(content["LastEvaluatedKey"].is_object());

    // Query without explicit limit: should be capped at 2
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
    // The response includes Count, ScannedCount, Items: might be close
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

    // Start the MCP server in HTTP mode, again on another port if it loses the
    // race for the first. A fixed token via env keeps the test deterministic and
    // avoids writing to the real per-user config dir.
    let (port, mut child) = spawn_on_free_port("127.0.0.1", |port| {
        let mut command = Command::new(binary);
        command
            .args(["mcp", "--http", "--port", &port.to_string()])
            .env("DYNOXIDE_MCP_AUTH_TOKEN", "test-transport-token")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    });

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
         Authorization: Bearer test-transport-token\r\n\
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

// Regression for CVE-2026-42559 / GHSA-89vp-x53w-74fx (DNS rebinding against
// rmcp's Streamable HTTP transport). src/mcp/mod.rs sets allowed_hosts and
// allowed_origins explicitly; this test locks both in. Asserts on rmcp's
// rejection-reason body so a future 403 emitted from elsewhere can't masquerade
// as the Host or Origin check still working.
#[test]
fn test_http_transport_dns_rebinding_defences() {
    use std::io::Read;

    // Kill the child on every exit path, including panic between spawn and the
    // final assertion. std::process::Child does not kill on drop, so a panicked
    // test would otherwise leak a dynoxide process holding the loopback port and
    // flake subsequent runs.
    struct ChildKiller(std::process::Child);
    impl Drop for ChildKiller {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let binary = env!("CARGO_BIN_EXE_dynoxide");

    let (port, child) = spawn_on_free_port("127.0.0.1", |port| {
        let mut command = Command::new(binary);
        command
            .args(["mcp", "--http", "--port", &port.to_string()])
            // Auth now wraps the transport. Use a fixed token and send it on
            // every request so these assertions still exercise rmcp's
            // Host/Origin checks (which sit behind auth) rather than the 401
            // from the auth layer.
            .env("DYNOXIDE_MCP_AUTH_TOKEN", "dns-rebind-token")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    });
    let _kill_on_drop = ChildKiller(child);

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
    let body = serde_json::to_string(&init_body).unwrap();

    let send = |host: &str, origin: Option<&str>| -> String {
        let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let origin_line = match origin {
            Some(o) => format!("Origin: {o}\r\n"),
            None => String::new(),
        };
        let request = format!(
            "POST /mcp HTTP/1.1\r\n\
             Host: {host}\r\n\
             {origin_line}\
             Authorization: Bearer dns-rebind-token\r\n\
             Content-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).unwrap();
        let mut buf = Vec::new();
        let _ = stream.read_to_end(&mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    };

    // Ordering is load-bearing: with legacy_session_mode=false the Host and
    // Origin checks run before session lookup, so per-request rejection cannot
    // leak state. If legacy_session_mode ever flips, revisit this ordering.
    let loopback = send(&format!("127.0.0.1:{port}"), None);
    assert!(
        loopback.starts_with("HTTP/1.1 2"),
        "Loopback Host should be accepted with 2xx, got: {loopback}"
    );
    assert!(
        loopback.contains("dynoxide"),
        "Loopback response should contain server name, got: {loopback}"
    );

    let foreign_host = send("evil.example.com", None);
    assert!(
        foreign_host.starts_with("HTTP/1.1 403"),
        "Foreign Host should be rejected with 403, got: {foreign_host}"
    );
    assert!(
        foreign_host.contains("Host header is not allowed"),
        "Foreign Host rejection should name the Host check, got: {foreign_host}"
    );

    let foreign_origin = send(
        &format!("127.0.0.1:{port}"),
        Some("https://evil.example.com"),
    );
    assert!(
        foreign_origin.starts_with("HTTP/1.1 403"),
        "Foreign Origin should be rejected with 403, got: {foreign_origin}"
    );
    assert!(
        foreign_origin.contains("Origin header is not allowed"),
        "Foreign Origin rejection should name the Origin check, got: {foreign_origin}"
    );
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

    // Find User entity: should have GSI1, not gs1
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

// ---------------------------------------------------------------------------
// MCP HTTP bearer-token auth (#27) + configurable bind host (#24)
// ---------------------------------------------------------------------------

/// Kills the child on drop, including panic paths (std::process::Child does not
/// kill on drop, so a panicked test would otherwise leak a process holding the
/// loopback port and flake subsequent runs).
struct AuthChild(std::process::Child);
impl Drop for AuthChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// How a freshly spawned server got on.
enum Startup {
    /// It accepts connections and is still running.
    Ready,
    /// It exited before accepting one, which is what a child that lost the
    /// race for its port does.
    Exited,
}

/// Watch `child` for a short while after something has answered on its port.
/// A child that lost the race for the port exits within moments of starting, so
/// one that is still running at the end of this is the one answering.
fn settle(child: &mut std::process::Child) -> Startup {
    for _ in 0..12 {
        if child.try_wait().expect("poll the server").is_some() {
            return Startup::Exited;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    Startup::Ready
}

/// Wait until `child` accepts connections on `addr`, or has exited.
///
/// A connect alone is not enough: if another process took the port between
/// choosing it and the child binding it, the connect reaches that process
/// while the child is on its way to exiting. So the first successful connect
/// is followed by [`settle`]. A child that neither exits nor accepts in 30
/// seconds is killed and fails the test.
fn wait_ready_or_exit(child: &mut std::process::Child, addr: &str) -> Startup {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
        if child.try_wait().expect("poll the server").is_some() {
            return Startup::Exited;
        }
        if std::net::TcpStream::connect(addr).is_ok() {
            return settle(child);
        }
    }
    let _ = child.kill();
    panic!("MCP HTTP server at {addr} did not start within 30 seconds");
}

/// Start the server `build(port)` describes on a port nothing holds, and wait
/// until it accepts connections. Returns the port and the child.
///
/// `free_port` binds a port and releases it before the child binds it, so any
/// other process, another test in the same run included, can take it in
/// between. The child then exits with "Address already in use". That is a lost
/// race, not a failure, so it is started again on a fresh port.
fn spawn_on_free_port(host: &str, build: impl Fn(u16) -> Command) -> (u16, std::process::Child) {
    let mut exits = Vec::new();
    for _ in 0..5 {
        let port = free_port();
        let mut child = build(port).spawn().expect("failed to spawn dynoxide");
        if let Startup::Ready = wait_ready_or_exit(&mut child, &format!("{host}:{port}")) {
            return (port, child);
        }
        exits.push(format!("port {port}: {:?}", child.wait()));
    }
    panic!(
        "the server exited before it accepted a connection on {host} on five different ports \
         (a real startup failure looks like this too, so run it by hand): {exits:?}"
    );
}

/// Spawn `dynoxide mcp --http` on 127.0.0.1 with a fixed token via env, waiting
/// until it accepts connections. The explicit token means no token file is
/// written, so tests never touch the real per-user config dir. Returns the port
/// it ended up on.
fn spawn_authed_mcp(token: &str) -> (u16, AuthChild) {
    spawn_authed_mcp_host(token, "127.0.0.1", &[])
}

/// Like [`spawn_authed_mcp`] but binds an explicit `--host` (e.g. `0.0.0.0`)
/// with optional extra args (e.g. `--allowed-host`). Connections are still made
/// over loopback: a `0.0.0.0` bind includes `127.0.0.1`, so tests reach it the
/// same way regardless of the bind host (the #24 reachability shape).
fn spawn_authed_mcp_host(token: &str, host: &str, extra_args: &[&str]) -> (u16, AuthChild) {
    let (port, child) = spawn_on_free_port("127.0.0.1", |port| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
        command
            .args(["mcp", "--http", "--host", host, "--port", &port.to_string()])
            .args(extra_args)
            .env("DYNOXIDE_MCP_AUTH_TOKEN", token)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    });
    (port, AuthChild(child))
}

#[test]
fn a_server_that_loses_the_race_for_its_port_is_started_again_on_another() {
    // Another process holds the first port offered and accepts connections on
    // it, as whatever took the port would. The child cannot bind it and exits;
    // the connect that reaches the holder must not pass for the child being up.
    let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken = holder.local_addr().unwrap().port();
    let offered = std::cell::RefCell::new(Vec::new());

    let (port, child) = spawn_on_free_port("127.0.0.1", |port| {
        offered.borrow_mut().push(port);
        let bind = if offered.borrow().len() == 1 {
            taken
        } else {
            port
        };
        let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
        command
            .args([
                "mcp",
                "--http",
                "--port",
                &bind.to_string(),
                "--token",
                "tok",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    });
    let _kill = AuthChild(child);

    assert_eq!(
        offered.borrow().len(),
        2,
        "started again after the lost race"
    );
    assert_ne!(port, taken, "the server ended up on a different port");
    let resp = mcp_request(port, &format!("127.0.0.1:{port}"), Some("tok"));
    assert!(
        resp.starts_with("HTTP/1.1 2"),
        "the server on the new port answers, got: {resp}"
    );
}

/// Send an `initialize` POST and return the raw HTTP response. `host` sets the
/// Host header; `auth` adds an `Authorization: Bearer` header when present.
fn mcp_request(connect_port: u16, host: &str, auth: Option<&str>) -> String {
    use std::io::Read;
    let init = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "1.0"}
        }
    });
    let body = serde_json::to_string(&init).unwrap();
    let auth_line = match auth {
        Some(t) => format!("Authorization: Bearer {t}\r\n"),
        None => String::new(),
    };
    let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{connect_port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let request = format!(
        "POST /mcp HTTP/1.1\r\n\
         Host: {host}\r\n\
         {auth_line}\
         Content-Type: application/json\r\n\
         Accept: application/json, text/event-stream\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

/// The body of a raw HTTP response: everything after the blank line.
fn response_body(resp: &str) -> &str {
    resp.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("")
}

/// The status line and headers of a raw HTTP response.
fn response_head(resp: &str) -> &str {
    resp.split_once("\r\n\r\n").map(|(h, _)| h).unwrap_or(resp)
}

/// Run the binary expecting it to exit non-zero before serving; returns
/// (success, stderr). Safe against hangs because the startup guards error
/// before any listener is bound.
fn run_expect_startup_failure(args: &[&str]) -> (bool, String) {
    let binary = env!("CARGO_BIN_EXE_dynoxide");
    let out = Command::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn dynoxide");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn auth_missing_and_wrong_token_return_identical_401() {
    let (port, _srv) = spawn_authed_mcp("the-secret-token");

    let no_token = mcp_request(port, &format!("127.0.0.1:{port}"), None);
    let wrong = mcp_request(port, &format!("127.0.0.1:{port}"), Some("not-the-token"));

    assert!(
        no_token.starts_with("HTTP/1.1 401"),
        "missing token should be 401, got: {no_token}"
    );
    assert!(
        wrong.starts_with("HTTP/1.1 401"),
        "wrong token should be 401, got: {wrong}"
    );
    let lower = no_token.to_lowercase();
    assert!(
        lower.contains("www-authenticate: bearer realm=\"dynoxide-mcp\""),
        "401 should carry the Bearer challenge, got: {no_token}"
    );
    assert!(
        !lower.contains("resource_metadata"),
        "WWW-Authenticate must not include resource_metadata, got: {no_token}"
    );
    // No oracle: missing and wrong tokens yield byte-identical bodies.
    assert_eq!(
        response_body(&no_token),
        response_body(&wrong),
        "401 bodies for missing vs wrong token must be identical"
    );
}

#[test]
fn auth_correct_token_succeeds() {
    let (port, _srv) = spawn_authed_mcp("the-secret-token");
    let resp = mcp_request(port, &format!("127.0.0.1:{port}"), Some("the-secret-token"));
    assert!(
        resp.starts_with("HTTP/1.1 2"),
        "correct token should be 2xx, got: {resp}"
    );
    assert!(resp.contains("dynoxide"), "got: {resp}");
}

#[test]
fn auth_non_bearer_header_is_401() {
    use std::io::Read;
    let (port, _srv) = spawn_authed_mcp("the-secret-token");
    let body = "{}";
    let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let request = format!(
        "POST /mcp HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Authorization: Basic dXNlcjpwYXNz\r\n\
         Content-Type: application/json\r\n\
         Accept: application/json, text/event-stream\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    let resp = String::from_utf8_lossy(&buf);
    assert!(
        resp.starts_with("HTTP/1.1 401"),
        "non-Bearer scheme should be 401, got: {resp}"
    );
}

#[test]
fn auth_does_not_mask_host_check() {
    // A token holder spoofing Host still hits rmcp's 403 (defense-in-depth);
    // an unauthenticated foreign-Host request is rejected by auth first (401).
    let (port, _srv) = spawn_authed_mcp("the-secret-token");

    let with_token = mcp_request(port, "evil.example.com", Some("the-secret-token"));
    assert!(
        with_token.starts_with("HTTP/1.1 403"),
        "foreign Host with valid token should be 403, got: {with_token}"
    );

    let no_token = mcp_request(port, "evil.example.com", None);
    assert!(
        no_token.starts_with("HTTP/1.1 401"),
        "foreign Host with no token should be 401, got: {no_token}"
    );
}

#[test]
fn off_loopback_without_token_fails_to_start() {
    let (ok, stderr) =
        run_expect_startup_failure(&["mcp", "--http", "--host", "0.0.0.0", "--port", "0"]);
    assert!(!ok, "off-loopback bind without a token must fail to start");
    assert!(
        stderr.contains("non-loopback"),
        "error should mention the non-loopback bind, got: {stderr}"
    );
}

#[test]
fn off_loopback_no_auth_fails_to_start() {
    let (ok, stderr) = run_expect_startup_failure(&[
        "mcp",
        "--http",
        "--host",
        "0.0.0.0",
        "--no-auth",
        "--port",
        "0",
    ]);
    assert!(!ok, "--no-auth on a non-loopback bind must fail to start");
    assert!(
        stderr.contains("loopback"),
        "error should explain --no-auth is loopback-only, got: {stderr}"
    );
}

#[test]
fn off_loopback_no_auth_with_token_still_fails() {
    // --no-auth is evaluated before the token, so the loopback-only guard wins
    // even when an explicit token is also supplied.
    let (ok, stderr) = run_expect_startup_failure(&[
        "mcp",
        "--http",
        "--host",
        "0.0.0.0",
        "--no-auth",
        "--token",
        "tok",
        "--port",
        "0",
    ]);
    assert!(
        !ok,
        "--no-auth on a non-loopback bind must fail even when --token is given"
    );
    assert!(
        stderr.contains("loopback"),
        "error should explain --no-auth is loopback-only, got: {stderr}"
    );
}

#[test]
fn no_auth_loopback_starts_and_warns() {
    use std::io::Read;
    let (port, mut child) = spawn_on_free_port("127.0.0.1", |port| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
        command
            .args(["mcp", "--http", "--no-auth", "--port", &port.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        command
    });

    // With auth disabled, an unauthenticated request succeeds.
    let resp = mcp_request(port, &format!("127.0.0.1:{port}"), None);
    assert!(
        resp.starts_with("HTTP/1.1 2"),
        "--no-auth should allow unauthenticated requests, got: {resp}"
    );

    let _ = child.kill();
    let mut stderr = String::new();
    if let Some(mut s) = child.stderr.take() {
        let _ = s.read_to_string(&mut stderr);
    }
    let _ = child.wait();
    assert!(
        stderr.contains("auth disabled"),
        "starting with --no-auth must print a warning, got: {stderr}"
    );
}

#[test]
fn token_flag_beats_env() {
    let (port, child) = spawn_on_free_port("127.0.0.1", |port| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
        command
            .args([
                "mcp",
                "--http",
                "--port",
                &port.to_string(),
                "--token",
                "flag-token",
            ])
            .env("DYNOXIDE_MCP_AUTH_TOKEN", "env-token")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    });
    let _kill = AuthChild(child);

    let with_flag = mcp_request(port, &format!("127.0.0.1:{port}"), Some("flag-token"));
    let with_env = mcp_request(port, &format!("127.0.0.1:{port}"), Some("env-token"));
    let no_token = mcp_request(port, &format!("127.0.0.1:{port}"), None);
    assert!(
        with_flag.starts_with("HTTP/1.1 2"),
        "flag token should win over env, got: {with_flag}"
    );
    assert!(
        with_env.starts_with("HTTP/1.1 401"),
        "env token should be rejected when flag is set, got: {with_env}"
    );
    // The rejected env token must look identical to no token: no oracle.
    assert_eq!(
        response_body(&with_env),
        response_body(&no_token),
        "rejected-token body must match missing-token body"
    );
}

#[test]
fn ipv6_loopback_is_treated_as_loopback() {
    // ::1 is loopback, so a token-less start would auto-generate; we pass a
    // token to keep it deterministic and assert the IPv6 bind accepts.
    let (_port, child) = spawn_on_free_port("[::1]", |port| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
        command
            .args([
                "mcp",
                "--http",
                "--host",
                "::1",
                "--port",
                &port.to_string(),
                "--token",
                "tok",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    });
    let _kill = AuthChild(child);
}

/// Run `dynoxide mcp --http` once in `home`, stop it as soon as it accepts
/// connections and return what it wrote to stderr.
///
/// The token is written before the server binds its port, so a child that
/// loses the race for the port has already used up the first run. A first run
/// therefore starts from an empty home on every attempt. `lose_first_port`
/// makes the first attempt run on a port that is already taken, to force that
/// race.
fn mcp_http_stderr(
    home: &std::path::Path,
    first_run: bool,
    lose_first_port: Option<u16>,
) -> String {
    use std::io::Read;
    let attempts = std::cell::Cell::new(0);
    let (_port, mut child) = spawn_on_free_port("127.0.0.1", |port| {
        attempts.set(attempts.get() + 1);
        if first_run {
            for entry in std::fs::read_dir(home).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    std::fs::remove_dir_all(path).unwrap();
                } else {
                    std::fs::remove_file(path).unwrap();
                }
            }
        }
        let bind = match lose_first_port {
            Some(taken) if attempts.get() == 1 => taken,
            _ => port,
        };
        let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
        command
            .args(["mcp", "--http", "--port", &bind.to_string()])
            .env("HOME", home)
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("DYNOXIDE_MCP_AUTH_TOKEN")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        command
    });
    let _ = child.kill();
    let mut stderr = String::new();
    if let Some(mut s) = child.stderr.take() {
        let _ = s.read_to_string(&mut stderr);
    }
    let _ = child.wait();
    stderr
}

#[test]
fn first_run_generates_token_then_reuses_silently() {
    let home = tempfile::tempdir().unwrap();

    let first = mcp_http_stderr(home.path(), true, None);
    assert!(
        first.contains("Generated an MCP auth token"),
        "first run should print one-time guidance, got: {first}"
    );
    let second = mcp_http_stderr(home.path(), false, None);
    assert!(
        !second.contains("Generated an MCP auth token"),
        "second run should silently reuse the persisted token, got: {second}"
    );
}

#[test]
fn a_first_run_that_loses_its_port_still_prints_the_guidance_on_the_retry() {
    // The child that loses the port has already written the token file, so a
    // retry in the same home would reuse it silently and the one-time guidance
    // would never be seen.
    let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken = holder.local_addr().unwrap().port();
    let home = tempfile::tempdir().unwrap();

    let first = mcp_http_stderr(home.path(), true, Some(taken));
    assert!(
        first.contains("Generated an MCP auth token"),
        "first run should print one-time guidance, got: {first}"
    );
}

#[test]
fn allowed_host_extends_acceptance() {
    let (port, child) = spawn_on_free_port("127.0.0.1", |port| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
        command
            .args([
                "mcp",
                "--http",
                "--port",
                &port.to_string(),
                "--token",
                "tok",
                "--allowed-host",
                "myhost.lan",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    });
    let _kill = AuthChild(child);

    // --allowed-host extends the loopback list, it does not replace it.
    let loopback = mcp_request(port, &format!("127.0.0.1:{port}"), Some("tok"));
    assert!(
        loopback.starts_with("HTTP/1.1 2"),
        "loopback Host must still be accepted when --allowed-host is set, got: {loopback}"
    );
    let allowed = mcp_request(port, "myhost.lan", Some("tok"));
    assert!(
        allowed.starts_with("HTTP/1.1 2"),
        "allowlisted host should be accepted, got: {allowed}"
    );
    let foreign = mcp_request(port, "evil.example.com", Some("tok"));
    assert!(
        foreign.starts_with("HTTP/1.1 403"),
        "non-allowlisted host should still be 403, got: {foreign}"
    );
}

#[test]
fn off_loopback_bind_is_reachable_with_token() {
    // #24: a 0.0.0.0 bind with a token is reachable over loopback and
    // authenticates: the core Docker reachability shape. Existing auth tests
    // all bind the 127.0.0.1 default; this is the only widened-bind happy path.
    let (port, _srv) = spawn_authed_mcp_host("the-secret-token", "0.0.0.0", &[]);
    let resp = mcp_request(port, &format!("127.0.0.1:{port}"), Some("the-secret-token"));
    assert!(
        resp.starts_with("HTTP/1.1 2"),
        "0.0.0.0 bind with a token should accept a loopback-Host request, got: {resp}"
    );
    assert!(resp.contains("dynoxide"), "got: {resp}");
}

#[test]
fn off_loopback_bind_accepts_allowlisted_name() {
    // #24 container-to-container shape: a 0.0.0.0 bind reached by service name
    // needs that name on the Host allowlist via --allowed-host. (Foreign-Host
    // 403 and loopback acceptance are covered by auth_does_not_mask_host_check
    // and allowed_host_extends_acceptance.)
    let (port, _srv) = spawn_authed_mcp_host(
        "the-secret-token",
        "0.0.0.0",
        &["--allowed-host", "dynoxide.test"],
    );
    let resp = mcp_request(
        port,
        &format!("dynoxide.test:{port}"),
        Some("the-secret-token"),
    );
    assert!(
        resp.starts_with("HTTP/1.1 2"),
        "allowlisted by-name Host on a 0.0.0.0 bind should be accepted, got: {resp}"
    );
}

#[test]
fn a_snake_case_create_the_canonicaliser_cannot_parse_is_still_validated() {
    // The canonicaliser rewrites a create action by round-tripping it through
    // `VectorIndex`, and falls back to the caller's raw object when that parse
    // fails. A fractional `dimensions` fails it, because the typed field is a
    // `u32` and normalisation has not run yet. The raw object is snake_case, so
    // the request-model collector, which reads wire spellings alone, reports
    // every member as null, naming five fields the caller did supply. The wire
    // path normalises before it validates; this one has to as well.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "McpVecBypass",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST"
        }),
    );

    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({
            "table_name": "McpVecBypass",
            "vector_index_updates": [{"create": {
                "index_name": "vix",
                "vector_attribute": {"attribute_name": ""},
                "projection": {"projection_type": "ALL"},
                "dimensions": 3.5,
                "distance_function": "COSINE"
            }}]
        }),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "ValidationException");
    let msg = content["message"].as_str().unwrap();
    assert!(
        msg.contains("vectorAttribute.attributeName"),
        "the empty attribute name is the fault worth reporting: {content}"
    );
    assert!(
        !msg.contains("indexName' failed to satisfy constraint: Member must not be null"),
        "a supplied member must not be reported as null: {content}"
    );

    let resp = call_tool(
        &mut child,
        3,
        "describe_table",
        json!({"table_name": "McpVecBypass"}),
    );
    let vixs = tool_content(&resp)["vector_indexes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        vixs.is_empty(),
        "a refused create must not leave an index behind: {vixs:?}"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn a_null_delete_action_is_reported_as_a_missing_action() {
    // The canonicaliser rewrote any delete value, null included, into
    // `{"IndexName": null}`. That is no longer null, so the parser's own null
    // filter stopped skipping it and the entry answered a type error where the
    // wire answers that neither action was supplied.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "McpNullDel",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST"
        }),
    );

    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({
            "table_name": "McpNullDel",
            "vector_index_updates": [{"delete": null}]
        }),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let content = tool_content(&resp);
    assert!(
        content["message"]
            .as_str()
            .unwrap()
            .contains("One of VectorIndexUpdate.Create, VectorIndexUpdate.Delete must not be null"),
        "unexpected message: {content}"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn an_index_without_a_search_schema_clones_without_one() {
    // The read-back defaulted an absent search schema to `[]`, and the clone
    // test fed the read-back straight back to create_table, so a copy gained an
    // empty search schema the original never carried. The sibling clone test
    // covers the populated case; this one covers the absent case it stopped
    // exercising when its fixture gained a schema.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "McpNoSchema",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST",
            "vector_indexes": [{
                "index_name": "vix",
                "vector_attribute": {"attribute_name": "emb"},
                "projection": {"projection_type": "ALL"},
                "dimensions": 3,
                "distance_function": "COSINE"
            }]
        }),
    );
    let resp = call_tool(
        &mut child,
        2,
        "describe_table",
        json!({"table_name": "McpNoSchema"}),
    );
    let vixs = tool_content(&resp)["vector_indexes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(vixs.len(), 1);
    assert!(
        vixs[0].get("search_schema").is_none(),
        "an index created without a search schema must read back without one: {}",
        vixs[0]
    );

    // Feed it back verbatim, the way an agent cloning a table would.
    call_tool(
        &mut child,
        3,
        "create_table",
        json!({
            "table_name": "McpNoSchemaCopy",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST",
            "vector_indexes": vixs
        }),
    );
    let resp = call_tool(
        &mut child,
        4,
        "describe_table",
        json!({"table_name": "McpNoSchemaCopy"}),
    );
    let copied = tool_content(&resp)["vector_indexes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(copied.len(), 1, "the clone carries the index");
    assert!(
        copied[0].get("search_schema").is_none(),
        "the clone must not gain a search schema: {}",
        copied[0]
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn a_create_missing_a_member_is_named_the_way_the_wire_names_it() {
    // The request-model constraints run before the typed parse here, as they do
    // in the raw request's Deserialize. Reversed, a search schema element with
    // no attribute name answered serde's `missing field` where the wire names
    // the member and the path it sits on.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "McpVecOrder",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST"
        }),
    );

    let resp = call_tool(
        &mut child,
        2,
        "update_table",
        json!({
            "table_name": "McpVecOrder",
            "vector_index_updates": [{"create": {
                "index_name": "vix",
                "vector_attribute": {"attribute_name": "emb"},
                "search_schema": [{"search_schema_element_type": "HASH"}],
                "projection": {"projection_type": "ALL"},
                "dimensions": 3,
                "distance_function": "COSINE"
            }}]
        }),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "ValidationException");
    // The wire's own string, verbatim. Its sibling on the wire side is
    // `update_table_search_schema_missing_attribute_name_rejected_at_request_model_layer`,
    // which pins the same literal, so a drift on either surface fails there.
    assert_eq!(
        content["message"],
        "1 validation error detected: Value null at \
         'vectorIndexUpdates.1.member.create.searchSchema.1.member.attributeName' failed to \
         satisfy constraint: Member must not be null"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn an_agents_negative_dimensions_are_echoed_the_way_the_wire_echoes_them() {
    // The collector reads the raw value, so it must run before normalisation.
    // Reversed, the clamp lands first and the agent is told its -1 was a 0,
    // which is a different string from the one a wire client gets for the same
    // mistake. The wire side pins the same text in
    // `create_table_negative_dimensions_reports_the_value_as_given`.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "McpNegDims",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST",
            "vector_indexes": [{
                "index_name": "vix",
                "vector_attribute": {"attribute_name": "emb"},
                "projection": {"projection_type": "ALL"},
                "dimensions": -1,
                "distance_function": "COSINE"
            }]
        }),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let content = tool_content(&resp);
    assert!(
        content["message"]
            .as_str()
            .unwrap()
            .contains("Value '-1' at 'vectorIndexes.1.member.dimensions'"),
        "the raw value is what gets echoed: {content}"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

#[test]
fn an_agents_search_is_held_to_the_wire_request_model() {
    // The tool built SearchVectorsRequest field by field, which skips the
    // hand-written Deserialize where the topK, SearchVector and IndexName
    // bounds live. Nothing was wrongly accepted, but an agent got a different
    // string than a wire client for the same mistake, on the one vector tool
    // the parity work missed.
    let mut child = spawn_mcp();
    init_mcp(&mut child);

    call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "McpSearchRM",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}],
            "billing_mode": "PAY_PER_REQUEST",
            "vector_indexes": [{
                "index_name": "vix",
                "vector_attribute": {"attribute_name": "emb"},
                "projection": {"projection_type": "ALL"},
                "dimensions": 3,
                "distance_function": "COSINE"
            }]
        }),
    );

    // topK below the floor: the enveloped member-path constraint, not the
    // operation-layer range message.
    let resp = call_tool(
        &mut child,
        2,
        "search_vectors",
        json!({
            "table_name": "McpSearchRM", "index_name": "vix",
            "search_vector": [{"N": "1"}, {"N": "1"}, {"N": "0"}], "top_k": 0
        }),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let msg = tool_content(&resp)["message"].as_str().unwrap().to_string();
    assert!(
        msg.contains("at 'TopK' failed to satisfy constraint"),
        "topK must answer the request-model constraint: {msg}"
    );

    // An empty search vector: the no-echo 'SearchVector' member-path form.
    let resp = call_tool(
        &mut child,
        3,
        "search_vectors",
        json!({
            "table_name": "McpSearchRM", "index_name": "vix",
            "search_vector": [], "top_k": 5
        }),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let msg = tool_content(&resp)["message"].as_str().unwrap().to_string();
    assert!(
        msg.contains("'SearchVector' failed to satisfy constraint"),
        "an empty vector must answer the request-model constraint: {msg}"
    );

    // An index name under the floor is a length constraint, not a lookup miss.
    let resp = call_tool(
        &mut child,
        4,
        "search_vectors",
        json!({
            "table_name": "McpSearchRM", "index_name": "ab",
            "search_vector": [{"N": "1"}, {"N": "1"}, {"N": "0"}], "top_k": 5
        }),
    );
    assert!(is_tool_error(&resp), "expected a tool error: {resp}");
    let msg = tool_content(&resp)["message"].as_str().unwrap().to_string();
    assert!(
        msg.contains("'IndexName' failed to satisfy constraint"),
        "a short index name must answer the length constraint: {msg}"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// Import report tests
// ---------------------------------------------------------------------------

/// The instructions sentence and the `import` object are only present when
/// the database was populated by `dynoxide import` in this process.
#[test]
fn test_no_import_report_without_import() {
    let mut child = spawn_mcp();
    let resp = init_mcp(&mut child);

    let instructions = resp["result"]["instructions"].as_str().unwrap();
    assert!(
        !instructions.contains("populated by dynoxide import"),
        "instructions must not mention an import that did not happen"
    );

    let resp = call_tool(&mut child, 1, "get_database_info", json!({}));
    let content = tool_content(&resp);
    assert!(
        content.get("import").is_none(),
        "import must be absent when nothing was imported: {content}"
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// A report with no notices still tells the agent where the data came from.
#[test]
fn test_import_report_with_no_notices_in_instructions() {
    use dynoxide::mcp::{ImportReport, McpConfig, McpServer};
    use rmcp::ServerHandler;
    use std::sync::Arc;

    let db = dynoxide::Database::memory().unwrap();
    let report = ImportReport {
        tables: 2,
        items: 40,
        skipped: 0,
        notices: Vec::new(),
        exposures_accepted: false,
    };
    let config = McpConfig {
        import: Some(report),
        ..Default::default()
    };
    let server = McpServer::with_config(Arc::new(db), config);

    let instructions = server.get_info().instructions.unwrap();
    assert!(
        instructions
            .contains("This database was populated by dynoxide import, which raised no notices."),
        "a clean import is still worth a sentence: {instructions}"
    );
    assert!(
        !instructions.contains("accepted"),
        "nothing was accepted, so nothing should say so: {instructions}"
    );
}

/// A report whose notices are all cautions says so, rather than reporting
/// zero exposures as if they had been accepted.
#[test]
fn test_import_report_with_cautions_only_in_instructions() {
    use dynoxide::mcp::{ImportConcern, ImportNotice, ImportReport, McpConfig, McpServer};
    use rmcp::ServerHandler;
    use std::sync::Arc;

    let db = dynoxide::Database::memory().unwrap();
    let report = ImportReport {
        tables: 1,
        items: 3,
        skipped: 1,
        notices: vec![ImportNotice {
            concern: ImportConcern::Caution,
            message: "1 lines skipped".into(),
        }],
        exposures_accepted: false,
    };
    let config = McpConfig {
        import: Some(report),
        ..Default::default()
    };
    let server = McpServer::with_config(Arc::new(db), config);

    let instructions = server.get_info().instructions.unwrap();
    assert!(
        instructions.contains(
            "This database was populated by dynoxide import; get_database_info reports 1 notice, none of them exposures."
        ),
        "cautions are reported without an exposure count: {instructions}"
    );
}

/// Write a DynamoDB export directory with one data file for `table`.
#[cfg(feature = "import")]
fn write_export_dir(dir: &std::path::Path, table: &str, items: &[&str]) {
    let data_dir = dir.join(table).join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut f = std::fs::File::create(data_dir.join("00000000.json")).unwrap();
    for item in items {
        writeln!(f, "{item}").unwrap();
    }
}

/// An export whose one rule matches every item and rewrites none of them,
/// because its path is misspelt. Importing it raises one caution and one
/// exposure, so the gate refuses it without `--accept-exposure`. Returns the
/// export directory, the schema file and the rules file.
#[cfg(feature = "import")]
fn write_exposed_export(
    tmp: &std::path::Path,
) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let source = tmp.join("export");
    let schema_file = tmp.join("schema.json");
    let rules_file = tmp.join("rules.toml");
    write_export_dir(
        &source,
        "Users",
        &[
            r#"{"Item": {"pk": {"S": "USER#1"}, "sk": {"S": "PROFILE"}, "email": {"S": "a@real.co.uk"}}}"#,
        ],
    );
    std::fs::write(
        &schema_file,
        json!([{
            "Table": {
                "TableName": "Users",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"}
                ]
            }
        }])
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        &rules_file,
        "[[rules]]\nmatch = \"attribute_exists(pk)\"\npath = \"emial\"\naction = { type = \"redact\" }\n",
    )
    .unwrap();
    (source, schema_file, rules_file)
}

/// The `import` object `get_database_info` must return for the export
/// `write_exposed_export` builds, once the exposure has been accepted.
#[cfg(feature = "import")]
fn assert_accepted_exposure_report(content: &Value) {
    let import = &content["import"];
    assert!(import.is_object(), "import must be reported: {content}");
    assert_eq!(import["tables"], 1);
    assert_eq!(import["items"], 1);
    assert_eq!(import["skipped"], 0);
    assert_eq!(import["notice_count"], 2);
    assert_eq!(import["exposure_count"], 1);
    assert_eq!(import["exposures_accepted"], true);

    let notices = import["notices"].as_array().unwrap();
    assert_eq!(notices.len(), 2, "every notice is listed: {notices:?}");
    assert_eq!(notices[0]["concern"], "caution");
    assert!(
        notices[0]["message"]
            .as_str()
            .unwrap()
            .contains("rules rewrite attributes only"),
        "the caution keeps its message: {notices:?}"
    );
    assert_eq!(notices[1]["concern"], "exposure");
    assert!(
        notices[1]["message"]
            .as_str()
            .unwrap()
            .contains("rewrote none of them"),
        "the exposure keeps its message: {notices:?}"
    );
}

/// The whole path: `dynoxide import --mcp --accept-exposure` on an export
/// whose rule rewrote nothing. The gate lets the run through because the
/// exposure was accepted, and the server that starts must then say so to
/// the agent, which has no other way to learn it.
#[cfg(feature = "import")]
#[test]
fn test_import_mcp_reports_accepted_exposure() {
    let tmp = tempfile::tempdir().unwrap();
    let (source, schema_file, rules_file) = write_exposed_export(tmp.path());

    let mut child = Command::new(env!("CARGO_BIN_EXE_dynoxide"))
        .args(["import", "--mcp", "--accept-exposure", "--source"])
        .arg(&source)
        .arg("--schema")
        .arg(&schema_file)
        .arg("--rules")
        .arg(&rules_file)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn dynoxide import --mcp");

    let resp = init_mcp(&mut child);
    let instructions = resp["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions.contains(
            "This database was populated by dynoxide import; get_database_info reports 2 notices, of which 1 is an exposure that was accepted with --accept-exposure."
        ),
        "the instructions must carry the counts: {instructions}"
    );

    let resp = call_tool(&mut child, 1, "get_database_info", json!({}));
    assert_accepted_exposure_report(&tool_content(&resp));

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `--mcp-read-only` on `import --mcp` must reach the server: an agent handed
/// a copy of real data over stdio is refused writes the same way `dynoxide
/// mcp --read-only` refuses them.
#[cfg(feature = "import")]
#[test]
fn test_import_mcp_read_only_rejects_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("export");
    let schema_file = tmp.path().join("schema.json");
    write_export_dir(
        &source,
        "Users",
        &[r#"{"Item": {"pk": {"S": "USER#1"}, "name": {"S": "Ada"}}}"#],
    );
    std::fs::write(
        &schema_file,
        json!([{
            "Table": {
                "TableName": "Users",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
            }
        }])
        .to_string(),
    )
    .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_dynoxide"))
        .args(["import", "--mcp", "--mcp-read-only", "--source"])
        .arg(&source)
        .arg("--schema")
        .arg(&schema_file)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn dynoxide import --mcp --mcp-read-only");

    init_mcp(&mut child);

    let resp = call_tool(&mut child, 1, "get_database_info", json!({}));
    assert!(!is_tool_error(&resp));
    let content = tool_content(&resp);
    assert_eq!(
        content["read_only"], true,
        "the flag must be reported: {content}"
    );

    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "Users", "item": {"pk": {"S": "USER#2"}, "name": {"S": "Bob"}}}),
    );
    assert!(is_tool_error(&resp), "put_item must be refused: {resp}");
    let content = tool_content(&resp);
    assert_eq!(content["error_type"], "AccessDeniedException");
    assert!(content["message"].as_str().unwrap().contains("read-only"));

    // The imported data is still readable.
    let resp = call_tool(
        &mut child,
        3,
        "get_item",
        json!({"table_name": "Users", "key": {"pk": {"S": "USER#1"}}}),
    );
    assert!(!is_tool_error(&resp), "reads still work: {resp}");
    assert_eq!(tool_content(&resp)["Item"]["name"]["S"], "Ada");

    drop(child.stdin.take());
    let _ = child.wait();
}

/// One JSON-RPC request over the streamable HTTP transport. The server runs
/// stateless with JSON responses, so every call is a single POST.
#[cfg(all(feature = "import", feature = "http-server"))]
async fn mcp_http(client: &reqwest::Client, url: &str, body: Value) -> Value {
    let resp = client
        .post(url)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .json(&body)
        .send()
        .await
        .expect("the MCP HTTP server answers");
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert!(status.is_success(), "MCP HTTP {status}: {text}");
    serde_json::from_str(&text).expect("a JSON-RPC response")
}

/// The same import served the other way: `--serve --mcp` runs the DynamoDB
/// wire surface and the MCP server side by side, and the MCP side must carry
/// the same report the stdio server does.
#[cfg(all(feature = "import", feature = "http-server"))]
#[tokio::test]
async fn test_import_serve_mcp_reports_accepted_exposure_over_http() {
    let tmp = tempfile::tempdir().unwrap();
    let (source, schema_file, rules_file) = write_exposed_export(tmp.path());

    // Both servers start on port 0, so no other test can take a port first,
    // and the MCP port is read from its own listening line.
    let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
    command
        .args([
            "import",
            "--serve",
            "--mcp",
            "--mcp-no-auth",
            "--accept-exposure",
        ])
        .args(["--port", "0"])
        .args(["--mcp-port", "0"])
        .arg("--source")
        .arg(&source)
        .arg("--schema")
        .arg(&schema_file)
        .arg("--rules")
        .arg(&rules_file)
        .stdin(Stdio::null());
    let server = common::Server::spawn(command);

    // The import runs first and the two servers bind after it. The line is
    // printed once the listener is bound, so connections made from here on
    // queue until the server takes them.
    let mcp_port = server.port(common::Listener::Mcp);

    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{mcp_port}/mcp");

    let resp = mcp_http(
        &client,
        &url,
        json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1.0"}
            }
        }),
    )
    .await;
    let instructions = resp["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions.contains(
            "This database was populated by dynoxide import; get_database_info reports 2 notices, of which 1 is an exposure that was accepted with --accept-exposure."
        ),
        "the instructions must carry the counts: {instructions}"
    );

    let resp = mcp_http(
        &client,
        &url,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "get_database_info", "arguments": {}}
        }),
    )
    .await;
    assert!(
        !is_tool_error(&resp),
        "get_database_info must succeed: {resp}"
    );
    assert_accepted_exposure_report(&tool_content(&resp));

    drop(server);
}

// ---------------------------------------------------------------------------
// HTTP transport behaviour: response format, origins, body size, protocol
// versions. These pin what an MCP client sees over HTTP, so an rmcp upgrade
// that changes any of it fails here rather than in a client.
// ---------------------------------------------------------------------------

const HTTP_TOKEN: &str = "http-behaviour-token";

/// POST a JSON-RPC body to `/mcp` with the bearer token and return the raw
/// HTTP response. `extra_headers` is inserted verbatim, one `Name: value\r\n`
/// per header. A write error is ignored so a server that refuses a large body
/// early still has its response read.
fn mcp_post(port: u16, extra_headers: &str, body: &str) -> String {
    use std::io::Read;
    let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let head = format!(
        "POST /mcp HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Authorization: Bearer {HTTP_TOKEN}\r\n\
         {extra_headers}\
         Content-Type: application/json\r\n\
         Accept: application/json, text/event-stream\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    let _ = stream
        .write_all(head.as_bytes())
        .and_then(|_| stream.write_all(body.as_bytes()));
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

/// The JSON-RPC message in a JSON (not event-stream) response body.
fn json_rpc_body(resp: &str) -> Value {
    let body = response_body(resp);
    let start = body
        .find('{')
        .unwrap_or_else(|| panic!("no JSON in: {resp}"));
    let end = body.rfind('}').unwrap();
    serde_json::from_str(&body[start..=end]).unwrap_or_else(|e| panic!("{e}: {resp}"))
}

fn content_type(resp: &str) -> String {
    response_head(resp)
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-type:")
                .map(|v| v.trim().to_string())
        })
        .unwrap_or_default()
}

fn list_tables_call(id: u64, extra_params: Value) -> Value {
    let mut params = json!({"name": "list_tables", "arguments": {}});
    if let (Some(p), Some(extra)) = (params.as_object_mut(), extra_params.as_object()) {
        for (k, v) in extra {
            p.insert(k.clone(), v.clone());
        }
    }
    json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": params})
}

fn tool_names(list: &Value) -> Vec<String> {
    let mut names: Vec<String> = list["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("no tools in {list}"))
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    names
}

#[test]
fn http_lists_the_same_tools_as_stdio_and_answers_in_json() {
    let (port, _srv) = spawn_authed_mcp(HTTP_TOKEN);

    let init = mcp_request(port, &format!("127.0.0.1:{port}"), Some(HTTP_TOKEN));
    assert!(init.starts_with("HTTP/1.1 200"), "got: {init}");
    assert_eq!(
        json_rpc_body(&init)["result"]["serverInfo"]["name"],
        "dynoxide"
    );

    let list = mcp_post(
        port,
        "",
        &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}).to_string(),
    );
    assert!(list.starts_with("HTTP/1.1 200"), "got: {list}");
    let http_tools = tool_names(&json_rpc_body(&list));

    let mut child = spawn_mcp();
    init_mcp(&mut child);
    let stdio_list = send(
        &mut child,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    );
    let _ = child.kill();
    let _ = child.wait();
    assert_eq!(http_tools, tool_names(&stdio_list));

    // A tool call is answered with a single JSON body, not an event stream.
    let call = mcp_post(port, "", &list_tables_call(3, json!({})).to_string());
    assert!(call.starts_with("HTTP/1.1 200"), "got: {call}");
    assert!(
        content_type(&call).starts_with("application/json"),
        "expected a JSON response, got: {call}"
    );
    assert!(
        json_rpc_body(&call)["result"]["content"].is_array(),
        "got: {call}"
    );
}

#[test]
fn http_accepts_an_allowed_origin_on_any_port() {
    // Browser clients send an Origin with the page's port. The allowlist
    // entries are written with an explicit `:*`, so a dev server on 3000 and
    // an --allowed-host name on another port both pass.
    let (port, _srv) = spawn_authed_mcp_host(
        HTTP_TOKEN,
        "127.0.0.1",
        &["--allowed-host", "dynoxide.test"],
    );
    for origin in [
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://dynoxide.test:8080",
    ] {
        let resp = mcp_post(
            port,
            &format!("Origin: {origin}\r\n"),
            &list_tables_call(1, json!({})).to_string(),
        );
        assert!(
            resp.starts_with("HTTP/1.1 200"),
            "Origin {origin} should be accepted, got: {resp}"
        );
    }
    // `:*` widens the port only: another scheme, or a host that merely starts
    // with an allowed one, is still refused.
    for origin in [
        "https://localhost:3000",
        "http://localhost.evil.example:3000",
    ] {
        let resp = mcp_post(
            port,
            &format!("Origin: {origin}\r\n"),
            &list_tables_call(1, json!({})).to_string(),
        );
        assert!(
            resp.starts_with("HTTP/1.1 403") && resp.contains("Origin header is not allowed"),
            "Origin {origin} should be refused, got: {resp}"
        );
    }
}

#[test]
fn http_request_bodies_up_to_16_mib_are_accepted_and_larger_ones_refused() {
    const LIMIT: usize = 16 * 1024 * 1024;
    let (port, _srv) = spawn_authed_mcp(HTTP_TOKEN);
    // list_tables ignores arguments it does not know, so padding one out
    // makes a body of exactly the wanted size without depending on tool
    // behaviour.
    let body_of = |total: usize| {
        let with_padding = |n: usize| {
            list_tables_call(1, json!({"arguments": {"padding": "x".repeat(n)}})).to_string()
        };
        let body = with_padding(total - with_padding(0).len());
        assert_eq!(body.len(), total);
        body
    };

    let at_limit = mcp_post(port, "", &body_of(LIMIT));
    assert!(
        at_limit.starts_with("HTTP/1.1 200"),
        "a body of exactly 16 MiB should be accepted, got: {}",
        &at_limit[..at_limit.len().min(300)]
    );

    let over_limit = mcp_post(port, "", &body_of(LIMIT + 1));
    assert!(
        over_limit.starts_with("HTTP/1.1 413"),
        "a body one byte over 16 MiB should be refused with 413, got: {}",
        &over_limit[..over_limit.len().min(300)]
    );
}

#[test]
fn http_answers_a_2026_07_28_request_like_an_initialised_one() {
    // Protocol 2026-07-28 has no initialize: each request carries its version
    // and client capabilities in `_meta`, and names its method and tool in
    // headers. The tool result must match the one a client gets after a
    // 2025-03-26 initialize. The 2026-07-28 call goes first, to a server that
    // has seen no initialize at all.
    let (port, _srv) = spawn_authed_mcp(HTTP_TOKEN);
    let call_2026 = list_tables_call(
        2,
        json!({"_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {}
        }}),
    )
    .to_string();

    let new = mcp_post(
        port,
        "MCP-Protocol-Version: 2026-07-28\r\n\
         Mcp-Method: tools/call\r\n\
         Mcp-Name: list_tables\r\n",
        &call_2026,
    );
    assert!(new.starts_with("HTTP/1.1 200"), "got: {new}");

    // A 2026-07-28 request must name its method in a header.
    let no_method = mcp_post(
        port,
        "MCP-Protocol-Version: 2026-07-28\r\nMcp-Name: list_tables\r\n",
        &call_2026,
    );
    assert!(
        no_method.starts_with("HTTP/1.1 400") && no_method.contains("Mcp-Method"),
        "got: {no_method}"
    );

    // The server reads the version in `_meta`: one that disagrees with the
    // header is refused, so the call above was checked as a 2026-07-28 request
    // rather than served as a legacy one.
    let mismatched = mcp_post(
        port,
        "MCP-Protocol-Version: 2026-07-28\r\n\
         Mcp-Method: tools/call\r\n\
         Mcp-Name: list_tables\r\n",
        &list_tables_call(
            3,
            json!({"_meta": {
                "io.modelcontextprotocol/protocolVersion": "2025-03-26",
                "io.modelcontextprotocol/clientCapabilities": {}
            }}),
        )
        .to_string(),
    );
    assert!(
        mismatched.starts_with("HTTP/1.1 400") && mismatched.contains("_meta"),
        "got: {mismatched}"
    );

    assert!(
        mcp_request(port, &format!("127.0.0.1:{port}"), Some(HTTP_TOKEN))
            .starts_with("HTTP/1.1 200")
    );
    let old = mcp_post(port, "", &list_tables_call(2, json!({})).to_string());
    assert_eq!(
        json_rpc_body(&new)["result"]["content"],
        json_rpc_body(&old)["result"]["content"],
        "2026-07-28: {new}\n2025-03-26: {old}"
    );
}

#[test]
fn stdio_drops_an_oversized_line_without_writing_anything_else_to_stdout() {
    // rmcp discards a stdio line over 16 MiB and logs an error as it does.
    // That log must not land on stdout, where the client expects only
    // JSON-RPC: the next response read here would fail to parse. RUST_LOG is
    // pinned so an inherited `off` cannot silence the log and pass vacuously.
    let mut child = Command::new(env!("CARGO_BIN_EXE_dynoxide"))
        .arg("mcp")
        .env("RUST_LOG", "error")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn dynoxide mcp");
    init_mcp(&mut child);

    let stdin = child.stdin.as_mut().unwrap();
    let oversized = "x".repeat(17 * 1024 * 1024);
    writeln!(stdin, "{oversized}").unwrap();
    stdin.flush().unwrap();

    let resp = call_tool(&mut child, 2, "list_tables", json!({}));
    assert_eq!(resp["id"], 2, "got: {resp}");
    assert_eq!(tool_content(&resp)["TableNames"], json!([]));

    drop(child.stdin.take());
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("exceeded the maximum line length"),
        "the dropped line should be logged on stderr, got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Arguments read as the HTTP API reads them
// ---------------------------------------------------------------------------
//
// Every expectation below is the HTTP API's answer to the same request, as
// pinned in tests/bool_and_null_values_http.rs, tests/value_layers.rs,
// tests/partiql_member_errors_before_parameters.rs and the scan member checks.

const UNEXPECTED_TOKEN: &str = "Unexpected token received from parser";

/// Spawn a server holding a hash-only table called `table`.
fn mcp_with_table(table: &str) -> std::process::Child {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": table,
            "billing_mode": "PAY_PER_REQUEST",
            "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
            "attribute_definitions": [{"attribute_name": "pk", "attribute_type": "S"}]
        }),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    child
}

/// The error type and message of a tool error, failing if the call succeeded.
fn tool_error(resp: &Value) -> (String, String) {
    assert!(is_tool_error(resp), "expected a tool error: {resp}");
    let raw = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        !raw.contains("VALIDATION") && !raw.contains("SERIALIZATION") && !raw.contains(" at line "),
        "internal marker or serde position leaked: {raw}"
    );
    let content = tool_content(resp);
    (
        content["error_type"].as_str().unwrap().to_string(),
        content["message"].as_str().unwrap().to_string(),
    )
}

fn serialization_error(message: &str) -> (String, String) {
    ("SerializationException".to_string(), message.to_string())
}

fn validation_error(message: &str) -> (String, String) {
    ("ValidationException".to_string(), message.to_string())
}

/// What the HTTP API answers for a flag it can't read as a boolean.
fn refused_flags() -> Vec<(Value, (String, String))> {
    vec![
        (json!("x"), serialization_error(UNEXPECTED_TOKEN)),
        (json!("off"), serialization_error(UNEXPECTED_TOKEN)),
        (json!(""), serialization_error(UNEXPECTED_TOKEN)),
        (json!(" true"), serialization_error(UNEXPECTED_TOKEN)),
        (
            json!(1),
            serialization_error("NUMBER_VALUE cannot be converted to Boolean"),
        ),
        (
            json!(0),
            serialization_error("NUMBER_VALUE cannot be converted to Boolean"),
        ),
        (
            json!(1.5),
            serialization_error("DECIMAL_VALUE cannot be converted to Boolean"),
        ),
        // Pinned to the HTTP API's present answer for a list, which the tool
        // follows if that answer changes.
        (
            json!([true]),
            serialization_error("Unrecognized collection type class java.lang.Boolean"),
        ),
        (
            json!({"a": true}),
            serialization_error("Start of structure or map found where not expected"),
        ),
    ]
}

const TRUTHY: [&str; 9] = ["true", "TRUE", "True", "t", "T", "yes", "y", "Y", "1"];
const FALSY: [&str; 10] = ["false", "FALSE", "f", "F", "no", "No", "n", "N", "0", "nO"];

/// `get_item`'s `consistent_read` reads a string as GetItem's `ConsistentRead`
/// does over HTTP: one that reads as true is a strongly consistent read,
/// charged 1.0 for a small item, one that reads as false an eventually
/// consistent one, charged 0.5, and anything else is refused as malformed.
#[test]
fn get_item_reads_consistent_read_as_the_http_api_does() {
    let mut child = mcp_with_table("Flags");
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "Flags", "item": {"pk": {"S": "c"}}}),
    );
    assert!(!is_tool_error(&resp), "{resp}");

    let mut id = 10;
    let mut get = |child: &mut std::process::Child, flag: Value| {
        id += 1;
        call_tool(
            child,
            id,
            "get_item",
            json!({
                "table_name": "Flags", "key": {"pk": {"S": "c"}},
                "consistent_read": flag, "return_consumed_capacity": "TOTAL"
            }),
        )
    };
    let mut flags: Vec<(Value, f64)> = Vec::new();
    flags.extend(TRUTHY.iter().map(|t| (json!(t), 1.0)));
    flags.extend(FALSY.iter().map(|t| (json!(t), 0.5)));
    flags.extend([(json!(true), 1.0), (json!(false), 0.5), (Value::Null, 0.5)]);
    for (flag, units) in flags {
        let resp = get(&mut child, flag.clone());
        assert!(!is_tool_error(&resp), "{flag}: {resp}");
        let content = tool_content(&resp);
        assert_eq!(content["Item"]["pk"], json!({"S": "c"}), "{flag}");
        assert_eq!(
            content["ConsumedCapacity"]["CapacityUnits"],
            json!(units),
            "{flag}"
        );
    }
    for (flag, refusal) in refused_flags() {
        let resp = get(&mut child, flag.clone());
        assert_eq!(tool_error(&resp), refusal, "{flag}");
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

/// A flag that doesn't read fails the request as it is decoded, so it is the
/// error reported beside a `return_consumed_capacity` outside the enum, which
/// is checked later. That holds for `ConsistentRead` inside `batch_get_item`'s
/// `request_items` too, as BatchGetItem answers over HTTP.
#[test]
fn an_unreadable_flag_is_reported_ahead_of_a_bad_capacity_mode() {
    let mut child = mcp_with_table("Flags");
    let resp = call_tool(
        &mut child,
        2,
        "get_item",
        json!({
            "table_name": "Flags", "key": {"pk": {"S": "c"}},
            "consistent_read": "x", "return_consumed_capacity": "BOGUS"
        }),
    );
    assert_eq!(tool_error(&resp), serialization_error(UNEXPECTED_TOKEN));
    let resp = call_tool(
        &mut child,
        3,
        "query",
        json!({
            "table_name": "Flags", "key_condition_expression": "pk = :p",
            "expression_attribute_values": {":p": {"S": "c"}},
            "scan_index_forward": "x", "return_consumed_capacity": "BOGUS"
        }),
    );
    assert_eq!(tool_error(&resp), serialization_error(UNEXPECTED_TOKEN));
    let resp = call_tool(
        &mut child,
        4,
        "batch_get_item",
        json!({
            "request_items": {"Flags": {"Keys": [{"pk": {"S": "c"}}], "ConsistentRead": "x"}},
            "return_consumed_capacity": "BOGUS"
        }),
    );
    assert_eq!(tool_error(&resp), serialization_error(UNEXPECTED_TOKEN));

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `batch_get_item` leaves a `return_consumed_capacity` outside the enum to
/// BatchGetItem's own checks, so the request's member errors and a refused
/// key value are reported in the order BatchGetItem reports them over HTTP.
#[test]
fn batch_get_item_reports_a_bad_capacity_mode_where_batch_get_item_does() {
    let mut child = mcp_with_table("Flags");
    let bogus = "Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member \
                 must satisfy enum value set: [INDEXES, TOTAL, NONE]";
    let cases: Vec<(Value, (String, String))> = vec![
        (
            json!({}),
            enveloped(
                "Value at 'RequestItems' failed to satisfy constraint: Member must have length \
                 greater than or equal to 1",
            ),
        ),
        (
            json!({"Flags": {"Keys": []}}),
            enveloped(
                "Value at 'RequestItems.Flags.member.Keys' failed to satisfy constraint: Member \
                 must have length greater than or equal to 1",
            ),
        ),
        (
            json!({"T": {"Keys": [{"pk": {"S": "a"}}]}}),
            validation_error(&format!(
                "2 validation errors detected: Value '{{\"T\":{{\"Keys\":[{{\"pk\":{{\"S\":\"a\"}}}}]}}}}' \
                 at 'requestItems' failed to satisfy constraint: Map keys must satisfy \
                 constraint: [Member must have length less than or equal to 255, Member must \
                 have length greater than or equal to 3, Member must satisfy regular \
                 expression pattern: [a-zA-Z0-9_.-]+]; {bogus}"
            )),
        ),
        (
            json!({"Flags": {"Keys": [{"pk": {"S": "a"}}]}}),
            enveloped(bogus),
        ),
        (
            json!({"Flags": {"Keys": [{"pk": {"N": "abc"}}]}}),
            validation_error(ABC_NUMBER),
        ),
        (
            json!({"Flags": {"Keys": [{"pk": {"S": "a", "N": "1"}}]}}),
            validation_error(TWO_TYPES),
        ),
        (
            json!({"Flags": {"Keys": [{"pk": {"BOOL": "no"}}]}}),
            enveloped(bogus),
        ),
        (
            json!({"Flags": {"Keys": (0..101)
                .map(|i| json!({"pk": {"S": i.to_string()}}))
                .collect::<Vec<_>>()}}),
            enveloped(
                "Value at 'RequestItems.Flags.member.Keys' failed to satisfy constraint: Member \
                 must have length less than or equal to 100",
            ),
        ),
    ];
    let checked = cases.len();
    let mut mismatches = Vec::new();
    for (id, (request_items, refusal)) in (10..).zip(cases) {
        let resp = call_tool(
            &mut child,
            id,
            "batch_get_item",
            json!({"request_items": request_items, "return_consumed_capacity": "BOGUS"}),
        );
        let answer = tool_error(&resp);
        if answer != refusal {
            mismatches.push(format!("{request_items}: {answer:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of {checked} answers differ: {mismatches:#?}",
        mismatches.len()
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `query`'s `scan_index_forward` reads a string as Query's `ScanIndexForward`
/// does over HTTP: one that reads as false reads the sort key descending, one
/// that reads as true ascending, and anything else is refused as malformed.
#[test]
fn query_reads_scan_index_forward_as_the_http_api_does() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Sorted",
            "billing_mode": "PAY_PER_REQUEST",
            "key_schema": [
                {"attribute_name": "pk", "key_type": "HASH"},
                {"attribute_name": "sk", "key_type": "RANGE"}
            ],
            "attribute_definitions": [
                {"attribute_name": "pk", "attribute_type": "S"},
                {"attribute_name": "sk", "attribute_type": "N"}
            ]
        }),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    for (id, sk) in [(2, "1"), (3, "2"), (4, "3")] {
        let resp = call_tool(
            &mut child,
            id,
            "put_item",
            json!({"table_name": "Sorted", "item": {"pk": {"S": "p"}, "sk": {"N": sk}}}),
        );
        assert!(!is_tool_error(&resp), "{resp}");
    }

    let mut id = 10;
    let mut query = |child: &mut std::process::Child, flag: Value| {
        id += 1;
        call_tool(
            child,
            id,
            "query",
            json!({
                "table_name": "Sorted", "key_condition_expression": "pk = :p",
                "expression_attribute_values": {":p": {"S": "p"}},
                "scan_index_forward": flag
            }),
        )
    };
    let order = |resp: &Value| -> Vec<String> {
        tool_content(resp)["Items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["sk"]["N"].as_str().unwrap().to_string())
            .collect()
    };
    let mut flags: Vec<(Value, [&str; 3])> = Vec::new();
    flags.extend(TRUTHY.iter().map(|t| (json!(t), ["1", "2", "3"])));
    flags.extend(FALSY.iter().map(|t| (json!(t), ["3", "2", "1"])));
    flags.extend([
        (json!(true), ["1", "2", "3"]),
        (json!(false), ["3", "2", "1"]),
        (Value::Null, ["1", "2", "3"]),
    ]);
    for (flag, expected) in flags {
        let resp = query(&mut child, flag.clone());
        assert!(!is_tool_error(&resp), "{flag}: {resp}");
        assert_eq!(order(&resp), expected, "{flag}");
    }
    for (flag, refusal) in refused_flags() {
        let resp = query(&mut child, flag.clone());
        assert_eq!(tool_error(&resp), refusal, "{flag}");
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `ConsistentRead` inside `batch_get_item`'s `request_items` reads as
/// BatchGetItem's does over HTTP, under either spelling. A string used to be
/// dropped without a word, so `"true"` read eventually consistent.
#[test]
fn batch_get_item_reads_consistent_read_as_the_http_api_does() {
    let mut child = mcp_with_table("Flags");
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "Flags", "item": {"pk": {"S": "c"}}}),
    );
    assert!(!is_tool_error(&resp), "{resp}");

    let mut id = 10;
    let mut batch = |child: &mut std::process::Child, member: &str, flag: Value| {
        id += 1;
        call_tool(
            child,
            id,
            "batch_get_item",
            json!({
                "request_items": {"Flags": {"keys": [{"pk": {"S": "c"}}], member: flag}},
                "return_consumed_capacity": "TOTAL"
            }),
        )
    };
    for member in ["ConsistentRead", "consistent_read"] {
        for (flag, units) in [
            (json!("true"), 1.0),
            (json!("Yes"), 1.0),
            (json!("1"), 1.0),
            (json!(true), 1.0),
            (json!("false"), 0.5),
            (json!("n"), 0.5),
            (json!(false), 0.5),
            (Value::Null, 0.5),
        ] {
            let resp = batch(&mut child, member, flag.clone());
            assert!(!is_tool_error(&resp), "{member} {flag}: {resp}");
            assert_eq!(
                tool_content(&resp)["ConsumedCapacity"][0]["CapacityUnits"],
                json!(units),
                "{member} {flag}"
            );
        }
        for (flag, refusal) in refused_flags() {
            let resp = batch(&mut child, member, flag.clone());
            assert_eq!(tool_error(&resp), refusal, "{member} {flag}");
        }
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

/// Strings are read but not advertised: each flag's listed schema is the
/// boolean it has always been.
#[test]
fn the_flags_still_advertise_a_boolean() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    let resp = send(
        &mut child,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    );
    let tools = resp["result"]["tools"].as_array().unwrap();
    let property = |tool: &str, name: &str| {
        let tool = tools.iter().find(|t| t["name"] == tool).unwrap();
        assert!(
            !tool["inputSchema"]["required"]
                .as_array()
                .is_some_and(|r| r.contains(&json!(name))),
            "{name} is optional"
        );
        tool["inputSchema"]["properties"][name].clone()
    };
    assert_eq!(
        property("get_item", "consistent_read"),
        json!({
            "default": null,
            "description": "Whether to use strongly consistent reads",
            "type": ["boolean", "null"]
        })
    );
    assert_eq!(
        property("query", "scan_index_forward"),
        json!({
            "description": "Whether to scan forward (true, default) or backward (false) by sort key",
            "type": ["boolean", "null"]
        })
    );
    assert_eq!(
        property("execute_partiql", "consistent_read"),
        json!({
            "default": null,
            "description": "Whether to use strongly consistent reads. Does not change which rows \
                            come back, but it doubles the rate a SELECT is charged at and it makes \
                            a SELECT qualified by a GSI a rejection, because a GSI cannot serve one.",
            "type": ["boolean", "null"]
        })
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `scan`'s `segment` and `total_segments` take a negative value now, but the
/// listed schema is the unsigned integer it has always been.
#[test]
fn the_segments_still_advertise_an_unsigned_integer() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    let resp = send(
        &mut child,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    );
    let tools = resp["result"]["tools"].as_array().unwrap();
    let scan = tools.iter().find(|t| t["name"] == "scan").unwrap();
    let properties = &scan["inputSchema"]["properties"];
    assert_eq!(
        properties["segment"],
        json!({
            "description": "Segment number for parallel scan (0-based). Must be used with \
                            total_segments.",
            "format": "uint32",
            "minimum": 0,
            "type": ["integer", "null"]
        })
    );
    assert_eq!(
        properties["total_segments"],
        json!({
            "description": "Total number of parallel scan segments (1 to 1000000). Must be used \
                            with segment.",
            "format": "uint32",
            "minimum": 0,
            "type": ["integer", "null"]
        })
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

const ABC_NUMBER: &str = "The parameter cannot be converted to a numeric value: abc";
const NULL_NOT_TRUE: &str = "One or more parameter values were invalid: Null attribute value \
                             types must have the value of true";
const TWO_TYPES: &str = "Supplied AttributeValue has more than one datatypes set, must contain \
                         exactly one of the supported datatypes";

fn enveloped(message: &str) -> (String, String) {
    validation_error(&format!("1 validation error detected: {message}"))
}

fn named(message: &str, key: &str) -> (String, String) {
    validation_error(&format!(
        "ExpressionAttributeValues contains invalid value: {message} for key {key}"
    ))
}

/// A request holding a `BOOL` or `NULL` written as a string, or a value with
/// two members, is read on the older layer, as tests/value_layers.rs pins over
/// HTTP: its refusals are bare on `put_item`, and `update_item` names the key,
/// where the same refusal alone is enveloped. The cases run in one process, so
/// a current-layer call after an older-layer one shows the layer doesn't leak.
#[test]
fn item_tools_read_values_on_the_layer_the_http_api_uses() {
    let mut child = mcp_with_table("Layers");
    let bad_number = json!({"N": "abc"});
    let string_bool = json!({"BOOL": "no"});
    let cases = vec![
        (
            vec![("v", bad_number.clone())],
            enveloped(ABC_NUMBER),
            enveloped(ABC_NUMBER),
        ),
        (
            vec![("v", bad_number.clone()), ("w", string_bool.clone())],
            validation_error(ABC_NUMBER),
            named(ABC_NUMBER, ":v"),
        ),
        (
            vec![("v", bad_number.clone())],
            enveloped(ABC_NUMBER),
            enveloped(ABC_NUMBER),
        ),
        (
            vec![("w", string_bool.clone()), ("v", bad_number.clone())],
            validation_error(ABC_NUMBER),
            named(ABC_NUMBER, ":v"),
        ),
        (
            vec![("v", bad_number.clone()), ("w", json!({"BOOL": true}))],
            enveloped(ABC_NUMBER),
            enveloped(ABC_NUMBER),
        ),
        (
            vec![("v", json!({"NULL": false}))],
            enveloped(NULL_NOT_TRUE),
            enveloped(NULL_NOT_TRUE),
        ),
        (
            vec![("v", json!({"NULL": "no"}))],
            validation_error(NULL_NOT_TRUE),
            named(NULL_NOT_TRUE, ":v"),
        ),
        (
            vec![("v", json!({"S": "x", "N": "1"}))],
            validation_error(TWO_TYPES),
            named(TWO_TYPES, ":v"),
        ),
        (
            vec![("v", json!({"N": "abc", "S": "x"}))],
            validation_error(ABC_NUMBER),
            named(ABC_NUMBER, ":v"),
        ),
        (
            vec![("v", json!({"NULL": false}))],
            enveloped(NULL_NOT_TRUE),
            enveloped(NULL_NOT_TRUE),
        ),
    ];
    let mut id = 10;
    for (pairs, on_put, on_update) in cases {
        let mut item = json!({"pk": {"S": "m1"}});
        let mut values = json!({});
        let mut set = Vec::new();
        for (name, value) in &pairs {
            item[name] = value.clone();
            values[format!(":{name}")] = value.clone();
            set.push(format!("{name} = :{name}"));
        }
        id += 1;
        let resp = call_tool(
            &mut child,
            id,
            "put_item",
            json!({"table_name": "Layers", "item": item}),
        );
        assert_eq!(tool_error(&resp), on_put, "put_item {pairs:?}");
        id += 1;
        let resp = call_tool(
            &mut child,
            id,
            "update_item",
            json!({
                "table_name": "Layers", "key": {"pk": {"S": "m1"}},
                "update_expression": format!("SET {}", set.join(", ")),
                "expression_attribute_values": values
            }),
        );
        assert_eq!(tool_error(&resp), on_update, "update_item {pairs:?}");
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `delete_item` follows the layer too: a key value it refuses is enveloped
/// on its own and bare beside a string-read `BOOL`, as DeleteItem answers over
/// HTTP.
#[test]
fn delete_item_reads_its_key_on_the_layer_the_http_api_uses() {
    let mut child = mcp_with_table("Layers");
    let older = json!({
        "table_name": "Layers", "key": {"pk": {"N": "abc"}},
        "condition_expression": "w = :w",
        "expression_attribute_values": {":w": {"BOOL": "no"}}
    });
    let current = json!({"table_name": "Layers", "key": {"pk": {"N": "abc"}}});
    for (id, args, refusal) in [
        (2, current.clone(), enveloped(ABC_NUMBER)),
        (3, older, validation_error(ABC_NUMBER)),
        (4, current, enveloped(ABC_NUMBER)),
    ] {
        let resp = call_tool(&mut child, id, "delete_item", args.clone());
        assert_eq!(tool_error(&resp), refusal, "{args}");
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

/// An argument that isn't DynamoDB JSON at all is refused with the tool's own
/// message naming the argument, with no serde marker or position in it.
#[test]
fn a_map_argument_that_is_not_dynamodb_json_names_the_argument() {
    let mut child = mcp_with_table("Layers");
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "Layers", "item": [1]}),
    );
    assert_eq!(
        tool_error(&resp),
        validation_error("Invalid DynamoDB JSON in 'item': invalid type: sequence, expected a map")
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// The tools without the envelope keep their operation's own layer: GetItem
/// and TransactWriteItems read values on the current layer, bare, and the
/// batch, TransactGetItems and PartiQL operations on the older one, which
/// reads `1e2147483647` as Java's `BigDecimal` does and calls it an underflow
/// (tests/value_layers.rs).
#[test]
fn other_tools_read_values_on_their_operations_layer() {
    let mut child = mcp_with_table("Layers");
    let resp = call_tool(
        &mut child,
        2,
        "get_item",
        json!({"table_name": "Layers", "key": {"pk": {"N": "abc"}}}),
    );
    assert_eq!(tool_error(&resp), validation_error(ABC_NUMBER));
    let resp = call_tool(
        &mut child,
        3,
        "get_item",
        json!({"table_name": "Layers", "key": {"pk": {"N": "abc"}, "x": {"BOOL": "no"}}}),
    );
    assert_eq!(tool_error(&resp), validation_error(ABC_NUMBER));
    let underflow = "Number underflow. Attempting to store a number with magnitude smaller than \
                     supported range";
    let tiny = json!({"N": "1e2147483647"});
    let select = "SELECT * FROM \"Layers\" WHERE pk = ?";
    // The batch_write_item and transact_get_items cases read on the older
    // layer only through the tool's entry in the operation map. The three
    // PartiQL cases get the same answer without it, from the actions
    // themselves, so they pin the tool's answer whatever the map says.
    let older: Vec<(&str, Value, (String, String))> = vec![
        (
            "batch_write_item",
            json!({"request_items": {"Layers": [
                {"PutRequest": {"Item": {"pk": {"S": "a"}, "v": tiny}}}
            ]}}),
            validation_error(underflow),
        ),
        (
            "transact_get_items",
            json!({"transact_items": [{"Get": {"TableName": "Layers", "Key": {"pk": tiny}}}]}),
            validation_error(underflow),
        ),
        (
            "execute_partiql",
            json!({"statement": select, "parameters": [tiny]}),
            validation_error(underflow),
        ),
        (
            "execute_transaction_partiql",
            json!({"transact_statements": [{"Statement": select, "Parameters": [tiny]}]}),
            validation_error(underflow),
        ),
    ];
    let checked = older.len() + 1;
    let mut mismatches = Vec::new();
    let mut id = 10;
    for (tool, args, refusal) in older {
        id += 1;
        let resp = call_tool(&mut child, id, tool, args.clone());
        let answer = tool_error(&resp);
        if answer != refusal {
            mismatches.push(format!("{tool} {args}: {answer:?}"));
        }
    }
    // BatchExecuteStatement answers each statement in its own response.
    let resp = call_tool(
        &mut child,
        30,
        "batch_execute_partiql",
        json!({"statements": [{"Statement": select, "Parameters": [tiny]}]}),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    let responses = &tool_content(&resp)["Responses"];
    if *responses != json!([{"Error": {"Code": "ValidationError", "Message": underflow}}]) {
        mismatches.push(format!("batch_execute_partiql: {responses}"));
    }
    assert!(
        mismatches.is_empty(),
        "{} of {checked} older-layer answers differ: {mismatches:#?}",
        mismatches.len()
    );
    // transact_write_items reads its values and its items on the current
    // layer, bare (captured eu-west-2, us-east-1, eu-west-1 and us-west-2,
    // 2026-11-11).
    let overflow = "Number overflow. Attempting to store a number with magnitude larger than \
                    supported range";
    for (id, action) in [
        json!({"Update": {
            "TableName": "Layers", "Key": {"pk": {"S": "a"}},
            "UpdateExpression": "SET v = :v",
            "ExpressionAttributeValues": {":v": tiny}
        }}),
        json!({"Put": {"TableName": "Layers", "Item": {"pk": {"S": "a"}, "v": tiny}}}),
    ]
    .into_iter()
    .enumerate()
    {
        let resp = call_tool(
            &mut child,
            40 + id as u64,
            "transact_write_items",
            json!({"transact_items": [action]}),
        );
        assert_eq!(tool_error(&resp), validation_error(overflow), "{action}");
    }
    // And put_item, straight after, is back on the current layer.
    let resp = call_tool(
        &mut child,
        31,
        "put_item",
        json!({"table_name": "Layers", "item": {"pk": {"S": "a"}, "v": {"N": "1e2147483647"}}}),
    );
    assert_eq!(
        tool_error(&resp),
        enveloped(
            "Number overflow. Attempting to store a number with magnitude larger than \
             supported range"
        )
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `scan`'s `segment` and `total_segments` answer as Scan's member checks do
/// over HTTP (tests/scan_member_stages.rs): a negative one gets the member
/// check's message rather than failing argument decoding, and one past what
/// a 32-bit integer holds is read as the largest one, which moves the request
/// to the older wording with the value echoed.
#[test]
fn scan_segments_answer_as_the_http_api_does() {
    let mut child = mcp_with_table("Segments");
    let one = |m: &str| validation_error(&format!("1 validation error detected: {m}"));
    let segment_min = "Value at 'Segment' failed to satisfy constraint: Member must have value \
                       greater than or equal to 0";
    let total_min = "Value at 'TotalSegments' failed to satisfy constraint: Member must have \
                     value greater than or equal to 1";
    let zero_based = validation_error(
        "The Segment parameter is zero-based and must be less than parameter TotalSegments",
    );
    let cases: Vec<(Value, (String, String))> = vec![
        (
            json!({"segment": -1, "total_segments": 2}),
            one(segment_min),
        ),
        (json!({"segment": -1}), one(segment_min)),
        (json!({"segment": 0, "total_segments": -1}), one(total_min)),
        (json!({"total_segments": -1}), one(total_min)),
        (json!({"segment": 0, "total_segments": 0}), one(total_min)),
        (
            json!({"segment": -1, "total_segments": -1}),
            validation_error(&format!(
                "2 validation errors detected: {segment_min}; {total_min}"
            )),
        ),
        (
            json!({"segment": 2, "total_segments": 2}),
            zero_based.clone(),
        ),
        (json!({"segment": 3, "total_segments": 2}), zero_based),
        (
            json!({"segment": 0, "total_segments": 1_000_001}),
            one(
                "Value at 'TotalSegments' failed to satisfy constraint: Member must have value \
                 less than or equal to 1000000",
            ),
        ),
        (
            json!({"segment": 3_000_000_000_u64, "total_segments": 2}),
            one(
                "Value '2147483647' at 'segment' failed to satisfy constraint: Member must have \
                 value less than or equal to 999999",
            ),
        ),
        (
            json!({"segment": 4_294_967_296_u64, "total_segments": 2}),
            one(
                "Value '2147483647' at 'segment' failed to satisfy constraint: Member must have \
                 value less than or equal to 999999",
            ),
        ),
        (
            json!({"segment": -3_000_000_000_i64, "total_segments": 2}),
            one(
                "Value '-2147483648' at 'segment' failed to satisfy constraint: Member must have \
                 value greater than or equal to 0",
            ),
        ),
        (
            json!({"segment": 0, "total_segments": 4_294_967_296_u64}),
            one(
                "Value '2147483647' at 'totalSegments' failed to satisfy constraint: Member must \
                 have value less than or equal to 1000000",
            ),
        ),
        // The member checks come ahead of the capacity mode, and the older
        // wording lists the mode and the limit with the segment.
        (
            json!({"segment": -1, "total_segments": 2, "return_consumed_capacity": "BOGUS"}),
            one(segment_min),
        ),
        (
            json!({
                "segment": 3_000_000_000_u64, "total_segments": 2,
                "limit": 0, "return_consumed_capacity": "BOGUS"
            }),
            validation_error(
                "3 validation errors detected: Value 'BOGUS' at 'returnConsumedCapacity' failed \
                 to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, \
                 NONE]; Value '2147483647' at 'segment' failed to satisfy constraint: Member \
                 must have value less than or equal to 999999; Value '0' at 'limit' failed to \
                 satisfy constraint: Member must have value greater than or equal to 1",
            ),
        ),
        // A limit past what a 32-bit integer holds is read as the largest one,
        // which moves a segment's refusal to the older wording too, whether
        // or not the segment itself is past 32 bits.
        // With no segment, the limit's own read leaves an enum refusal as it
        // is worded on either layer.
        (
            json!({"limit": 3_000_000_000_u64, "return_consumed_capacity": "BOGUS"}),
            one(
                "Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member \
                 must satisfy enum value set: [INDEXES, TOTAL, NONE]",
            ),
        ),
        (
            json!({"limit": 3_000_000_000_u64, "select": "BOGUS"}),
            one(
                "Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum \
                 value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]",
            ),
        ),
        (
            json!({"total_segments": 0, "limit": 3_000_000_000_u64}),
            one(
                "Value '0' at 'totalSegments' failed to satisfy constraint: Member must have \
                 value greater than or equal to 1",
            ),
        ),
        (
            json!({"segment": -1, "total_segments": 2, "limit": 3_000_000_000_u64}),
            one(
                "Value '-1' at 'segment' failed to satisfy constraint: Member must have value \
                 greater than or equal to 0",
            ),
        ),
    ];
    let mut id = 10;
    for (segments, refusal) in cases {
        let mut args = json!({"table_name": "Segments"});
        for (k, v) in segments.as_object().unwrap() {
            args[k] = v.clone();
        }
        id += 1;
        let resp = call_tool(&mut child, id, "scan", args);
        assert_eq!(tool_error(&resp), refusal, "{segments}");
    }
    // A segment in range still scans.
    let resp = call_tool(
        &mut child,
        2,
        "scan",
        json!({"table_name": "Segments", "segment": 1, "total_segments": 2}),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    assert_eq!(tool_content(&resp)["Count"], json!(0));
    // And so does a limit past 32 bits with no segments, which reads every
    // item rather than stopping short.
    for (id, pk) in (3..).zip(["a", "b", "c"]) {
        let resp = call_tool(
            &mut child,
            id,
            "put_item",
            json!({"table_name": "Segments", "item": {"pk": {"S": pk}}}),
        );
        assert!(!is_tool_error(&resp), "{resp}");
    }
    let resp = call_tool(
        &mut child,
        6,
        "scan",
        json!({"table_name": "Segments", "limit": 3_000_000_000_u64}),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    let content = tool_content(&resp);
    assert_eq!(content["Count"], json!(3), "{content}");
    let mut keys: Vec<&str> = content["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["pk"]["S"].as_str().unwrap())
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["a", "b", "c"]);
    assert!(content.get("LastEvaluatedKey").is_none(), "{content}");

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `execute_partiql` reports a member error ahead of a parameter it refuses
/// for its value, and a parameter of the wrong JSON type, or `parameters`
/// that isn't a list, ahead of both, as ExecuteStatement answers over HTTP
/// (tests/partiql_member_errors_before_parameters.rs). No table is needed:
/// each statement is refused before its table is looked up.
#[test]
fn execute_partiql_orders_member_and_parameter_errors_as_the_http_api_does() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    let select = "SELECT * FROM \"Missing\" WHERE pk = ?";
    let limit_0 = enveloped(
        "Value '0' at 'limit' failed to satisfy constraint: Member must have value greater \
         than or equal to 1",
    );
    let bad_capacity = "Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: \
                        Member must satisfy enum value set: [INDEXES, TOTAL, NONE]";
    let empty = "Supplied AttributeValue is empty, must contain exactly one of the supported \
                 datatypes";
    let number_to_string = serialization_error("NUMBER_VALUE cannot be converted to String");
    let bad = json!({"N": "abc"});

    let mut cases: Vec<(Value, (String, String))> = vec![
        // Member errors beat a number the parameter walk refuses.
        (
            json!({"statement": select, "parameters": [bad], "limit": 0}),
            limit_0.clone(),
        ),
        (
            json!({
                "statement": select, "parameters": [bad],
                "return_values_on_condition_check_failure": "BAD"
            }),
            enveloped(
                "Value 'BAD' at 'returnValuesOnConditionCheckFailure' failed to satisfy \
                 constraint: Member must satisfy enum value set: [ALL_OLD, NONE]",
            ),
        ),
        (
            json!({"statement": select, "parameters": [bad], "return_consumed_capacity": "BAD"}),
            enveloped(bad_capacity),
        ),
        (
            json!({"statement": "", "parameters": [bad]}),
            enveloped(
                "Value '' at 'statement' failed to satisfy constraint: Member must have length \
                 greater than or equal to 1",
            ),
        ),
        (
            json!({
                "statement": select, "parameters": [bad], "limit": 0,
                "return_consumed_capacity": "BAD"
            }),
            validation_error(&format!(
                "2 validation errors detected: Value '0' at 'limit' failed to satisfy \
                 constraint: Member must have value greater than or equal to 1; {bad_capacity}"
            )),
        ),
        // A parameter of the wrong JSON type fails first.
        (
            json!({"statement": select, "parameters": [bad, {"N": 1}]}),
            number_to_string.clone(),
        ),
        (
            json!({"statement": select, "parameters": [bad, {"N": 1}], "limit": 0}),
            number_to_string.clone(),
        ),
        (
            json!({"statement": select, "parameters": [bad, {"S": 1}], "limit": 0}),
            number_to_string.clone(),
        ),
        (
            json!({"statement": select, "parameters": [{"N": 1}], "limit": 0}),
            number_to_string,
        ),
        (
            json!({"statement": select, "parameters": ["str"]}),
            serialization_error("Unexpected value type in payload"),
        ),
        (
            json!({"statement": select, "parameters": ["str"], "limit": 0}),
            serialization_error("Unexpected value type in payload"),
        ),
        // With no member error, the first refused parameter is reported.
        (
            json!({"statement": select, "parameters": [bad]}),
            validation_error(ABC_NUMBER),
        ),
        (
            json!({"statement": select, "parameters": [{}, bad]}),
            validation_error(empty),
        ),
        (
            json!({"statement": select, "parameters": [bad, {}]}),
            validation_error(ABC_NUMBER),
        ),
        (
            json!({"statement": select, "parameters": [bad], "next_token": "garbage"}),
            validation_error(ABC_NUMBER),
        ),
        (
            json!({"statement": select, "parameters": []}),
            enveloped(
                "Value '[]' at 'parameters' failed to satisfy constraint: Member must have \
                 length greater than or equal to 1",
            ),
        ),
    ];
    // A limit of 0 beats every kind of parameter refused for its value.
    for refused in [
        json!([{}]),
        json!([{"S": "a", "N": "1"}]),
        json!([{"NULL": false}]),
        json!([{"NULL": "false"}]),
        json!([{"NULL": false, "S": "a"}]),
        json!([{"X": "a"}]),
        json!([{"S": null}]),
        json!([null]),
        json!([{"NS": ["abc"]}]),
        json!([{"M": {"a": {"N": "abc"}}}]),
        json!([{"L": [{"N": "abc"}]}]),
        json!([{"N": ""}]),
        json!([{}, {"N": "abc"}]),
        json!([{"N": "1"}, {"N": "abc"}, {"SS": ["a", "a"]}]),
    ] {
        cases.push((
            json!({"statement": select, "parameters": refused, "limit": 0}),
            limit_0.clone(),
        ));
    }
    // Parameters that aren't a list fail the request first.
    for (parameters, message) in [
        (json!("x"), "Unexpected field type"),
        (json!(5), "Unexpected field type"),
        (json!(true), "Unexpected field type"),
        (
            json!({}),
            "Start of structure or map found where not expected",
        ),
        (
            json!({"N": "1"}),
            "Start of structure or map found where not expected",
        ),
    ] {
        cases.push((
            json!({"statement": select, "parameters": parameters}),
            serialization_error(message),
        ));
        cases.push((
            json!({"statement": select, "parameters": parameters, "limit": 0}),
            serialization_error(message),
        ));
    }
    let mut id = 10;
    for (args, refusal) in cases {
        id += 1;
        let resp = call_tool(&mut child, id, "execute_partiql", args.clone());
        assert_eq!(tool_error(&resp), refusal, "{args}");
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `execute_partiql`'s `consistent_read` is ExecuteStatement's
/// `ConsistentRead` as given, so it answers whatever ExecuteStatement answers
/// over HTTP. A string DynamoDB reads as a boolean, in any case, reads as
/// GetItem reads it, and the read is charged by the consistency it names
/// (captured eu-west-2 and us-east-1, 2026-11-11: `"true"`, `"t"`, `"yes"`,
/// `"y"`, `"1"`, `"TRUE"`, `"Yes"`, `"false"`, `"f"`, `"no"`, `"n"` and `"0"`
/// run). Any other string, `"on"` and `" true"` among them, is refused as a
/// token the parser didn't expect (captured eu-west-2 and us-east-1,
/// 2026-11-11). A number is refused as GetItem refuses it (captured eu-west-2,
/// 2026-11-10), and a list with no message (captured eu-west-2 and us-east-1,
/// 2026-11-11).
#[test]
fn execute_partiql_reads_consistent_read_as_execute_statement_does() {
    let mut child = mcp_with_table("Pql");
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "Pql", "item": {"pk": {"S": "a"}}}),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    let select = "SELECT * FROM \"Pql\" WHERE pk = 'a'";
    let mut id = 10;
    let consistent = ["true", "t", "yes", "y", "1", "TRUE", "Yes"].map(|s| (json!(s), 1.0));
    let eventual = ["false", "f", "no", "n", "0"].map(|s| (json!(s), 0.5));
    let read = [(json!(true), 1.0), (json!(false), 0.5), (Value::Null, 0.5)]
        .into_iter()
        .chain(consistent)
        .chain(eventual);
    for (flag, units) in read {
        id += 1;
        let resp = call_tool(
            &mut child,
            id,
            "execute_partiql",
            json!({
                "statement": select, "consistent_read": flag,
                "return_consumed_capacity": "TOTAL"
            }),
        );
        assert!(!is_tool_error(&resp), "{flag}: {resp}");
        let content = tool_content(&resp);
        assert_eq!(content["Items"].as_array().unwrap().len(), 1, "{flag}");
        assert_eq!(
            content["ConsumedCapacity"]["CapacityUnits"],
            json!(units),
            "{flag}"
        );
    }
    for (flag, refusal) in [
        (json!("x"), serialization_error(UNEXPECTED_TOKEN)),
        (json!("on"), serialization_error(UNEXPECTED_TOKEN)),
        (json!(" true"), serialization_error(UNEXPECTED_TOKEN)),
        (json!(""), serialization_error(UNEXPECTED_TOKEN)),
        (
            json!(1),
            serialization_error("NUMBER_VALUE cannot be converted to Boolean"),
        ),
    ] {
        id += 1;
        let resp = call_tool(
            &mut child,
            id,
            "execute_partiql",
            json!({"statement": select, "consistent_read": flag}),
        );
        assert_eq!(tool_error(&resp), refusal, "{flag}");
    }
    for flag in [json!([]), json!([true])] {
        id += 1;
        let resp = call_tool(
            &mut child,
            id,
            "execute_partiql",
            json!({"statement": select, "consistent_read": flag}),
        );
        assert!(is_tool_error(&resp), "{flag}: {resp}");
        let content = tool_content(&resp);
        assert_eq!(content["error_type"], "SerializationException", "{flag}");
        assert!(
            content["message"].as_str().is_none_or(str::is_empty),
            "{flag}: {content}"
        );
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

/// A JSON null where an attribute value belongs gets the HTTP API's answer
/// through the tools too: named after its attribute in an item or a key,
/// named by key in the expression values, and wrapped as the starting key's
/// fault in an `exclusive_start_key` (captured eu-west-2 and us-east-1,
/// 2026-11-11, and pinned over HTTP in tests/null_attribute_values.rs).
#[test]
fn a_null_value_is_refused_through_the_tools_as_over_http() {
    const NULL_VALUE: &str = "The supplied Item contains a null AttributeValue";
    let mut child = mcp_with_table("Nulls");
    let member = validation_error(&format!("{NULL_VALUE}: pk is null"));
    let cases: Vec<(&str, Value, (String, String))> = vec![
        (
            "put_item",
            json!({"table_name": "Nulls", "item": {"pk": null}}),
            member.clone(),
        ),
        (
            "get_item",
            json!({"table_name": "Nulls", "key": {"pk": null}}),
            member.clone(),
        ),
        (
            "delete_item",
            json!({"table_name": "Nulls", "key": {"pk": null}}),
            member.clone(),
        ),
        (
            "update_item",
            json!({"table_name": "Nulls", "key": {"pk": null}, "update_expression": "SET a = :v",
                "expression_attribute_values": {":v": {"S": "x"}}}),
            member.clone(),
        ),
        (
            "put_item",
            json!({"table_name": "Nulls", "item": {"pk": {"S": "a"}},
                "condition_expression": "a = :v", "expression_attribute_values": {":v": null}}),
            named(NULL_VALUE, ":v"),
        ),
        (
            "update_item",
            json!({"table_name": "Nulls", "key": {"pk": {"S": "a"}}, "update_expression": "SET a = :v",
                "expression_attribute_values": {":v": null}}),
            named(NULL_VALUE, ":v"),
        ),
        (
            "delete_item",
            json!({"table_name": "Nulls", "key": {"pk": {"S": "a"}},
                "condition_expression": "a = :v", "expression_attribute_values": {":v": null}}),
            named(NULL_VALUE, ":v"),
        ),
        (
            "query",
            json!({"table_name": "Nulls", "key_condition_expression": "pk = :v",
                "expression_attribute_values": {":v": null}}),
            named(NULL_VALUE, ":v"),
        ),
        (
            "scan",
            json!({"table_name": "Nulls", "filter_expression": "a = :v",
                "expression_attribute_values": {":v": null}}),
            named(NULL_VALUE, ":v"),
        ),
        (
            "scan",
            json!({"table_name": "Nulls", "exclusive_start_key": {"pk": null}}),
            validation_error(&format!(
                "The provided starting key is invalid: {NULL_VALUE}: pk is null"
            )),
        ),
        (
            "put_item",
            json!({"table_name": "Nulls", "item": {"pk": {"S": "a"}},
                "condition_expression": "x = :a AND y = :b",
                "expression_attribute_values": {":b": null, ":a": {"N": "abc"}}}),
            named(NULL_VALUE, ":b"),
        ),
        // A null puts the call on the older layer: a bad number beside one in
        // the item is bare, and one in the expression values is named.
        (
            "put_item",
            json!({"table_name": "Nulls", "item": {"pk": null, "x": {"N": "abc"}}}),
            validation_error(ABC_NUMBER),
        ),
        (
            "put_item",
            json!({"table_name": "Nulls", "item": {"pk": {"S": "a"}},
                "condition_expression": "x = :a AND y = :b",
                "expression_attribute_values": {":a": null, ":b": {"N": "abc"}}}),
            named(ABC_NUMBER, ":b"),
        ),
    ];
    let checked = cases.len();
    let mut mismatches = Vec::new();
    for (id, (tool, args, refusal)) in (10..).zip(cases) {
        let resp = call_tool(&mut child, id, tool, args.clone());
        let answer = tool_error(&resp);
        if answer != refusal {
            mismatches.push(format!("{tool} {args}: {answer:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of {checked} answers differ: {mismatches:#?}",
        mismatches.len()
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// A null name in `expression_attribute_names` is read as an empty one, and a
/// null member of a set is a value fault, through the tools as over HTTP
/// (captured from DynamoDB on 12 November 2026, and pinned over HTTP in
/// tests/null_attribute_values.rs).
#[test]
fn a_null_name_or_set_member_is_refused_through_the_tools_as_over_http() {
    const EMPTY_NAME: &str = "ExpressionAttributeNames contains invalid value: \
                              Empty attribute name for key #n";
    const NAMES_WITHOUT_EXPRESSIONS: &str =
        "ExpressionAttributeNames can only be specified when using expressions";
    const NULL_STRING: &str = "One or more parameter values were invalid: \
                               An string set may not have a null string as a member";
    let mut child = mcp_with_table("NullNames");
    let cases: Vec<(&str, Value, (String, String))> = vec![
        (
            "put_item",
            json!({"table_name": "NullNames", "item": {"pk": {"S": "a"}},
                "condition_expression": "#n = :a", "expression_attribute_names": {"#n": null},
                "expression_attribute_values": {":a": {"S": "x"}}}),
            validation_error(EMPTY_NAME),
        ),
        (
            "put_item",
            json!({"table_name": "NullNames", "item": {"pk": {"S": "a"}},
                "expression_attribute_names": {"#n": null}}),
            validation_error(NAMES_WITHOUT_EXPRESSIONS),
        ),
        (
            "get_item",
            json!({"table_name": "NullNames", "key": {"pk": {"S": "a"}},
                "projection_expression": "#n", "expression_attribute_names": {"#n": null}}),
            validation_error(EMPTY_NAME),
        ),
        (
            "update_item",
            json!({"table_name": "NullNames", "key": {"pk": {"S": "a"}},
                "update_expression": "SET #n = :a", "expression_attribute_names": {"#n": null},
                "expression_attribute_values": {":a": {"S": "x"}}}),
            validation_error(EMPTY_NAME),
        ),
        (
            "delete_item",
            json!({"table_name": "NullNames", "key": {"pk": {"S": "a"}},
                "condition_expression": "#n = :a", "expression_attribute_names": {"#n": null},
                "expression_attribute_values": {":a": {"S": "x"}}}),
            validation_error(EMPTY_NAME),
        ),
        (
            "query",
            json!({"table_name": "NullNames", "key_condition_expression": "#n = :a",
                "expression_attribute_names": {"#n": null},
                "expression_attribute_values": {":a": {"S": "x"}}}),
            validation_error(EMPTY_NAME),
        ),
        (
            "scan",
            json!({"table_name": "NullNames", "filter_expression": "#n = :a",
                "expression_attribute_names": {"#n": null},
                "expression_attribute_values": {":a": {"S": "x"}}}),
            validation_error(EMPTY_NAME),
        ),
        (
            "put_item",
            json!({"table_name": "NullNames", "item": {"pk": {"S": "a"}, "s": {"SS": [null]}}}),
            validation_error(NULL_STRING),
        ),
        (
            "query",
            json!({"table_name": "NullNames", "key_condition_expression": "pk = :a",
                "expression_attribute_values": {":a": {"SS": [null]}}}),
            named(NULL_STRING, ":a"),
        ),
        (
            "transact_get_items",
            json!({"transact_items": [{"Get": {"TableName": "NullNames", "Key": {"pk": {"S": "a"}},
                "ProjectionExpression": "#n", "ExpressionAttributeNames": {"#n": null}}}]}),
            validation_error(EMPTY_NAME),
        ),
        // An item attribute named like a legacy request member is an
        // ordinary attribute: a boolean written as a string in it puts the
        // call on the older layer, so a bad number beside it is bare.
        (
            "put_item",
            json!({"table_name": "NullNames", "item": {"pk": {"S": "a"},
                "Expected": {"BOOL": "true"}, "n": {"N": "abc"}}}),
            validation_error("The parameter cannot be converted to a numeric value: abc"),
        ),
    ];
    let checked = cases.len();
    let mut mismatches = Vec::new();
    for (id, (tool, args, refusal)) in (10..).zip(cases) {
        let resp = call_tool(&mut child, id, tool, args.clone());
        let answer = tool_error(&resp);
        if answer != refusal {
            mismatches.push(format!("{tool} {args}: {answer:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of {checked} answers differ: {mismatches:#?}",
        mismatches.len()
    );

    // A `Get` with no expression whose names hold a null goes on to the
    // table, as over HTTP, whatever its other names.
    let resp = call_tool(
        &mut child,
        99,
        "transact_get_items",
        json!({"transact_items": [{"Get": {"TableName": "NullNames", "Key": {"pk": {"S": "a"}},
            "ExpressionAttributeNames": {"#n": null, "#m": "x"}}}]}),
    );
    assert!(!is_tool_error(&resp), "{resp}");

    drop(child.stdin.take());
    let _ = child.wait();
}

/// Expression values that aren't a map of values are refused as the HTTP API
/// refuses the same body, and `delete_item` deletes nothing.
#[test]
fn malformed_expression_values_are_refused_and_delete_nothing() {
    let mut child = mcp_with_table("Shapes");
    let resp = call_tool(
        &mut child,
        2,
        "put_item",
        json!({"table_name": "Shapes", "item": {"pk": {"S": "a"}}}),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    let cases = [
        (json!("x"), "Unexpected field type"),
        (json!(5), "Unexpected field type"),
        (
            json!([]),
            "Unrecognized collection type java.util.Map<java.lang.String, \
             com.amazonaws.dynamodb.v20120810.AttributeValue>",
        ),
        (json!({":v": "x"}), "Unexpected value type in payload"),
    ];
    let mut id = 10;
    for (values, message) in cases {
        for tool in ["delete_item", "put_item"] {
            id += 1;
            let mut args = json!({
                "table_name": "Shapes", "condition_expression": "attribute_exists(pk)",
                "expression_attribute_values": values
            });
            let target = if tool == "delete_item" { "key" } else { "item" };
            args[target] = json!({"pk": {"S": "a"}});
            let resp = call_tool(&mut child, id, tool, args);
            assert_eq!(
                tool_error(&resp),
                serialization_error(message),
                "{tool} {values}"
            );
        }
    }
    let resp = call_tool(
        &mut child,
        50,
        "get_item",
        json!({"table_name": "Shapes", "key": {"pk": {"S": "a"}}}),
    );
    assert_eq!(tool_content(&resp)["Item"], json!({"pk": {"S": "a"}}));

    drop(child.stdin.take());
    let _ = child.wait();
}

/// A valid expression value reaches the condition or filter it is used in:
/// a condition on it decides whether `put_item` and `delete_item` write, and
/// a filter on it decides what `scan` returns.
#[test]
fn expression_values_decide_conditions_and_filters() {
    let mut child = mcp_with_table("Decided");
    let mut id = 1;
    let mut call = |child: &mut std::process::Child, tool: &str, args: Value| {
        id += 1;
        call_tool(child, id, tool, args)
    };
    for (pk, v) in [("a", "1"), ("b", "2")] {
        let resp = call(
            &mut child,
            "put_item",
            json!({"table_name": "Decided", "item": {"pk": {"S": pk}, "v": {"N": v}}}),
        );
        assert!(!is_tool_error(&resp), "{resp}");
    }
    let conditional = |tool: &str, v: &str| {
        let mut args = json!({
            "table_name": "Decided", "condition_expression": "v = :v",
            "expression_attribute_values": {":v": {"N": v}}
        });
        if tool == "put_item" {
            args["item"] = json!({"pk": {"S": "a"}, "v": {"N": "1"}, "w": {"S": "put"}});
        } else {
            args["key"] = json!({"pk": {"S": "a"}});
        }
        args
    };
    for tool in ["put_item", "delete_item"] {
        let resp = call(&mut child, tool, conditional(tool, "9"));
        assert_eq!(
            tool_content(&resp)["error_type"],
            "ConditionalCheckFailedException",
            "{tool}: {resp}"
        );
        let resp = call(&mut child, tool, conditional(tool, "1"));
        assert!(!is_tool_error(&resp), "{tool}: {resp}");
    }
    let resp = call(
        &mut child,
        "get_item",
        json!({"table_name": "Decided", "key": {"pk": {"S": "a"}}}),
    );
    assert!(tool_content(&resp).get("Item").is_none(), "{resp}");
    let resp = call(
        &mut child,
        "scan",
        json!({"table_name": "Decided", "filter_expression": "v = :v",
            "expression_attribute_values": {":v": {"N": "2"}}}),
    );
    assert_eq!(
        tool_content(&resp)["Items"],
        json!([{"pk": {"S": "b"}, "v": {"N": "2"}}])
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `query` and `scan` page with an `exclusive_start_key` taken from the last
/// page's `LastEvaluatedKey`, and every item comes back once.
#[test]
fn query_and_scan_page_from_the_last_evaluated_key() {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    let resp = call_tool(
        &mut child,
        1,
        "create_table",
        json!({
            "table_name": "Pages",
            "billing_mode": "PAY_PER_REQUEST",
            "key_schema": [
                {"attribute_name": "pk", "key_type": "HASH"},
                {"attribute_name": "sk", "key_type": "RANGE"}
            ],
            "attribute_definitions": [
                {"attribute_name": "pk", "attribute_type": "S"},
                {"attribute_name": "sk", "attribute_type": "N"}
            ]
        }),
    );
    assert!(!is_tool_error(&resp), "{resp}");
    let mut id = 1;
    for sk in 1..=5 {
        id += 1;
        let resp = call_tool(
            &mut child,
            id,
            "put_item",
            json!({"table_name": "Pages", "item": {"pk": {"S": "p"}, "sk": {"N": sk.to_string()}}}),
        );
        assert!(!is_tool_error(&resp), "{resp}");
    }
    for tool in ["query", "scan"] {
        let mut seen = Vec::new();
        let mut start = Value::Null;
        loop {
            let mut args = json!({"table_name": "Pages", "limit": 2});
            if tool == "query" {
                args["key_condition_expression"] = json!("pk = :p");
                args["expression_attribute_values"] = json!({":p": {"S": "p"}});
            }
            if !start.is_null() {
                args["exclusive_start_key"] = start.clone();
            }
            id += 1;
            let resp = call_tool(&mut child, id, tool, args);
            assert!(!is_tool_error(&resp), "{tool}: {resp}");
            let page = tool_content(&resp);
            for item in page["Items"].as_array().unwrap() {
                seen.push(item["sk"]["N"].as_str().unwrap().to_string());
            }
            start = page["LastEvaluatedKey"].clone();
            if start.is_null() {
                break;
            }
            assert!(seen.len() <= 5, "{tool} pages past the end: {seen:?}");
        }
        assert_eq!(seen, ["1", "2", "3", "4", "5"], "{tool}");
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// The same request over MCP and over the HTTP API
// ---------------------------------------------------------------------------

/// What a request came back with, on either surface.
#[derive(Debug, PartialEq)]
enum Answer {
    Ok(Value),
    Err(String, String),
}

/// A tool call's answer. A call the argument decoder refuses, before the
/// tool runs, comes back as plain text rather than a tool error's JSON.
fn mcp_answer(resp: &Value) -> Answer {
    if let Some(message) = resp["error"]["message"].as_str() {
        return Answer::Err("JSON-RPC error".to_string(), message.to_string());
    }
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default();
    if serde_json::from_str::<Value>(text).is_err() {
        return Answer::Err("Unparsed arguments".to_string(), text.to_string());
    }
    if is_tool_error(resp) {
        let (error_type, message) = tool_error(resp);
        Answer::Err(error_type, message)
    } else {
        Answer::Ok(tool_content(resp))
    }
}

/// A `dynoxide` HTTP server from the same build, so a test can ask the HTTP
/// API for its answer to the request it sends a tool.
struct HttpApi {
    port: u16,
    _server: common::Server,
}

impl HttpApi {
    fn start() -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
        command.args(["--port", "0"]).stdin(Stdio::null());
        let server = common::Server::spawn(command);
        let port = server.port(common::Listener::Http);
        Self {
            port,
            _server: server,
        }
    }

    /// Send `operation` with `body` and return the status and parsed body.
    fn call(&self, operation: &str, body: &Value) -> (u16, Value) {
        use std::io::Read;
        let body = body.to_string();
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        let request = format!(
            "POST / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\
             X-Amz-Target: DynamoDB_20120810.{operation}\r\n\
             Content-Type: application/x-amz-json-1.0\r\n\
             Authorization: AWS4-HMAC-SHA256 Credential=key/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=sig\r\n\
             X-Amz-Date: 20260101T000000Z\r\n\
             Content-Length: {len}\r\n\r\n{body}",
            port = self.port,
            len = body.len(),
        );
        stream.write_all(request.as_bytes()).unwrap();
        let mut raw = String::new();
        stream.read_to_string(&mut raw).unwrap();
        let (head, body) = raw.split_once("\r\n\r\n").expect("an HTTP response");
        let status = head
            .split(' ')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .expect("a status line");
        (status, serde_json::from_str(body).unwrap_or(Value::Null))
    }

    /// The HTTP API's answer, in the shape [`mcp_answer`] gives a tool's.
    fn answer(&self, operation: &str, body: &Value) -> Answer {
        let (status, body) = self.call(operation, body);
        if status == 200 {
            return Answer::Ok(body);
        }
        let error_type = body["__type"].as_str().unwrap_or_default();
        let error_type = error_type
            .rsplit('#')
            .next()
            .unwrap_or_default()
            .to_string();
        let message = body
            .get("message")
            .or_else(|| body.get("Message"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        Answer::Err(error_type, message)
    }
}

/// An MCP server and an HTTP server, each holding a hash-only table called
/// `table` with the same items in it.
fn both_with_items(table: &str, items: &[Value]) -> (std::process::Child, HttpApi) {
    let mut child = mcp_with_table(table);
    let http = HttpApi::start();
    let (status, body) = http.call(
        "CreateTable",
        &json!({
            "TableName": table,
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        }),
    );
    assert_eq!(status, 200, "{body}");
    for (id, item) in (900..).zip(items) {
        let resp = call_tool(
            &mut child,
            id,
            "put_item",
            json!({"table_name": table, "item": item}),
        );
        assert!(!is_tool_error(&resp), "{resp}");
        let (status, body) = http.call("PutItem", &json!({"TableName": table, "Item": item}));
        assert_eq!(status, 200, "{body}");
    }
    (child, http)
}

/// Send each `(tool, arguments, operation, body)` to both surfaces and list
/// the cases whose answers differ, with both answers.
fn differences(
    child: &mut std::process::Child,
    http: &HttpApi,
    cases: Vec<(&str, Value, &str, Value)>,
) -> Vec<String> {
    let mut differ = Vec::new();
    for (id, (tool, args, operation, body)) in (1000..).zip(cases) {
        let over_mcp = mcp_answer(&call_tool(child, id, tool, args.clone()));
        let over_http = http.answer(operation, &body);
        if over_mcp != over_http {
            differ.push(format!(
                "{tool} {args}\n  MCP:  {over_mcp:?}\n  HTTP: {over_http:?}"
            ));
        }
    }
    differ
}

/// One request member: (tool argument, wire member, value).
type Member<'a> = (&'a str, &'a str, Value);

/// A tool's arguments and the HTTP body for the same request, from pairs of
/// (tool argument, wire member, value).
fn same_request(members: &[Member]) -> (Value, Value) {
    let mut args = json!({});
    let mut body = json!({});
    for (arg, member, value) in members {
        args[*arg] = value.clone();
        body[*member] = value.clone();
    }
    (args, body)
}

/// `query` and `scan` read their arguments as Query and Scan read the same
/// members over HTTP: `limit`, `segment` and `total_segments` given as a
/// negative, a fraction, a value past 32 bits or a string; an expression
/// value or start key that is refused, beside a bad
/// `return_consumed_capacity` or alone. Every expectation is the HTTP API's
/// answer to the same request.
#[test]
fn query_and_scan_answer_as_the_http_api_does() {
    let (mut child, http) = both_with_items("Reads", &[json!({"pk": {"S": "a"}})]);
    let table = ("table_name", "TableName", json!("Reads"));
    let key_condition = (
        "key_condition_expression",
        "KeyConditionExpression",
        json!("pk = :v"),
    );
    let key_value = |v: Value| {
        (
            "expression_attribute_values",
            "ExpressionAttributeValues",
            json!({":v": v}),
        )
    };
    let bogus = (
        "return_consumed_capacity",
        "ReturnConsumedCapacity",
        json!("BOGUS"),
    );
    let mut requests: Vec<(&str, Vec<Member>)> = Vec::new();
    let numbers = [
        json!(-1),
        json!(0),
        json!(1),
        json!(1.5),
        json!(3_000_000_000_u64),
        json!(1e20),
        json!("5"),
    ];
    for n in &numbers {
        requests.push((
            "query",
            vec![
                table.clone(),
                key_condition.clone(),
                key_value(json!({"S": "a"})),
                ("limit", "Limit", n.clone()),
            ],
        ));
        requests.push(("scan", vec![table.clone(), ("limit", "Limit", n.clone())]));
        requests.push((
            "scan",
            vec![
                table.clone(),
                ("segment", "Segment", n.clone()),
                ("total_segments", "TotalSegments", json!(4)),
            ],
        ));
        requests.push((
            "scan",
            vec![
                table.clone(),
                ("segment", "Segment", json!(0)),
                ("total_segments", "TotalSegments", n.clone()),
            ],
        ));
    }
    // A limit read as 32 bits beside a bad mode moves to the older wording.
    requests.push((
        "scan",
        vec![table.clone(), ("limit", "Limit", json!(1.5)), bogus.clone()],
    ));
    requests.push((
        "query",
        vec![
            table.clone(),
            key_condition.clone(),
            key_value(json!({"S": "a"})),
            ("limit", "Limit", json!(-1)),
            bogus.clone(),
        ],
    ));
    let refused = [
        json!({"S": null}),
        json!({"NULL": "no"}),
        json!({"N": "abc"}),
        json!({"S": "a", "N": "1"}),
        json!({"S": ""}),
    ];
    for value in &refused {
        for with_mode in [false, true] {
            let mut query = vec![
                table.clone(),
                key_condition.clone(),
                key_value(value.clone()),
            ];
            let mut scan = vec![
                table.clone(),
                ("filter_expression", "FilterExpression", json!("x = :v")),
                key_value(value.clone()),
            ];
            let mut start = vec![
                table.clone(),
                (
                    "exclusive_start_key",
                    "ExclusiveStartKey",
                    json!({"pk": value}),
                ),
            ];
            if with_mode {
                query.push(bogus.clone());
                scan.push(bogus.clone());
                start.push(bogus.clone());
            }
            requests.push(("query", query));
            requests.push(("scan", scan));
            requests.push(("scan", start));
        }
    }
    requests.push((
        "scan",
        vec![
            table.clone(),
            ("exclusive_start_key", "ExclusiveStartKey", json!("x")),
        ],
    ));
    requests.push(("scan", vec![table.clone(), bogus.clone()]));
    requests.push((
        "query",
        vec![
            table.clone(),
            key_condition.clone(),
            key_value(json!({"S": "a"})),
            bogus.clone(),
        ],
    ));

    let checked = requests.len();
    let cases = requests
        .into_iter()
        .map(|(tool, members)| {
            let (args, body) = same_request(&members);
            let operation = if tool == "query" { "Query" } else { "Scan" };
            (tool, args, operation, body)
        })
        .collect();
    let differ = differences(&mut child, &http, cases);
    assert!(
        differ.is_empty(),
        "{} of {checked} answers differ:\n{}",
        differ.len(),
        differ.join("\n")
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// `limit` on `execute_partiql` and the stream tools, and `top_k` on
/// `search_vectors`, are read as the HTTP operation reads the same member, so
/// a negative, a fraction, a value past 32 bits or a string gets the HTTP
/// API's answer. The searches and stream reads name nothing that exists, so
/// a value the request reads reaches the same lookup on both surfaces.
#[test]
fn the_other_numeric_arguments_answer_as_the_http_api_does() {
    let (mut child, http) = both_with_items("Counted", &[json!({"pk": {"S": "a"}})]);
    let select = json!("SELECT * FROM \"Counted\"");
    let arn = json!("arn:aws:dynamodb:dynoxide:000000000000:table/Gone/stream/1.0");
    let search = |n: Value| {
        vec![
            ("table_name", "TableName", json!("Gone")),
            ("index_name", "IndexName", json!("vix")),
            ("search_vector", "SearchVector", json!([{"N": "1"}])),
            ("top_k", "TopK", n),
        ]
    };
    let mut requests: Vec<(&str, &str, Vec<Member>)> = Vec::new();
    // A TopK that is not a 32-bit integer and a negative stream limit are
    // answered with serde's text and its position, which the HTTP API passes
    // through; they are checked on their own below.
    for n in [
        json!(-1),
        json!(0),
        json!(1),
        json!(1.5),
        json!(3_000_000_000_u64),
        json!(1e20),
        json!("5"),
    ] {
        requests.push((
            "execute_partiql",
            "ExecuteStatement",
            vec![
                ("statement", "Statement", select.clone()),
                ("limit", "Limit", n.clone()),
            ],
        ));
        if [json!(-1), json!(0), json!(1)].contains(&n) {
            requests.push(("search_vectors", "SearchVectors", search(n.clone())));
        }
        if n != json!(-1) {
            requests.push((
                "list_streams",
                "ListStreams",
                vec![("limit", "Limit", n.clone())],
            ));
            requests.push((
                "describe_stream",
                "DescribeStream",
                vec![
                    ("stream_arn", "StreamArn", arn.clone()),
                    ("limit", "Limit", n.clone()),
                ],
            ));
            requests.push((
                "get_records",
                "GetRecords",
                vec![
                    ("shard_iterator", "ShardIterator", json!("nothing")),
                    ("limit", "Limit", n.clone()),
                ],
            ));
        }
    }
    // SearchVectors checks the capacity mode among its member checks, and
    // reads expression values strictly.
    let mut with_mode = search(json!(0));
    with_mode.push((
        "return_consumed_capacity",
        "ReturnConsumedCapacity",
        json!("BOGUS"),
    ));
    requests.push(("search_vectors", "SearchVectors", with_mode));
    let mut with_mode = search(json!(5));
    with_mode.push((
        "return_consumed_capacity",
        "ReturnConsumedCapacity",
        json!("BOGUS"),
    ));
    requests.push(("search_vectors", "SearchVectors", with_mode));
    let with_value = |value: Value| {
        let mut members = search(json!(5));
        members.push((
            "search_condition_expression",
            "SearchConditionExpression",
            json!("t = :t"),
        ));
        members.push((
            "expression_attribute_values",
            "ExpressionAttributeValues",
            json!({":t": value}),
        ));
        members
    };
    for value in [json!({"N": "abc"}), json!({"S": "a", "N": "1"})] {
        requests.push(("search_vectors", "SearchVectors", with_value(value)));
    }
    let checked = requests.len();
    let cases = requests
        .into_iter()
        .map(|(tool, operation, members)| {
            let (args, body) = same_request(&members);
            (tool, args, operation, body)
        })
        .collect();
    let differ = differences(&mut child, &http, cases);
    assert!(
        differ.is_empty(),
        "{} of {checked} answers differ:\n{}",
        differ.len(),
        differ.join("\n")
    );

    // The answers that carry serde's position: the same text on both
    // surfaces, the request being written the same way.
    let raw = |resp: &Value| -> Value {
        assert!(is_tool_error(resp), "expected a tool error: {resp}");
        let content = tool_content(resp);
        json!([content["error_type"], content["message"]])
    };
    let mut id = 2000;
    let mut positioned = vec![(
        "list_streams",
        "ListStreams",
        vec![("limit", "Limit", json!(-1))],
    )];
    for n in [
        json!(1.5),
        json!(3_000_000_000_u64),
        json!(1e20),
        json!("5"),
    ] {
        positioned.push(("search_vectors", "SearchVectors", search(n)));
    }
    for value in [json!({"S": null}), json!({"X": "a"})] {
        positioned.push(("search_vectors", "SearchVectors", with_value(value)));
    }
    for (tool, operation, members) in positioned {
        let (args, body) = same_request(&members);
        id += 1;
        let over_mcp = raw(&call_tool(&mut child, id, tool, args));
        let (status, body) = http.call(operation, &body);
        assert_eq!(status, 400, "{body}");
        let message = body.get("message").or_else(|| body.get("Message")).unwrap();
        let error_type = body["__type"].as_str().unwrap().rsplit('#').next().unwrap();
        assert_eq!(over_mcp, json!([error_type, message]), "{tool}");
    }

    drop(child.stdin.take());
    let _ = child.wait();
}

/// The item tools read their values first and then check the request's
/// members as the HTTP operation does: a refused key, item or expression
/// value ahead of a bad `return_consumed_capacity`, the member errors listed
/// together where the operation lists them, UpdateItem's single enum error
/// in its own wording, and `return_values_on_condition_check_failure`
/// outside its enum refused. `delete_item` reads its expression values after
/// the member checks, as DeleteItem does. Every expectation is the HTTP API's
/// answer to the same request.
#[test]
fn item_tools_check_their_members_as_the_http_api_does() {
    let table = "Members";
    let (mut child, http) = both_with_items(table, &[json!({"pk": {"S": "a"}, "v": {"N": "1"}})]);
    let name = ("table_name", "TableName", json!(table));
    let key = |v: Value| ("key", "Key", json!({"pk": v}));
    let item = |v: Value| ("item", "Item", json!({"pk": {"S": "q"}, "x": v}));
    let values = |v: Value| {
        (
            "expression_attribute_values",
            "ExpressionAttributeValues",
            json!({":x": v}),
        )
    };
    let condition = (
        "condition_expression",
        "ConditionExpression",
        json!("x = :x"),
    );
    let update = ("update_expression", "UpdateExpression", json!("SET y = :x"));
    let mode = |m: &str| {
        (
            "return_consumed_capacity",
            "ReturnConsumedCapacity",
            json!(m),
        )
    };
    let return_values = ("return_values", "ReturnValues", json!("BAD"));
    let on_failure = (
        "return_values_on_condition_check_failure",
        "ReturnValuesOnConditionCheckFailure",
        json!("X"),
    );
    let fine = json!({"S": "a"});
    let mut requests: Vec<(&str, &str, Vec<Member>)> = Vec::new();
    // Values refused beside a bad mode.
    for bad in [
        json!({"N": "abc"}),
        json!({"S": "a", "N": "1"}),
        json!({"BOOL": "no"}),
        json!({"S": ""}),
        json!({"NULL": "no"}),
    ] {
        requests.push((
            "get_item",
            "GetItem",
            vec![name.clone(), key(bad.clone()), mode("BOGUS")],
        ));
        requests.push((
            "put_item",
            "PutItem",
            vec![name.clone(), item(bad.clone()), mode("BOGUS")],
        ));
        requests.push((
            "put_item",
            "PutItem",
            vec![
                name.clone(),
                item(fine.clone()),
                condition.clone(),
                values(bad.clone()),
                mode("BOGUS"),
            ],
        ));
        requests.push((
            "update_item",
            "UpdateItem",
            vec![
                name.clone(),
                key(bad.clone()),
                update.clone(),
                values(json!({"N": "1"})),
                mode("BOGUS"),
            ],
        ));
        requests.push((
            "update_item",
            "UpdateItem",
            vec![
                name.clone(),
                key(fine.clone()),
                update.clone(),
                values(bad.clone()),
                mode("BOGUS"),
            ],
        ));
        requests.push((
            "delete_item",
            "DeleteItem",
            vec![name.clone(), key(bad.clone()), mode("BOGUS")],
        ));
        for with_mode in [false, true] {
            let mut members = vec![
                name.clone(),
                key(json!({"S": "z"})),
                condition.clone(),
                values(bad.clone()),
            ];
            if with_mode {
                members.push(mode("BOGUS"));
            }
            requests.push(("delete_item", "DeleteItem", members));
        }
        requests.push((
            "batch_write_item",
            "BatchWriteItem",
            vec![
                (
                    "request_items",
                    "RequestItems",
                    json!({table: [{"PutRequest": {"Item": {"pk": {"S": "q"}, "x": bad}}}]}),
                ),
                mode("BOGUS"),
            ],
        ));
    }
    // The members alone and together.
    let enums: Vec<Vec<(&str, &str, Value)>> = vec![
        vec![mode("BOGUS")],
        vec![return_values.clone()],
        vec![return_values.clone(), mode("BOGUS")],
        vec![on_failure.clone()],
        vec![on_failure.clone(), mode("BOGUS")],
        vec![return_values.clone(), on_failure.clone(), mode("BOGUS")],
    ];
    for members in enums {
        let with = |mut base: Vec<(&'static str, &'static str, Value)>| {
            base.extend(members.iter().cloned());
            base
        };
        requests.push((
            "put_item",
            "PutItem",
            with(vec![name.clone(), item(fine.clone())]),
        ));
        requests.push((
            "update_item",
            "UpdateItem",
            with(vec![
                name.clone(),
                key(fine.clone()),
                update.clone(),
                values(json!({"N": "1"})),
            ]),
        ));
        requests.push((
            "delete_item",
            "DeleteItem",
            with(vec![name.clone(), key(json!({"S": "z"}))]),
        ));
    }
    for tool_and_operation in [
        ("get_item", "GetItem"),
        ("delete_item", "DeleteItem"),
        ("update_item", "UpdateItem"),
    ] {
        requests.push((
            tool_and_operation.0,
            tool_and_operation.1,
            vec![
                ("table_name", "TableName", json!("ab")),
                key(fine.clone()),
                mode("BOGUS"),
            ],
        ));
    }
    requests.push((
        "put_item",
        "PutItem",
        vec![
            ("table_name", "TableName", json!("ab")),
            item(fine.clone()),
            mode("BOGUS"),
        ],
    ));

    let checked = requests.len();
    let cases = requests
        .into_iter()
        .map(|(tool, operation, members)| {
            let (args, body) = same_request(&members);
            (tool, args, operation, body)
        })
        .collect();
    let differ = differences(&mut child, &http, cases);
    assert!(
        differ.is_empty(),
        "{} of {checked} answers differ:\n{}",
        differ.len(),
        differ.join("\n")
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// A tool error's type and message as given, serde's position included, for
/// the answers the HTTP API gives with one.
fn mcp_answer_as_given(resp: &Value) -> Answer {
    if !is_tool_error(resp) {
        return mcp_answer(resp);
    }
    let content = tool_content(resp);
    Answer::Err(
        content["error_type"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        content["message"].as_str().unwrap_or_default().to_string(),
    )
}

/// `batch_get_item` and `batch_write_item` decode `request_items` as their
/// operations' requests do, after the snake_case spellings the tools accept
/// are given their wire names, so a malformed entry gets the HTTP API's
/// answer for the same request items. The HTTP API's answers for several of
/// these differ from DynamoDB's and will change; the tools follow them, so
/// each is compared with the HTTP API's answer here rather than pinned.
#[test]
fn batch_request_items_answer_as_the_http_api_does() {
    let table = "Batch";
    let (mut child, http) = both_with_items(table, &[json!({"pk": {"S": "a"}})]);
    let keys = json!([{"pk": {"S": "a"}}]);
    let item = |pk: &str| json!({"pk": {"S": pk}});
    // (tool, the tool's request_items, operation, the wire's RequestItems)
    let mut cases: Vec<(&str, Value, &str, Value)> = Vec::new();
    let mut same = |tool: &'static str, operation: &'static str, items: Value| {
        cases.push((tool, items.clone(), operation, items));
    };
    for items in [
        json!("x"),
        json!([]),
        json!({}),
        json!({table: {}}),
        json!({table: "x"}),
        json!({table: []}),
        json!({table: {"Keys": "x"}}),
        json!({table: {"Keys": [1]}}),
        json!({table: {"Keys": ["x"]}}),
        json!({table: {"Keys": [{"pk": "x"}]}}),
        json!({table: {"Keys": null}}),
        json!({table: {"Keys": keys, "ProjectionExpression": 5}}),
        json!({table: {"Keys": keys, "ExpressionAttributeNames": "x"}}),
        json!({table: {"Keys": keys, "ExpressionAttributeNames": {"#a": 5}}}),
        json!({table: {"Keys": keys, "ConsistentRead": "x"}}),
        json!({table: {"Keys": keys, "AttributesToGet": "pk"}}),
        json!({table: {"Keys": keys, "AttributesToGet": ["pk"]}}),
        json!({table: {"Keys": keys}}),
    ] {
        same("batch_get_item", "BatchGetItem", items);
    }
    for items in [
        json!("x"),
        json!([]),
        json!({}),
        json!({table: "x"}),
        json!({table: {}}),
        json!({table: []}),
        json!({table: [1]}),
        json!({table: ["x"]}),
        json!({table: [[]]}),
        json!({table: [{}]}),
        json!({table: [null]}),
        json!({table: [{"PutRequest": "x"}]}),
        json!({table: [{"PutRequest": {}}]}),
        json!({table: [{"DeleteRequest": {}}]}),
        json!({table: [{"PutRequest": {"Item": "x"}}]}),
        json!({table: [{"PutRequest": {"Item": {"pk": "x"}}}]}),
        json!({table: [{"DeleteRequest": {"Key": "x"}}]}),
        json!({table: [{"PutRequest": {"Item": item("b")}, "DeleteRequest": {"Key": item("a")}}]}),
        json!({table: [{"PutRequest": {"Item": item("b")}}]}),
    ] {
        same("batch_write_item", "BatchWriteItem", items);
    }
    // The snake_case spellings, given their wire names; where an entry gives
    // both, the wire spelling is the one read.
    cases.push((
        "batch_get_item",
        json!({table: {"keys": keys, "projection_expression": "pk", "consistent_read": "yes"}}),
        "BatchGetItem",
        json!({table: {"Keys": keys, "ProjectionExpression": "pk", "ConsistentRead": "yes"}}),
    ));
    cases.push((
        "batch_get_item",
        json!({table: {"keys": keys, "projection_expression": 5}}),
        "BatchGetItem",
        json!({table: {"Keys": keys, "ProjectionExpression": 5}}),
    ));
    cases.push((
        "batch_get_item",
        json!({table: {"keys": keys, "expression_attribute_names": "x"}}),
        "BatchGetItem",
        json!({table: {"Keys": keys, "ExpressionAttributeNames": "x"}}),
    ));
    cases.push((
        "batch_get_item",
        json!({table: {"projection_expression": "pk"}}),
        "BatchGetItem",
        json!({table: {"ProjectionExpression": "pk"}}),
    ));
    cases.push((
        "batch_get_item",
        json!({table: {"Keys": keys, "keys": "x"}}),
        "BatchGetItem",
        json!({table: {"Keys": keys}}),
    ));
    cases.push((
        "batch_write_item",
        json!({table: [
            {"put_request": {"item": item("c")}},
            {"delete_request": {"key": item("b")}}
        ]}),
        "BatchWriteItem",
        json!({table: [
            {"PutRequest": {"Item": item("c")}},
            {"DeleteRequest": {"Key": item("b")}}
        ]}),
    ));
    cases.push((
        "batch_write_item",
        json!({table: [{"put_request": {}}]}),
        "BatchWriteItem",
        json!({table: [{"PutRequest": {}}]}),
    ));
    cases.push((
        "batch_write_item",
        json!({table: [{"PutRequest": {"Item": item("d"), "item": "x"}, "put_request": "x"}]}),
        "BatchWriteItem",
        json!({table: [{"PutRequest": {"Item": item("d")}}]}),
    ));

    let checked = cases.len();
    let mut differ = Vec::new();
    for (id, (tool, items, operation, wire)) in (3000..).zip(cases) {
        let over_mcp = mcp_answer_as_given(&call_tool(
            &mut child,
            id,
            tool,
            json!({"request_items": items}),
        ));
        let over_http = http.answer(operation, &json!({"RequestItems": wire}));
        if over_mcp != over_http {
            differ.push(format!(
                "{tool} {items}\n  MCP:  {over_mcp:?}\n  HTTP: {over_http:?}"
            ));
        }
    }
    assert!(
        differ.is_empty(),
        "{} of {checked} answers differ:\n{}",
        differ.len(),
        differ.join("\n")
    );

    drop(child.stdin.take());
    let _ = child.wait();
}

/// Run one request on both surfaces during setup, failing if either refuses.
fn on_both(
    child: &mut std::process::Child,
    http: &HttpApi,
    id: u64,
    call: (&str, Value, &str, Value),
) {
    let (tool, args, operation, body) = call;
    let resp = call_tool(child, id, tool, args);
    assert!(!is_tool_error(&resp), "{tool}: {resp}");
    let (status, answer) = http.call(operation, &body);
    assert_eq!(status, 200, "{operation}: {answer}");
}

/// Two copies of the same data, one behind a `dynoxide mcp` subprocess and
/// one behind the HTTP API: `Rich` (pk, sk) with a sparse index `byG` on
/// (g, sk), and `Vecs` with a vector index `vix` on `emb`.
fn both_with_rich_data() -> (std::process::Child, HttpApi) {
    let mut child = spawn_mcp();
    init_mcp(&mut child);
    let http = HttpApi::start();
    on_both(
        &mut child,
        &http,
        800,
        (
            "create_table",
            json!({
                "table_name": "Rich",
                "billing_mode": "PAY_PER_REQUEST",
                "key_schema": [
                    {"attribute_name": "pk", "key_type": "HASH"},
                    {"attribute_name": "sk", "key_type": "RANGE"}
                ],
                "attribute_definitions": [
                    {"attribute_name": "pk", "attribute_type": "S"},
                    {"attribute_name": "sk", "attribute_type": "N"},
                    {"attribute_name": "g", "attribute_type": "S"}
                ],
                "global_secondary_indexes": [{
                    "index_name": "byG",
                    "key_schema": [
                        {"attribute_name": "g", "key_type": "HASH"},
                        {"attribute_name": "sk", "key_type": "RANGE"}
                    ],
                    "projection": {"projection_type": "ALL"}
                }]
            }),
            "CreateTable",
            json!({
                "TableName": "Rich",
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "N"},
                    {"AttributeName": "g", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [{
                    "IndexName": "byG",
                    "KeySchema": [
                        {"AttributeName": "g", "KeyType": "HASH"},
                        {"AttributeName": "sk", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }]
            }),
        ),
    );
    on_both(
        &mut child,
        &http,
        801,
        (
            "create_table",
            json!({
                "table_name": "Vecs",
                "billing_mode": "PAY_PER_REQUEST",
                "key_schema": [{"attribute_name": "pk", "key_type": "HASH"}],
                "attribute_definitions": [
                    {"attribute_name": "pk", "attribute_type": "S"},
                    {"attribute_name": "t", "attribute_type": "S"}
                ],
                "vector_indexes": [{
                    "index_name": "vix",
                    "vector_attribute": {"attribute_name": "emb"},
                    "search_schema": [
                        {"attribute_name": "t", "search_schema_element_type": "INLINE_FILTER"}
                    ],
                    "projection": {"projection_type": "ALL"},
                    "dimensions": 3,
                    "distance_function": "COSINE"
                }]
            }),
            "CreateTable",
            json!({
                "TableName": "Vecs",
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "t", "AttributeType": "S"}
                ],
                "VectorIndexes": [{
                    "IndexName": "vix",
                    "VectorAttribute": {"AttributeName": "emb"},
                    "SearchSchema": [
                        {"AttributeName": "t", "SearchSchemaElementType": "INLINE_FILTER"}
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "Dimensions": 3,
                    "DistanceFunction": "COSINE"
                }]
            }),
        ),
    );
    let mut items = Vec::new();
    for sk in 1..=6 {
        let mut item = json!({
            "pk": {"S": "a"},
            "sk": {"N": sk.to_string()},
            "t": {"S": if sk % 2 == 1 { "keep" } else { "drop" }},
            "v": {"N": (sk * 10).to_string()},
            "o": {"S": "other"}
        });
        if sk <= 3 {
            item["g"] = json!({"S": "x"});
        }
        items.push(("Rich", item));
    }
    items.push((
        "Rich",
        json!({"pk": {"S": "b"}, "sk": {"N": "1"}, "g": {"S": "y"}, "t": {"S": "keep"}}),
    ));
    for (pk, emb, t) in [
        ("v1", ["1", "0", "0"], "keep"),
        ("v2", ["0.9", "0.1", "0"], "drop"),
        ("v3", ["0", "1", "0"], "keep"),
        ("v4", ["0", "0", "1"], "drop"),
    ] {
        let emb: Vec<Value> = emb.iter().map(|n| json!({"N": n})).collect();
        items.push((
            "Vecs",
            json!({"pk": {"S": pk}, "emb": {"L": emb}, "t": {"S": t}, "o": {"S": "other"}}),
        ));
    }
    for (id, (table, item)) in (810..).zip(items) {
        on_both(
            &mut child,
            &http,
            id,
            (
                "put_item",
                json!({"table_name": table, "item": item}),
                "PutItem",
                json!({"TableName": table, "Item": item}),
            ),
        );
    }
    (child, http)
}

/// Valid calls that each use one optional argument in a way that changes the
/// result answer as the HTTP API does, whole successful bodies included. A
/// tool that dropped the argument on the way to the operation would answer
/// differently.
#[test]
fn valid_calls_answer_as_the_http_api_does() {
    let (mut child, http) = both_with_rich_data();
    let rich = ("table_name", "TableName", json!("Rich"));
    let vecs = ("table_name", "TableName", json!("Vecs"));
    let on_a = [
        (
            "key_condition_expression",
            "KeyConditionExpression",
            json!("pk = :p"),
        ),
        (
            "expression_attribute_values",
            "ExpressionAttributeValues",
            json!({":p": {"S": "a"}}),
        ),
    ];
    let query = |extra: Vec<Member<'static>>| {
        let mut members = vec![rich.clone(), on_a[0].clone(), on_a[1].clone()];
        for member in extra {
            members.retain(|(arg, _, _)| *arg != member.0);
            members.push(member);
        }
        ("query", "Query", members)
    };
    let key = json!({"pk": {"S": "a"}, "sk": {"N": "3"}});
    let vector = json!([{"N": "1"}, {"N": "0"}, {"N": "0"}]);
    let search = |extra: Vec<Member<'static>>| {
        let mut members = vec![
            vecs.clone(),
            ("index_name", "IndexName", json!("vix")),
            ("search_vector", "SearchVector", vector.clone()),
            ("top_k", "TopK", json!(2)),
        ];
        members.extend(extra);
        ("search_vectors", "SearchVectors", members)
    };

    // A page boundary taken from the HTTP API's own first page.
    let (status, first_page) = http.call(
        "Query",
        &json!({
            "TableName": "Rich", "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "a"}}, "Limit": 2
        }),
    );
    assert_eq!(status, 200, "{first_page}");
    let start_key = first_page["LastEvaluatedKey"].clone();
    assert!(
        start_key.is_object(),
        "the first page ends early: {first_page}"
    );

    let requests: Vec<(&str, &str, Vec<Member>)> = vec![
        query(vec![
            ("filter_expression", "FilterExpression", json!("t = :t")),
            (
                "expression_attribute_values",
                "ExpressionAttributeValues",
                json!({":p": {"S": "a"}, ":t": {"S": "keep"}}),
            ),
        ]),
        query(vec![(
            "projection_expression",
            "ProjectionExpression",
            json!("sk, t"),
        )]),
        query(vec![
            (
                "projection_expression",
                "ProjectionExpression",
                json!("#t, sk"),
            ),
            (
                "expression_attribute_names",
                "ExpressionAttributeNames",
                json!({"#t": "t"}),
            ),
        ]),
        query(vec![
            ("limit", "Limit", json!(2)),
            (
                "exclusive_start_key",
                "ExclusiveStartKey",
                start_key.clone(),
            ),
        ]),
        query(vec![("select", "Select", json!("COUNT"))]),
        query(vec![
            ("select", "Select", json!("SPECIFIC_ATTRIBUTES")),
            ("projection_expression", "ProjectionExpression", json!("v")),
        ]),
        query(vec![
            ("index_name", "IndexName", json!("byG")),
            (
                "key_condition_expression",
                "KeyConditionExpression",
                json!("g = :g"),
            ),
            (
                "expression_attribute_values",
                "ExpressionAttributeValues",
                json!({":g": {"S": "x"}}),
            ),
        ]),
        (
            "scan",
            "Scan",
            vec![
                rich.clone(),
                (
                    "projection_expression",
                    "ProjectionExpression",
                    json!("pk, sk"),
                ),
            ],
        ),
        (
            "scan",
            "Scan",
            vec![
                rich.clone(),
                (
                    "projection_expression",
                    "ProjectionExpression",
                    json!("#o, sk"),
                ),
                (
                    "expression_attribute_names",
                    "ExpressionAttributeNames",
                    json!({"#o": "o"}),
                ),
            ],
        ),
        (
            "scan",
            "Scan",
            vec![rich.clone(), ("index_name", "IndexName", json!("byG"))],
        ),
        (
            "get_item",
            "GetItem",
            vec![
                rich.clone(),
                ("key", "Key", key.clone()),
                ("projection_expression", "ProjectionExpression", json!("t")),
            ],
        ),
        (
            "get_item",
            "GetItem",
            vec![
                rich.clone(),
                ("key", "Key", key.clone()),
                ("projection_expression", "ProjectionExpression", json!("#v")),
                (
                    "expression_attribute_names",
                    "ExpressionAttributeNames",
                    json!({"#v": "v"}),
                ),
            ],
        ),
        search(vec![
            (
                "search_condition_expression",
                "SearchConditionExpression",
                json!("t = :t"),
            ),
            (
                "expression_attribute_values",
                "ExpressionAttributeValues",
                json!({":t": {"S": "keep"}}),
            ),
        ]),
        search(vec![(
            "projection_expression",
            "ProjectionExpression",
            json!("pk"),
        )]),
        // BatchGetItem applies AttributesToGet, so the tool does too.
        (
            "batch_get_item",
            "BatchGetItem",
            vec![(
                "request_items",
                "RequestItems",
                json!({"Rich": {"Keys": [key.clone()], "AttributesToGet": ["t", "sk"]}}),
            )],
        ),
        // The writes come last, as they change the data the reads see.
        (
            "put_item",
            "PutItem",
            vec![
                rich.clone(),
                (
                    "item",
                    "Item",
                    json!({"pk": {"S": "a"}, "sk": {"N": "1"}, "v": {"N": "11"}}),
                ),
                (
                    "condition_expression",
                    "ConditionExpression",
                    json!("v = :v"),
                ),
                (
                    "expression_attribute_values",
                    "ExpressionAttributeValues",
                    json!({":v": {"N": "10"}}),
                ),
                ("return_values", "ReturnValues", json!("ALL_OLD")),
            ],
        ),
        (
            "update_item",
            "UpdateItem",
            vec![
                rich.clone(),
                ("key", "Key", key.clone()),
                ("update_expression", "UpdateExpression", json!("SET w = :w")),
                (
                    "condition_expression",
                    "ConditionExpression",
                    json!("attribute_exists(pk)"),
                ),
                (
                    "expression_attribute_values",
                    "ExpressionAttributeValues",
                    json!({":w": {"S": "new"}}),
                ),
                ("return_values", "ReturnValues", json!("ALL_NEW")),
            ],
        ),
        (
            "update_item",
            "UpdateItem",
            vec![
                rich.clone(),
                ("key", "Key", json!({"pk": {"S": "a"}, "sk": {"N": "9"}})),
                ("update_expression", "UpdateExpression", json!("SET w = :w")),
                (
                    "condition_expression",
                    "ConditionExpression",
                    json!("attribute_exists(pk)"),
                ),
                (
                    "expression_attribute_values",
                    "ExpressionAttributeValues",
                    json!({":w": {"S": "new"}}),
                ),
            ],
        ),
        (
            "put_item",
            "PutItem",
            vec![
                rich.clone(),
                ("item", "Item", json!({"pk": {"S": "a"}, "sk": {"N": "2"}})),
                (
                    "condition_expression",
                    "ConditionExpression",
                    json!("v = :v"),
                ),
                (
                    "expression_attribute_values",
                    "ExpressionAttributeValues",
                    json!({":v": {"N": "999"}}),
                ),
            ],
        ),
    ];

    let checked = requests.len();
    let mut differ = Vec::new();
    for (id, (tool, operation, members)) in (4000..).zip(requests) {
        let (args, body) = same_request(&members);
        let over_mcp = mcp_answer(&call_tool(&mut child, id, tool, args.clone()));
        let over_http = http.answer(operation, &body);
        // Each read and the first two writes are meant to succeed, so an
        // argument the tool dropped can't hide behind an error both give.
        if !matches!(over_http, Answer::Ok(_))
            && !matches!(&over_http, Answer::Err(t, _) if t == "ConditionalCheckFailedException")
        {
            differ.push(format!("{tool} {args}\n  HTTP refused it: {over_http:?}"));
        }
        if over_mcp != over_http {
            differ.push(format!(
                "{tool} {args}\n  MCP:  {over_mcp:?}\n  HTTP: {over_http:?}"
            ));
        }
    }

    // A refusal that carries more than a message: the failed condition's item
    // and a cancelled transaction's reasons, against the HTTP API's `Item`
    // and `CancellationReasons`.
    let failed_put = vec![
        rich.clone(),
        ("item", "Item", json!({"pk": {"S": "a"}, "sk": {"N": "4"}})),
        (
            "condition_expression",
            "ConditionExpression",
            json!("v = :v"),
        ),
        (
            "expression_attribute_values",
            "ExpressionAttributeValues",
            json!({":v": {"N": "999"}}),
        ),
        (
            "return_values_on_condition_check_failure",
            "ReturnValuesOnConditionCheckFailure",
            json!("ALL_OLD"),
        ),
    ];
    let cancelled = vec![(
        "transact_items",
        "TransactItems",
        json!([
            {"Put": {"TableName": "Rich", "Item": {"pk": {"S": "c"}, "sk": {"N": "1"}}}},
            {"ConditionCheck": {
                "TableName": "Rich", "Key": {"pk": {"S": "a"}, "sk": {"N": "5"}},
                "ConditionExpression": "v = :v",
                "ExpressionAttributeValues": {":v": {"N": "999"}},
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }}
        ]),
    )];
    for (id, (tool, operation, members, mcp_field, http_field)) in (4200..).zip([
        ("put_item", "PutItem", failed_put, "item", "Item"),
        (
            "transact_write_items",
            "TransactWriteItems",
            cancelled,
            "cancellation_reasons",
            "CancellationReasons",
        ),
    ]) {
        let (args, body) = same_request(&members);
        let resp = call_tool(&mut child, id, tool, args);
        assert!(is_tool_error(&resp), "{tool} succeeded: {resp}");
        let (status, over_http) = http.call(operation, &body);
        assert_eq!(status, 400, "{over_http}");
        assert!(
            !over_http[http_field].is_null(),
            "{operation} gave no {http_field}, so a match would prove nothing: {over_http}"
        );
        let over_mcp = tool_content(&resp);
        // TransactWriteItems spells its message `Message`.
        let http_message = over_http
            .get("message")
            .or_else(|| over_http.get("Message"));
        if over_mcp[mcp_field] != over_http[http_field]
            || Some(&over_mcp["message"]) != http_message
        {
            differ.push(format!(
                "{tool} {mcp_field}\n  MCP:  {over_mcp}\n  HTTP: {over_http}"
            ));
        }
    }

    // ExecuteStatement pages with each surface's own token, so the second
    // page is asked for separately on each and compared.
    let statement = r#"SELECT * FROM "Rich" WHERE pk = 'a'"#;
    let over_mcp = tool_content(&call_tool(
        &mut child,
        4100,
        "execute_partiql",
        json!({"statement": statement, "limit": 2}),
    ));
    let (status, over_http) = http.call(
        "ExecuteStatement",
        &json!({"Statement": statement, "Limit": 2}),
    );
    assert_eq!(status, 200, "{over_http}");
    assert_eq!(over_mcp["Items"], over_http["Items"]);
    let (mcp_token, http_token) = (
        over_mcp["NextToken"].clone(),
        over_http["NextToken"].clone(),
    );
    assert!(
        http_token.is_string(),
        "the first page ends early: {over_http}"
    );
    let over_mcp = mcp_answer(&call_tool(
        &mut child,
        4101,
        "execute_partiql",
        json!({"statement": statement, "limit": 2, "next_token": mcp_token}),
    ));
    let over_http = http.answer(
        "ExecuteStatement",
        &json!({"Statement": statement, "Limit": 2, "NextToken": http_token}),
    );
    if over_mcp != over_http {
        differ.push(format!(
            "execute_partiql second page\n  MCP:  {over_mcp:?}\n  HTTP: {over_http:?}"
        ));
    }

    assert!(
        differ.is_empty(),
        "{} of {} answers differ:\n{}",
        differ.len(),
        checked + 3,
        differ.join("\n")
    );

    drop(child.stdin.take());
    let _ = child.wait();
}
