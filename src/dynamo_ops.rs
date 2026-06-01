//! The single source of truth for DynamoDB operation names dynoxide knows
//! about.
//!
//! Two callers need this list and used to keep their own copies, which drifted:
//! the HTTP server (to recognise a `X-Amz-Target`) and the wasm engine API (to
//! phrase the "not supported by the preview" message). Both now derive from
//! [`KNOWN_OPERATIONS`], so adding an operation is a one-line edit here.

/// Every DynamoDB operation dynoxide recognises, whether or not a given build
/// implements it. The wasm preview's *supported* subset lives separately in
/// `wasm_api::SUPPORTED_OPS`; this is the wider "is this a real DynamoDB op at
/// all" set.
pub const KNOWN_OPERATIONS: &[&str] = &[
    "CreateTable",
    "DeleteTable",
    "DescribeTable",
    "ListTables",
    "UpdateTable",
    "PutItem",
    "GetItem",
    "DeleteItem",
    "UpdateItem",
    "Query",
    "Scan",
    "BatchGetItem",
    "BatchWriteItem",
    "TransactWriteItems",
    "TransactGetItems",
    "ListStreams",
    "DescribeStream",
    "GetShardIterator",
    "GetRecords",
    "UpdateTimeToLive",
    "DescribeTimeToLive",
    "ExecuteStatement",
    "ExecuteTransaction",
    "BatchExecuteStatement",
    "TagResource",
    "UntagResource",
    "ListTagsOfResource",
];

/// Whether `op` is a DynamoDB operation dynoxide recognises.
pub fn is_known_operation(op: &str) -> bool {
    KNOWN_OPERATIONS.contains(&op)
}
