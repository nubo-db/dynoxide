use crate::errors::{DynoxideError, Result};
use crate::types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, Item, KeySchemaElement, KeyType,
    ScalarAttributeType,
};

/// Per-operation context for table-name validation.
///
/// AWS DynamoDB applies different constraints to `tableName` depending on the
/// operation. CreateTable enforces a minimum of 3 characters and a regex
/// pattern. `ReadWrite` enforces a minimum of 1 character, with the regex
/// pattern only firing on a non-empty invalid name. PutItem, GetItem,
/// UpdateItem, DeleteItem, Query and Scan add a minimum of 3 for a non-empty
/// name on top of it (see `item_table_name_errors`); the batch and transaction
/// operations check their table names as members of the request body (see
/// `member_table_name_errors`). SearchVectors keeps `ReadWrite` as it is: its
/// short-name answer is uncaptured.
#[derive(Copy, Clone, Debug)]
pub enum TableNameContext {
    /// CreateTable: regex pattern + minimum length 3.
    CreateTable,
    /// PutItem and friends: minimum length 1, regex pattern only on non-empty input.
    ReadWrite,
}

/// Validate a DynamoDB table name for a read/write operation.
///
/// Equivalent to `table_name_constraint_errors(Some(name), TableNameContext::ReadWrite)`
/// followed by formatting into the multi-error envelope. CreateTable callers must use
/// `table_name_constraint_errors` directly with `TableNameContext::CreateTable` because
/// CreateTable's full validation produces additional errors that need to be folded into
/// a single envelope.
pub fn validate_table_name(name: &str) -> Result<()> {
    let errors = table_name_constraint_errors(Some(name), TableNameContext::ReadWrite);
    if errors.is_empty() {
        return Ok(());
    }
    let count = errors.len();
    let msg = format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        errors.join("; ")
    );
    Err(DynoxideError::ValidationException(msg))
}

/// Collect table-name constraint errors for the multi-error validation format.
///
/// Returns a (possibly empty) list of error strings. If `table_name` is `None`,
/// a "must not be null" error is emitted. If it is present but invalid, pattern
/// and/or length errors are emitted, gated by `context`.
pub fn table_name_constraint_errors(
    table_name: Option<&str>,
    context: TableNameContext,
) -> Vec<String> {
    let mut errors = Vec::new();
    match table_name {
        None => {
            errors.push(
                "Value null at 'tableName' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            );
        }
        Some(name) => match context {
            TableNameContext::CreateTable => {
                errors.extend(control_table_name_errors(name));
            }
            TableNameContext::ReadWrite => {
                if name.is_empty() {
                    errors.push(older_empty_table_name_rejection());
                } else if !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
                {
                    errors.push(format!(
                        "Value '{}' at 'tableName' failed to satisfy constraint: \
                         Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
                        name
                    ));
                }
                if name.len() > 255 {
                    errors.push(format!(
                        "Value '{}' at 'tableName' failed to satisfy constraint: \
                         Member must have length less than or equal to 255",
                        name
                    ));
                }
            }
        },
    }
    errors
}

/// The newer validation layer's answer for an empty table name: no value echo,
/// the member spelled `TableName`, reported on its own. PutItem, UpdateItem,
/// DeleteItem, GetItem, Query and SearchVectors give it (captured on
/// 2026-10-09 in at least five of seven regions for each), and Scan since the
/// newer layer reached most regions (20 of 33 on 2026-10-20). The older layer
/// gives the `ReadWrite` form, which a Scan still gets when a missing table
/// name or operator, or a coerced integer, hands its request to that layer.
pub(crate) const EMPTY_TABLE_NAME_REJECTION: &str = "Value at 'TableName' failed to satisfy \
     constraint: Member must have length greater than or equal to 1";

fn short_table_name_error(name: &str) -> String {
    format!(
        "Value '{name}' at 'tableName' failed to satisfy constraint: \
         Member must have length greater than or equal to 3"
    )
}

fn long_table_name_error(name: &str) -> String {
    format!(
        "Value '{name}' at 'tableName' failed to satisfy constraint: \
         Member must have length less than or equal to 255"
    )
}

fn table_name_pattern_error(name: &str) -> String {
    format!(
        "Value '{name}' at 'tableName' failed to satisfy constraint: \
         Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
    )
}

/// The first constraint a non-empty table name breaks, as the newer layer
/// reports it: one error per name, the length (3 to 255) before the pattern.
/// A name both short and malformed (`a!`) gets the length error alone
/// (captured 2026-10-09 on PutItem, DeleteItem, GetItem, Query and
/// SearchVectors). The length is in characters: `😀😀` is too short and 128
/// of them aren't too long (captured eu-west-2 and us-east-1, 2026-11-05).
pub(crate) fn first_table_name_error(name: &str) -> Option<String> {
    let length = length_in_characters(name);
    if length < 3 {
        Some(short_table_name_error(name))
    } else if length > 255 {
        Some(long_table_name_error(name))
    } else if !table_name_pattern_holds(name) {
        Some(table_name_pattern_error(name))
    } else {
        None
    }
}

/// The older layer's answer for an empty table name, which it checks with a
/// write's other members (captured on deep PutItem, UpdateItem and DeleteItem
/// requests, eu-west-2 and us-east-1, 2026-11-05).
pub(crate) fn older_empty_table_name_rejection() -> String {
    "Value '' at 'tableName' failed to satisfy constraint: \
     Member must have length greater than or equal to 1"
        .to_string()
}

/// Every length and pattern constraint a non-empty table name breaks, the
/// length first, as the older layer reports them once a write's members pass
/// (captured on deep PutItem, UpdateItem and DeleteItem requests, eu-west-2
/// and us-east-1, 2026-11-05). The length is in UTF-16 units, where the
/// newer layer counts characters: `😀😀` isn't too short here. Compiled where
/// deep requests are read.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
pub(crate) fn older_table_name_errors(name: &str) -> Vec<String> {
    if name.is_empty() {
        return Vec::new();
    }
    control_table_name_errors(name)
}

/// A name's length in characters, the unit DynamoDB counts an index name in,
/// a table name inside a transaction, ListTables' `ExclusiveStartTableName`
/// and the table name the newer layer checks on a single-item operation
/// (captured eu-west-2 and us-east-1, 12 November 2026).
pub(crate) fn length_in_characters(name: &str) -> usize {
    name.chars().count()
}

/// A name's length in UTF-16 units, the unit DynamoDB counts `TableName` in
/// on CreateTable, DescribeTable, DeleteTable, UpdateTable, UpdateTimeToLive
/// and DescribeTimeToLive: `😀` is two of them (captured eu-west-2 and
/// us-east-1, 12 November 2026).
pub(crate) fn length_in_utf16_units(name: &str) -> usize {
    name.encode_utf16().count()
}

/// The length fault for a name `length` long at `path`, if it is outside 3
/// to 255. The caller counts `length` in the unit DynamoDB uses there.
pub(crate) fn name_length_error(name: &str, path: &str, length: usize) -> Option<String> {
    let constraint = if length < 3 {
        "Member must have length greater than or equal to 3"
    } else if length > 255 {
        "Member must have length less than or equal to 255"
    } else {
        return None;
    };
    Some(format!(
        "Value '{name}' at '{path}' failed to satisfy constraint: {constraint}"
    ))
}

/// The pattern fault for a name at `path`, if it breaks `[a-zA-Z0-9_.-]+`.
pub(crate) fn name_pattern_error(name: &str, path: &str) -> Option<String> {
    (!table_name_pattern_holds(name)).then(|| {
        format!(
            "Value '{name}' at '{path}' failed to satisfy constraint: \
             Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
        )
    })
}

/// Both faults a name `length` long at `path` can break, the length first.
pub(crate) fn name_errors_length_first(name: &str, path: &str, length: usize) -> Vec<String> {
    name_length_error(name, path, length)
        .into_iter()
        .chain(name_pattern_error(name, path))
        .collect()
}

/// The faults in an index name at `path` on CreateTable or UpdateTable: the
/// length in characters, then the pattern. An empty name gets the length fault
/// alone, the answer CreateTable and UpdateTable's vector create have always
/// given one; DynamoDB's answer for one is uncaptured.
pub(crate) fn index_name_errors(name: &str, path: &str) -> Vec<String> {
    if name.is_empty() {
        return name_length_error(name, path, 0).into_iter().collect();
    }
    name_errors_length_first(name, path, length_in_characters(name))
}

/// The faults in a `TableName` on CreateTable, DescribeTable, DeleteTable,
/// UpdateTable, UpdateTimeToLive and DescribeTimeToLive: the length in UTF-16
/// units, then the pattern, both listed when both are broken (captured
/// eu-west-2 and us-east-1, 12 November 2026).
pub(crate) fn control_table_name_errors(name: &str) -> Vec<String> {
    name_errors_length_first(name, "tableName", length_in_utf16_units(name))
}

/// The bare refusal DescribeTable, DeleteTable, UpdateTable and
/// DescribeTimeToLive give an empty `TableName`.
pub(crate) const EMPTY_CONTROL_TABLE_NAME_REJECTION: &str =
    "TableName must be at least 3 characters long and at most 255 characters long";

/// The refusal for a `TableName` on DescribeTable, DeleteTable, UpdateTable
/// and DescribeTimeToLive as their bodies are read: the bare sentence for an
/// empty name, and otherwise [`control_table_name_errors`] in the envelope.
pub(crate) fn control_table_name_rejection(name: &str) -> Option<String> {
    if name.is_empty() {
        return Some(EMPTY_CONTROL_TABLE_NAME_REJECTION.to_string());
    }
    format_validation_errors(&control_table_name_errors(name))
}

/// [`control_table_name_errors`] for a request built in code, which these
/// operations and UpdateTimeToLive check in `execute`. An empty name gets the
/// answer [`validate_table_name`] gives it.
pub(crate) fn validate_control_table_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return validate_table_name(name);
    }
    match format_validation_errors(&control_table_name_errors(name)) {
        Some(msg) => Err(DynoxideError::ValidationException(msg)),
        None => Ok(()),
    }
}

/// The table-name errors for PutItem, DeleteItem, GetItem and Query: a missing
/// name as `ReadWrite` reports it, an empty one in the newer wording, and
/// otherwise [`first_table_name_error`]. Callers report the empty name on its
/// own before anything else; the other errors join the request's member
/// errors.
pub(crate) fn item_table_name_errors(table_name: Option<&str>) -> Vec<String> {
    match table_name {
        None => table_name_constraint_errors(None, TableNameContext::ReadWrite),
        Some("") => vec![EMPTY_TABLE_NAME_REJECTION.to_string()],
        Some(name) => first_table_name_error(name).into_iter().collect(),
    }
}

/// [`validate_table_name`] for the operations that take
/// [`item_table_name_errors`], so a request built in Rust gets the same answer
/// as one decoded from a body.
pub(crate) fn validate_item_table_name(name: &str) -> Result<()> {
    match format_validation_errors(&item_table_name_errors(Some(name))) {
        Some(msg) => Err(DynoxideError::ValidationException(msg)),
        None => Ok(()),
    }
}

/// UpdateItem's table-name errors in the newer layer: every constraint the
/// name breaks, the pattern listed before the length, where the other
/// single-item operations stop at the length (captured 2026-10-09, five of
/// seven regions).
pub(crate) fn update_item_table_name_errors(table_name: Option<&str>) -> Vec<String> {
    let name = match table_name {
        None => return table_name_constraint_errors(None, TableNameContext::ReadWrite),
        Some("") => return vec![EMPTY_TABLE_NAME_REJECTION.to_string()],
        Some(name) => name,
    };
    let mut errors = Vec::new();
    if !table_name_pattern_holds(name) {
        errors.push(table_name_pattern_error(name));
    }
    let length = name.chars().count();
    if length < 3 {
        errors.push(short_table_name_error(name));
    } else if length > 255 {
        errors.push(long_table_name_error(name));
    }
    errors
}

/// The rejection for a `ReturnValuesOnConditionCheckFailure` outside
/// `[ALL_OLD, NONE]`, at `path`: `returnValuesOnConditionCheckFailure` on the
/// single-item operations and `ExecuteStatement`, `transactItems.N.member.<action>.`
/// before it in a transaction, and `statements.N.member.` or
/// `transactStatements.N.member.` on a PartiQL batch or transaction. An empty
/// string is refused too. Captured in all seven regions tried, 2026-10-09, and
/// for the PartiQL operations in eu-west-2 and us-east-1, 2026-10-10.
pub(crate) fn return_values_on_condition_check_failure_rejection(
    value: Option<&str>,
    path: &str,
) -> Option<String> {
    let value = value?;
    if ["ALL_OLD", "NONE"].contains(&value) {
        return None;
    }
    Some(format!(
        "Value '{value}' at '{path}' failed to satisfy constraint: \
         Member must satisfy enum value set: [ALL_OLD, NONE]"
    ))
}

/// What DynamoDB says about a `ConditionalOperator` once the request's members
/// are valid. It only joins legacy conditions, so it is refused when there are
/// none and when there is only one; `conditions` is the number of entries in
/// `Expected`, `QueryFilter` or `ScanFilter`. The newer layer checks nothing
/// else about it, so an unknown value is refused here too (captured
/// 2026-10-09: PutItem, UpdateItem, DeleteItem and Query in at least five of
/// seven regions, Scan in all seven for a valid value and, since 2026-10-20, in
/// the 20 of 33 regions on the newer layer for any value).
pub(crate) fn conditional_operator_misuse(
    operator: Option<&str>,
    conditions: usize,
) -> Option<String> {
    let operator = operator?;
    match conditions {
        0 => Some("ConditionalOperator cannot be used without Filter or Expected".to_string()),
        1 => Some(
            "ConditionalOperator can only be used when Filter or Expected has two or more \
             elements"
                .to_string(),
        ),
        // With conditions to join, a value other than AND or OR, lower case
        // and the empty string included, is named with the member and the
        // list in this order. Query and Scan report it bare, the single-item
        // writes in the envelope (captured eu-west-2 and us-east-1,
        // 2026-11-11; on 2026-10-09 the message named neither and listed
        // `[ALL, OR]`).
        _ if !["AND", "OR"].contains(&operator) => Some(format!(
            "Value '{operator}' at 'conditionalOperator' failed to satisfy constraint: \
             Member must satisfy enum value set: [AND, OR]"
        )),
        _ => None,
    }
}

/// How many legacy conditions an `Expected`, `QueryFilter` or `ScanFilter`
/// carries.
pub(crate) fn legacy_condition_count(conditions: Option<&serde_json::Value>) -> usize {
    conditions
        .and_then(|value| value.as_object())
        .map_or(0, |object| object.len())
}

/// Format a list of constraint validation errors into the DynamoDB multi-error format.
///
/// Returns `Some(message)` if there are errors, `None` if empty.
pub fn format_validation_errors(errors: &[String]) -> Option<String> {
    if errors.is_empty() {
        return None;
    }
    if let [single] = errors {
        return Some(envelope_message(single));
    }
    Some(format!(
        "{} validation errors detected: {}",
        errors.len(),
        errors.join("; ")
    ))
}

/// The single-error form of the request-validation envelope. This and
/// `format_validation_errors` are the only producers of the prefix wording.
pub(crate) fn envelope_message(msg: &str) -> String {
    format!("1 validation error detected: {msg}")
}

/// The rejection for a `ReturnConsumedCapacity` outside the enum, or `None` when
/// the value is one DynamoDB accepts.
///
/// The only copy of the enum and its message. `GetItem`, `PutItem`,
/// `UpdateItem`, `DeleteItem`, `Query` and `Scan` each spelled it out during
/// their own deserialisation, and the PartiQL surfaces derive their requests
/// instead, so they had nowhere to put it and accepted anything. That was
/// invisible while the value only chose whether a figure was reported; it
/// stopped being invisible once an unrecognised value also meant the write path
/// skipped sizing its indexes. Every operation that takes the parameter now
/// asks here: those six, `SearchVectors`, the three PartiQL operations, and
/// `BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and
/// `TransactWriteItems`. The batch operations and TransactGetItems check it in
/// their `execute` once their own request-level constraints have passed;
/// TransactWriteItems lists it in the stages of its request check.
///
/// Callers that collect several failures push the message onto their own errors
/// vec, so the multi-error envelope they report is unchanged.
pub(crate) fn return_consumed_capacity_rejection(mode: Option<&str>) -> Option<String> {
    let mode = mode?;
    if ["INDEXES", "TOTAL", "NONE"].contains(&mode) {
        return None;
    }
    Some(format!(
        "Value '{mode}' at 'returnConsumedCapacity' failed to satisfy constraint: \
         Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
    ))
}

/// [`return_consumed_capacity_rejection`] as an error, in the single-error
/// envelope a wire client gets. For callers that check the mode outside a
/// deserialiser, where there is no errors vec to push onto: the MCP tools.
#[cfg(feature = "mcp-server")]
pub(crate) fn reject_bad_capacity_mode(mode: Option<&str>) -> Result<()> {
    match return_consumed_capacity_rejection(mode) {
        Some(msg) => Err(DynoxideError::ValidationException(envelope_message(&msg))),
        None => Ok(()),
    }
}

/// The values `ReturnValues` accepts on a write.
pub(crate) const RETURN_VALUES: [&str; 5] =
    ["ALL_NEW", "UPDATED_OLD", "ALL_OLD", "NONE", "UPDATED_NEW"];

/// DeleteItem's refusal of a `ReturnValues` the enum allows but it does not
/// honour.
pub(crate) const RETURN_VALUES_NOT_HONOURED: &str = "ReturnValues can only be ALL_OLD or NONE";

/// The rejection for a `ReturnValues` outside the enum on PutItem, UpdateItem
/// and DeleteItem: the validation framework's generic form, which names
/// neither the value nor the member and lists the enum in DynamoDB's own
/// order, its `UPDATE_NEW` typo included. Captured in eu-west-2, us-east-1 and
/// two more regions on 2026-10-08; 31 of 33 regions answer this way, and
/// eu-south-2 and me-central-1 still give the older per-value wording. Which
/// values are accepted is unchanged: `UPDATED_NEW` is valid, `UPDATE_NEW` is
/// only the message's spelling.
pub(crate) const RETURN_VALUES_REJECTION: &str = "Failed to satisfy constraint: Member must \
     satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]";

/// The older validation layer's rejection for a `ReturnValues` outside the
/// enum, naming the value and the member. UpdateItem still gives it when the
/// request has no key, in all seven regions captured on 2026-10-09.
pub(crate) fn return_values_value_rejection(value: &str) -> String {
    format!(
        "Value '{value}' at 'returnValues' failed to satisfy constraint: \
         Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]"
    )
}

/// The older validation layer's rejection for a `ConditionalOperator` outside
/// `[OR, AND]`, or `None` when the value is one DynamoDB accepts. It is checked
/// as a member, whatever the number of conditions, and a request reaches that
/// layer only when something else hands it there: a missing table name, a
/// missing UpdateItem or DeleteItem key, a missing Scan operator or a coerced
/// integer. The newer layer words the enum its own way, with the list the
/// other way round; see [`conditional_operator_misuse`].
pub(crate) fn conditional_operator_rejection(operator: Option<&str>) -> Option<String> {
    let operator = operator?;
    if ["OR", "AND"].contains(&operator) {
        return None;
    }
    Some(format!(
        "Value '{operator}' at 'conditionalOperator' failed to satisfy constraint: \
         Member must satisfy enum value set: [OR, AND]"
    ))
}

/// The rejection for a `ReturnItemCollectionMetrics` outside the enum, or
/// `None` when the value is one DynamoDB accepts. `BatchWriteItem` and
/// `TransactWriteItems` ask here; PutItem, UpdateItem and DeleteItem spell the
/// same check out in their own deserialisation.
pub(crate) fn return_item_collection_metrics_rejection(mode: Option<&str>) -> Option<String> {
    let mode = mode?;
    if ["SIZE", "NONE"].contains(&mode) {
        return None;
    }
    Some(format!(
        "Value '{mode}' at 'returnItemCollectionMetrics' failed to satisfy constraint: \
         Member must satisfy enum value set: [SIZE, NONE]"
    ))
}

fn table_name_pattern_holds(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// Whether a table name meets the constraints on a `TableName` member of a
/// request body: 3 to 255 characters from `[a-zA-Z0-9_.-]`. The single-item
/// operations validate a top-level `tableName` more loosely (see
/// [`TableNameContext::ReadWrite`]); the batch and transaction operations carry
/// their table names inside the body and apply the full constraint.
pub(crate) fn member_table_name_is_valid(name: &str) -> bool {
    (3..=255).contains(&length_in_characters(name)) && table_name_pattern_holds(name)
}

/// The constraint errors for a table name carried at `path` inside a
/// transaction, such as `transactItems.2.member.update.tableName`: the pattern
/// first, then the length in characters, which is the order and unit
/// eu-west-2 gives when a name breaks both (captured eu-west-2 and us-east-1,
/// 12 November 2026; us-east-1 listed the length first on 9 October 2026).
pub(crate) fn member_table_name_errors(name: &str, path: &str) -> Vec<String> {
    name_pattern_error(name, path)
        .into_iter()
        .chain(name_length_error(name, path, length_in_characters(name)))
        .collect()
}

/// The first fault in a Query or Scan `IndexName`, ahead of the table lookup:
/// the length in characters, with no value echo and the member spelled
/// `IndexName`, or else the pattern, echoing the value at `indexName`
/// (captured eu-west-2 and us-east-1, 12 November 2026).
pub(crate) fn first_index_name_error(name: &str) -> Option<String> {
    let length = length_in_characters(name);
    let constraint = if length < 3 {
        "Member must have length greater than or equal to 3"
    } else if length > 255 {
        "Member must have length less than or equal to 255"
    } else {
        return name_pattern_error(name, "indexName");
    };
    Some(format!(
        "Value at 'IndexName' failed to satisfy constraint: {constraint}"
    ))
}

/// The constraint a batch request's `RequestItems` map puts on its keys, the
/// table names. DynamoDB reports it once for the whole map, echoing the map,
/// whichever name broke it.
pub(crate) const REQUEST_ITEMS_KEY_CONSTRAINT: &str = "Map keys must satisfy constraint: \
     [Member must have length less than or equal to 255, Member must have length greater \
     than or equal to 3, Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]";

/// Wrap an `EnvelopedValidation` error in the `1 validation error detected: `
/// envelope, converting it to a plain `ValidationException`. Every other error
/// passes through unchanged.
///
/// PutItem and UpdateItem apply this at their operation boundary.
pub(crate) fn envelope_request_validation(err: DynoxideError) -> DynoxideError {
    match err {
        DynoxideError::EnvelopedValidation(msg) => {
            DynoxideError::ValidationException(envelope_message(&msg))
        }
        other => other,
    }
}

/// The layer a PutItem, UpdateItem or DeleteItem request's values are checked
/// on: the older layer when the request was read on it (a wire seam sets that
/// from the whole body, see [`crate::types::reads_on_older_layer`]) or when its
/// legacy `Expected` would put it there, and the current layer otherwise.
pub(crate) fn single_write_layer(expected: Option<&serde_json::Value>) -> ValueLayer {
    if crate::types::current_value_layer() == ValueLayer::Older
        || expected.is_some_and(crate::types::reads_on_older_layer)
    {
        ValueLayer::Older
    } else {
        ValueLayer::Current
    }
}

/// Resolve the request-validation tag at a single-item write's boundary: in
/// the envelope on the current layer, and bare on the older one, which never
/// gives it (captured eu-west-2 and us-east-1, 2026-10-10).
pub(crate) fn resolve_for_layer(layer: ValueLayer, err: DynoxideError) -> DynoxideError {
    match (layer, err) {
        (ValueLayer::Older, DynoxideError::EnvelopedValidation(msg)) => {
            DynoxideError::ValidationException(msg)
        }
        (_, err) => envelope_request_validation(err),
    }
}

/// Tag a plain `ValidationException` for the request-validation envelope, for
/// a PutItem or UpdateItem call site whose shared helper reports it plain.
/// Every other error passes through unchanged.
pub(crate) fn tag_for_envelope(err: DynoxideError) -> DynoxideError {
    match err {
        DynoxideError::ValidationException(msg) => DynoxideError::EnvelopedValidation(msg),
        other => other,
    }
}

/// Convert the wire-invisible `EnvelopedValidation` tag back to a plain,
/// unenveloped `ValidationException`. Every other error passes through
/// unchanged.
///
/// Operations other than PutItem and UpdateItem report the request-validation
/// family bare, and the tag must never reach the wire.
///
/// Compiled only for the surfaces that resolve the tag: the HTTP server, the
/// MCP tools, the wasm engine API, and the unit tests.
#[cfg(any(
    feature = "http-server",
    feature = "mcp-server",
    feature = "wasm-sqlite",
    test
))]
pub(crate) fn strip_request_validation_tag(err: DynoxideError) -> DynoxideError {
    match err {
        DynoxideError::EnvelopedValidation(msg) => DynoxideError::ValidationException(msg),
        other => other,
    }
}

/// Resolve the wire-invisible `EnvelopedValidation` tag for a named operation:
/// PutItem, UpdateItem and DeleteItem wrap the request-validation family in the
/// `1 validation error detected: ` envelope, every other operation reports it
/// bare. Either way the tag never reaches the wire. Enveloping is idempotent
/// for untagged errors, so applying this to an already-enveloped action error
/// is safe.
///
/// Owner of the operation split for the HTTP and wasm dispatch seams, which
/// route through here. The same PutItem/UpdateItem/DeleteItem membership is
/// encoded in two more places: at the action boundaries (`put_item::execute`,
/// `update_item::execute` and `delete_item::execute` apply the envelope for
/// the in-process API), and in the MCP tools' choice of resolver
/// (`resolve_single_write` for the put_item, update_item and delete_item
/// tools, `strip_request_validation_tag` for the rest). A future enveloped
/// operation must update all three.
///
/// Compiled only for the seams that consume it: the HTTP server, the MCP
/// tools, the wasm engine API, and the unit tests.
#[cfg(any(
    feature = "http-server",
    feature = "mcp-server",
    feature = "wasm-sqlite",
    test
))]
pub(crate) fn resolve_request_validation_tag(operation: &str, err: DynoxideError) -> DynoxideError {
    match operation {
        "PutItem" | "UpdateItem" | "DeleteItem" => envelope_request_validation(err),
        _ => strip_request_validation_tag(err),
    }
}

/// Resolve the request-validation tag for `operation` read on `layer`: bare on
/// the older layer, which never gives the envelope, and by the operation's
/// own rule ([`resolve_request_validation_tag`]) on the current one. The one
/// decision the HTTP and wasm dispatch seams and the MCP tools all make.
#[cfg(any(
    feature = "http-server",
    feature = "mcp-server",
    feature = "wasm-sqlite",
    test
))]
pub(crate) fn resolve_request_validation_on(
    operation: &str,
    layer: ValueLayer,
    err: DynoxideError,
) -> DynoxideError {
    match layer {
        ValueLayer::Older => strip_request_validation_tag(err),
        ValueLayer::Current => resolve_request_validation_tag(operation, err),
    }
}

/// A validation failure from a shared validator, classified so PutItem and
/// UpdateItem can tell which families DynamoDB wraps in the
/// `1 validation error detected: ` envelope.
///
/// Every other caller converts it straight back to the original error via the
/// `From` impl, so plain `?` propagation keeps their behaviour byte-identical.
#[derive(Debug)]
pub(crate) enum ClassifiedValidationError {
    /// A family DynamoDB reports bare, or an error adopted from an
    /// unclassified helper.
    Bare(DynoxideError),
    /// A family DynamoDB wraps in the request-validation envelope on PutItem
    /// and UpdateItem.
    Enveloped(String),
}

impl ClassifiedValidationError {
    /// A family DynamoDB reports bare.
    pub(crate) fn bare(message: impl Into<String>) -> Self {
        Self::Bare(DynoxideError::ValidationException(message.into()))
    }

    /// A family DynamoDB wraps in the request-validation envelope on PutItem
    /// and UpdateItem.
    pub(crate) fn enveloped(message: impl Into<String>) -> Self {
        Self::Enveloped(message.into())
    }

    /// Convert for a PutItem/UpdateItem call site: enveloped families become
    /// the wire-invisible `EnvelopedValidation` tag (unwrapped at the
    /// operation boundary), everything else passes through unchanged.
    pub(crate) fn into_tagged(self) -> DynoxideError {
        match self {
            Self::Enveloped(msg) => DynoxideError::EnvelopedValidation(msg),
            Self::Bare(err) => err,
        }
    }
}

impl From<ClassifiedValidationError> for DynoxideError {
    fn from(e: ClassifiedValidationError) -> Self {
        match e {
            ClassifiedValidationError::Enveloped(msg) => DynoxideError::ValidationException(msg),
            ClassifiedValidationError::Bare(err) => err,
        }
    }
}

impl From<DynoxideError> for ClassifiedValidationError {
    /// Errors adopted from unclassified helpers stay bare; only call sites
    /// that know a family is enveloped construct the enveloped form.
    fn from(error: DynoxideError) -> Self {
        Self::Bare(error)
    }
}

/// Validate key schema: exactly one HASH key, optionally one RANGE key.
///
/// DynamoDB validates positionally: the first element must be HASH and, if a
/// second element is present, it must be RANGE.
pub fn validate_key_schema(key_schema: &[KeySchemaElement]) -> Result<()> {
    if key_schema.is_empty() || key_schema.len() > 2 {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: \
             Member must have length less than or equal to 2"
                .to_string(),
        ));
    }

    // First element must be HASH.
    if key_schema[0].key_type != KeyType::HASH {
        return Err(DynoxideError::ValidationException(
            "Invalid KeySchema: The first KeySchemaElement is not a HASH key type".to_string(),
        ));
    }

    // Check for duplicate attribute names (before type check, matching DynamoDB ordering).
    if key_schema.len() == 2 && key_schema[0].attribute_name == key_schema[1].attribute_name {
        return Err(DynoxideError::ValidationException(
            "Both the Hash Key and the Range Key element in the KeySchema have the same name"
                .to_string(),
        ));
    }

    // Second element, if present, must be RANGE.
    if key_schema.len() == 2 && key_schema[1].key_type != KeyType::RANGE {
        return Err(DynoxideError::ValidationException(
            "Invalid KeySchema: The second KeySchemaElement is not a RANGE key type".to_string(),
        ));
    }

    Ok(())
}

/// Validate attribute definitions: types must be S, N, or B.
pub fn validate_attribute_definitions(defs: &[AttributeDefinition]) -> Result<()> {
    if defs.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value null at 'attributeDefinitions' failed to satisfy \
             constraint: Member must have length greater than or equal to 1"
                .to_string(),
        ));
    }

    for def in defs {
        match def.attribute_type {
            ScalarAttributeType::S | ScalarAttributeType::N | ScalarAttributeType::B => {}
        }
    }

    Ok(())
}

/// Attribute names in the order DynamoDB lists them in its index definition
/// messages: the iteration order of the Java hash table it collects them in.
///
/// That order is deterministic for a given set of names: each name lands in
/// bucket `(h ^ (h >>> 16)) & (capacity - 1)` of its `String.hashCode()` `h`,
/// buckets are walked in turn, and names sharing a bucket keep the order they
/// were added in. `sized_to_count` picks the table: one sized to the number of
/// names (`new HashMap<>(n)`), or a default one with sixteen buckets
/// (`new HashSet<>()`). Either grows by doubling once it is more than three
/// quarters full. Captured eu-west-2 and us-east-1, 2026-10-09, over 24 lists of
/// one to sixteen names.
pub(crate) fn java_hash_order<'a>(names: &[&'a str], sized_to_count: bool) -> Vec<&'a str> {
    let mut seen = std::collections::HashSet::with_capacity(names.len());
    let unique: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| seen.insert(*name))
        .collect();
    let initial = if sized_to_count {
        unique.len().max(1).next_power_of_two()
    } else {
        16
    };
    let capacity = java_table_capacity(initial, unique.len());
    let mut ordered: Vec<(usize, usize, &str)> = unique
        .iter()
        .enumerate()
        .map(|(added, name)| (java_hash_bucket(name, capacity), added, *name))
        .collect();
    ordered.sort_unstable();
    ordered.into_iter().map(|(_, _, name)| name).collect()
}

/// How many buckets a Java hash table that starts with `initial` (a power of
/// two) holds once `len` names are in it: it doubles whenever it is more than
/// three quarters full.
pub(crate) fn java_table_capacity(initial: usize, len: usize) -> usize {
    let mut capacity = initial;
    while len * 4 > capacity * 3 {
        capacity *= 2;
    }
    capacity
}

/// The bucket a name lands in in a Java hash table of `capacity` buckets (a
/// power of two): `(h ^ (h >>> 16)) & (capacity - 1)` of its
/// `String.hashCode()` `h`.
pub(crate) fn java_hash_bucket(name: &str, capacity: usize) -> usize {
    let h = name.encode_utf16().fold(0u32, |h, unit| {
        h.wrapping_mul(31).wrapping_add(u32::from(unit))
    });
    ((h ^ (h >> 16)) as usize) & (capacity - 1)
}

/// The error for an index whose key schema names an attribute the request
/// does not define, or `None` when every key attribute is defined.
///
/// The message lists the index's key attributes and the request's definitions
/// when the request defines at least as many attributes as the index has key
/// attributes, and is the generic one otherwise. CreateTable asks this of each
/// index in turn and reports the first; UpdateTable asks it of a new index.
/// Captured eu-west-2 and us-east-1, 2026-10-09.
pub(crate) fn undefined_index_key_error(
    key_schema: &[KeySchemaElement],
    definitions: &[AttributeDefinition],
) -> Option<String> {
    let defined: Vec<&str> = definitions
        .iter()
        .map(|d| d.attribute_name.as_str())
        .collect();
    let keys: Vec<&str> = key_schema
        .iter()
        .map(|k| k.attribute_name.as_str())
        .collect();
    if keys.iter().all(|k| defined.contains(k)) {
        return None;
    }
    let key_count = java_hash_order(&keys, true).len();
    if defined.len() < key_count {
        return Some("Invalid KeySchema: Some index key attribute have no definition".to_string());
    }
    Some(format!(
        "One or more parameter values were invalid: Some index key attributes are not defined \
         in AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
        java_hash_order(&keys, true).join(", "),
        java_hash_order(&defined, true).join(", ")
    ))
}

/// Validate that all key schema attributes are defined in attribute definitions.
pub fn validate_key_attributes_in_definitions(
    key_schema: &[KeySchemaElement],
    definitions: &[AttributeDefinition],
) -> Result<()> {
    for key_elem in key_schema {
        let found = definitions
            .iter()
            .any(|def| def.attribute_name == key_elem.attribute_name);
        if !found {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: Some index key attributes are not \
                 defined in AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
                key_elem.attribute_name,
                definitions
                    .iter()
                    .map(|d| d.attribute_name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }

    Ok(())
}

/// The shape rules for a global secondary index key, which can have up to four
/// partition key (HASH) and four sort key (RANGE) attributes, HASH first, each
/// named once. CreateTable and UpdateTable answer alike. Captured eu-west-2,
/// 2026-10-09. An empty schema gets the length refusal both callers gave it.
pub fn check_gsi_key_schema(key_schema: &[KeySchemaElement]) -> std::result::Result<(), String> {
    const MAX_PER_KIND: usize = 4;
    let Some(first) = key_schema.first() else {
        return Err(
            "1 validation error detected: Value null at 'keySchema' failed to satisfy \
                    constraint: Member must have length less than or equal to 2"
                .to_string(),
        );
    };
    if first.key_type != KeyType::HASH {
        return Err(
            "Invalid KeySchema: The first KeySchemaElement is not a HASH key type".to_string(),
        );
    }
    if let [hash, range] = key_schema
        && range.key_type == KeyType::RANGE
        && hash.attribute_name == range.attribute_name
    {
        return Err(
            "Both the Hash Key and the Range Key element in the KeySchema have the same name"
                .to_string(),
        );
    }
    for (i, element) in key_schema.iter().enumerate() {
        if key_schema[..i]
            .iter()
            .any(|earlier| earlier.attribute_name == element.attribute_name)
        {
            return Err(
                "The KeySchema contains multiple key attributes with the same name".to_string(),
            );
        }
    }
    if key_schema
        .windows(2)
        .any(|w| w[0].key_type == KeyType::RANGE && w[1].key_type == KeyType::HASH)
    {
        return Err(
            "One or more parameter values were invalid: All HASH key attributes must precede \
             RANGE key attributes in the KeySchema"
                .to_string(),
        );
    }
    for (key_type, name) in [(KeyType::HASH, "HASH"), (KeyType::RANGE, "RANGE")] {
        if key_schema.iter().filter(|k| k.key_type == key_type).count() > MAX_PER_KIND {
            return Err(format!(
                "One or more parameter values were invalid: The KeySchema exceeds the maximum \
                 allowed number of {name} key attributes"
            ));
        }
    }
    Ok(())
}

/// Validate a Global Secondary Index definition.
///
/// `request_definitions` must be the AttributeDefinitions declared in the
/// current request, not the table's merged stored set: DynamoDB requires a new
/// index's key attributes to be (re)declared in the request itself.
pub fn validate_gsi(
    gsi: &GlobalSecondaryIndex,
    request_definitions: &[AttributeDefinition],
) -> Result<()> {
    // Validate index name length
    if !(3..=255).contains(&length_in_characters(&gsi.index_name)) {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'globalSecondaryIndexes.1.member.indexName' \
             failed to satisfy constraint: Member must have length greater than or equal to 3",
            gsi.index_name
        )));
    }

    // Validate index name character set (same as table names)
    if !gsi
        .index_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'globalSecondaryIndexes.1.member.indexName' \
             failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
            gsi.index_name
        )));
    }

    // Validate key schema
    check_gsi_key_schema(&gsi.key_schema).map_err(DynoxideError::ValidationException)?;

    // Validate projection
    validate_projection(&gsi.projection, &gsi.index_name)?;

    // Validate GSI key attributes exist in definitions
    if let Some(message) = undefined_index_key_error(&gsi.key_schema, request_definitions) {
        return Err(DynoxideError::ValidationException(message));
    }

    Ok(())
}

/// Validate a single vector index definition.
///
/// `member_path` is the request-model path of the index's member, threaded
/// into the formulaic constraint messages: CreateTable passes
/// `vectorIndexes.N.member` with the entry's real 1-based position, and
/// UpdateTable passes `vectorIndexUpdates.1.member.create` (a call carries at
/// most one create action). The UpdateTable path shape is captured from real
/// DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
///
/// `request_definitions` must be the AttributeDefinitions declared in the
/// current request: SearchSchema attributes must be (re)declared there. The
/// vector attribute itself must NOT be declared in AttributeDefinitions (it
/// is not a key attribute; the list-level validation in CreateTable rejects
/// a request that declares it there).
pub fn validate_vector_index(
    vix: &crate::types::VectorIndex,
    request_definitions: &[AttributeDefinition],
    member_path: &str,
) -> Result<()> {
    // Index name length (request-model constraint; minimum length 3)
    if length_in_characters(&vix.index_name) < 3 {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at '{member_path}.indexName' \
             failed to satisfy constraint: Member must have length greater than or equal to 3",
            vix.index_name
        )));
    }
    if length_in_characters(&vix.index_name) > 255 {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at '{member_path}.indexName' \
             failed to satisfy constraint: Member must have length less than or equal to 255",
            vix.index_name
        )));
    }

    // Index name character set (same as table names)
    if !vix
        .index_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at '{member_path}.indexName' \
             failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
            vix.index_name
        )));
    }

    // Distance function enum (request-model constraint form)
    match vix.distance_function.as_str() {
        "COSINE" | "DOT_PRODUCT" | "EUCLIDEAN" => {}
        other => {
            return Err(DynoxideError::ValidationException(format!(
                "1 validation error detected: Value '{other}' at \
                 '{member_path}.distanceFunction' failed to satisfy constraint: \
                 Member must satisfy enum value set: [COSINE, DOT_PRODUCT, EUCLIDEAN]"
            )));
        }
    }

    // Dimensions bounds. The 0 case rejects earlier at the request-model
    // layer on the JSON path; this bare form covers the upper bound and the
    // programmatic path. Captured from real DynamoDB (eu-west-2, 2026-08-11).
    if vix.dimensions < 1 || vix.dimensions > 4096 {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: Number of dimensions must be between \
             1 and 4096 inclusive."
                .to_string(),
        ));
    }

    // Projection structure
    validate_projection(&vix.projection, &vix.index_name)?;

    // SearchSchema element types and AttributeDefinitions membership. The
    // missing-definition message is captured from real DynamoDB (eu-west-2,
    // 2026-08-11).
    if let Some(ref schema) = vix.search_schema {
        for (elem_idx, elem) in schema.iter().enumerate() {
            let elem_position = elem_idx + 1;
            match elem.search_schema_element_type.as_str() {
                "HASH" | "INLINE_FILTER" => {}
                other => {
                    return Err(DynoxideError::ValidationException(format!(
                        "1 validation error detected: Value '{other}' at \
                         '{member_path}.searchSchema.{elem_position}.member.searchSchemaElementType' \
                         failed to satisfy constraint: Member must satisfy enum value set: \
                         [HASH, INLINE_FILTER]"
                    )));
                }
            }
            if !request_definitions
                .iter()
                .any(|d| d.attribute_name == elem.attribute_name)
            {
                return Err(DynoxideError::ValidationException(
                    "One or more parameter values were invalid: One element in SearchSchema \
                     is not defined in attribute definitions"
                        .to_string(),
                ));
            }
        }

        // At most one HASH element per SearchSchema; the reported value is
        // the actual HASH count. Captured from real DynamoDB (eu-west-2 and
        // us-east-1, 2026-08-12).
        let hash_count = schema
            .iter()
            .filter(|e| e.search_schema_element_type == "HASH")
            .count();
        if hash_count > 1 {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: Value '{hash_count}' at \
                 'SearchSchema' failed to satisfy constraint: Member must have HASH count \
                 less than or equal to 1"
            )));
        }

        // No attribute may appear twice in one SearchSchema. Captured from
        // real DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
        let mut seen = std::collections::HashSet::new();
        for elem in schema {
            if !seen.insert(elem.attribute_name.as_str()) {
                return Err(DynoxideError::ValidationException(
                    "One or more parameter values were invalid: SearchSchema contains a \
                     duplicate AttributeName"
                        .to_string(),
                ));
            }
        }
    }

    Ok(())
}

/// Validate a Projection (for GSI or LSI).
///
/// DynamoDB checks:
/// 1. ProjectionType must be present (not null)
/// 2. If NonKeyAttributes is specified, ProjectionType must be INCLUDE
pub fn validate_projection(projection: &crate::types::Projection, _index_name: &str) -> Result<()> {
    match &projection.projection_type {
        None => {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: Unknown ProjectionType: null"
                    .to_string(),
            ));
        }
        Some(pt) => {
            if let Some(ref nka) = projection.non_key_attributes {
                // NonKeyAttributes is present; check ProjectionType compatibility
                match pt {
                    crate::types::ProjectionType::ALL => {
                        return Err(DynoxideError::ValidationException(
                            "One or more parameter values were invalid: \
                             ProjectionType is ALL, but NonKeyAttributes is specified"
                                .to_string(),
                        ));
                    }
                    crate::types::ProjectionType::KEYS_ONLY => {
                        return Err(DynoxideError::ValidationException(
                            "One or more parameter values were invalid: \
                             ProjectionType is KEYS_ONLY, but NonKeyAttributes is specified"
                                .to_string(),
                        ));
                    }
                    crate::types::ProjectionType::INCLUDE => {
                        // NonKeyAttributes with INCLUDE is valid, but must not be empty
                        if nka.is_empty() {
                            return Err(DynoxideError::ValidationException(
                                "One or more parameter values were invalid: \
                                 NonKeyAttributes must not be empty"
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Extract the partition key name from a key schema.
pub fn partition_key_name(key_schema: &[KeySchemaElement]) -> Option<&str> {
    key_schema
        .iter()
        .find(|k| k.key_type == KeyType::HASH)
        .map(|k| k.attribute_name.as_str())
}

/// DynamoDB caps document nesting at 32 levels, counting the top-level attribute
/// value as level 1. Values are validated 0-indexed (top-level = depth 0), so the
/// deepest permitted leaf sits at depth 31 and a value reaching depth 32 is rejected.
const MAX_NESTING_DEPTH: usize = 32;

/// Real DynamoDB's verbatim message when document nesting exceeds the limit. Shared
/// by the stored-item and ExpressionAttributeValue checks so both match AWS.
pub(crate) const NESTING_LIMIT_MESSAGE: &str = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit";

/// The short form of the nesting message, without the clause about the item's
/// attributes. UpdateItem sends it, bare, when the item an update would leave
/// behind is nested too deep, and a transaction's Update gives it as the
/// cancellation reason. Captured us-east-1 and eu-west-2, 2026-10-09.
pub(crate) const UPDATED_ITEM_NESTING_MESSAGE: &str =
    "Nesting Levels have exceeded supported limits";

/// Refuse an updated item nested deeper than DynamoDB allows, with the short
/// message an update gets.
pub(crate) fn validate_updated_item_nesting(item: &Item) -> Result<()> {
    if item
        .values()
        .any(|value| validate_nesting_depth(value).is_err())
    {
        return Err(DynoxideError::ValidationException(
            UPDATED_ITEM_NESTING_MESSAGE.to_string(),
        ));
    }
    Ok(())
}

/// Validate all attribute values in an item.
///
/// Rejects:
/// - Empty sets (`{"SS": []}`, `{"NS": []}`, `{"BS": []}`)
/// - Numbers that violate DynamoDB's precision/range constraints
/// - Nesting deeper than 32 levels
///
/// Validation is recursive: invalid values nested inside L (list) or M (map) are also rejected.
///
/// **Note:** Empty strings (`{"S": ""}`) and empty binary values (`{"B": ""}`) are
/// permitted in non-key attributes since DynamoDB's 2020 update. Key attributes
/// are validated separately in `helpers::validate_key_type`.
///
/// **Important:** This must only be called on items being persisted, NOT on
/// `ExpressionAttributeValues` (which may legitimately contain empty strings for comparisons).
pub fn validate_item_attribute_values(item: &Item) -> crate::Result<()> {
    validate_item_attribute_values_classified(item, ValueLayer::Older).map_err(Into::into)
}

/// Classified form of [`validate_item_attribute_values`], read by `layer`'s
/// rules: the set families (empty and duplicate SS/NS/BS) are marked as
/// enveloped so PutItem and UpdateItem can wrap them in the request-validation
/// envelope, and so are the current layer's number errors; the older layer's
/// numbers and the second nesting check stay bare. Every other caller converts
/// back to the plain error unchanged.
pub(crate) fn validate_item_attribute_values_classified(
    item: &Item,
    layer: ValueLayer,
) -> std::result::Result<(), ClassifiedValidationError> {
    for value in item.values() {
        validate_attribute_value(value, 0, layer).map_err(|e| match e {
            // From 63 levels a second check answers first, with the same
            // message outside the envelope.
            ClassifiedValidationError::Enveloped(msg)
                if msg == NESTING_LIMIT_MESSAGE && nesting_depth(value) > SECOND_NESTING_LIMIT =>
            {
                ClassifiedValidationError::bare(msg)
            }
            other => other,
        })?;
    }
    Ok(())
}

/// [`validate_item_attribute_values_classified`] for one value outside an
/// item, such as a legacy `Expected` condition's.
pub(crate) fn validate_value_classified(
    value: &AttributeValue,
    layer: ValueLayer,
) -> std::result::Result<(), ClassifiedValidationError> {
    validate_attribute_value(value, 0, layer)
}

/// The first empty set, or set with a repeated member, anywhere in `value`,
/// lists and maps included, worded by `layer`; `None` when every set is sound.
pub(crate) fn find_set_error(value: &AttributeValue, layer: ValueLayer) -> Option<String> {
    const INVALID: &str = "One or more parameter values were invalid: ";
    match value {
        AttributeValue::SS(set) if set.is_empty() => {
            Some(format!("{INVALID}An string set  may not be empty"))
        }
        AttributeValue::NS(set) if set.is_empty() => {
            Some(format!("{INVALID}An number set  may not be empty"))
        }
        AttributeValue::BS(set) if set.is_empty() => {
            Some(format!("{INVALID}Binary sets should not be empty"))
        }
        AttributeValue::L(list) => list.iter().find_map(|v| find_set_error(v, layer)),
        AttributeValue::M(map) => map.values().find_map(|v| find_set_error(v, layer)),
        _ => duplicate_set_message(value, layer),
    }
}

/// The first set with a repeated member anywhere in `value`, lists and maps
/// included, worded by `layer`; `None` when there is none.
pub(crate) fn find_duplicate_set(value: &AttributeValue, layer: ValueLayer) -> Option<String> {
    match value {
        AttributeValue::L(list) => list.iter().find_map(|v| find_duplicate_set(v, layer)),
        AttributeValue::M(map) => map.values().find_map(|v| find_duplicate_set(v, layer)),
        _ => duplicate_set_message(value, layer),
    }
}

/// The depth past which a single-item request's nesting is refused by a
/// second check, in a different form: a stored item's message loses the
/// envelope, and an ExpressionAttributeValues entry is refused naming its key.
/// Values at 62 levels get the first check's answer and values at 63 the
/// second's. Captured us-east-1 and eu-west-2, 2026-10-09.
pub(crate) const SECOND_NESTING_LIMIT: usize = 62;

/// How many lists and maps deep a value goes: a scalar is 0, a map holding a
/// scalar is 1.
pub(crate) fn nesting_depth(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::L(list) => 1 + list.iter().map(nesting_depth).max().unwrap_or(0),
        AttributeValue::M(map) => 1 + map.values().map(nesting_depth).max().unwrap_or(0),
        _ => 0,
    }
}

/// Check one PartiQL statement parameter as an item's values are checked
/// (unreadable numbers, empty and duplicated sets), leaving its depth to the
/// statement it fills in.
pub(crate) fn validate_parameter_value(value: &AttributeValue) -> crate::Result<()> {
    validate_attribute_value_within(value, 0, None, ValueLayer::Older).map_err(Into::into)
}

fn validate_attribute_value(
    value: &AttributeValue,
    depth: usize,
    layer: ValueLayer,
) -> std::result::Result<(), ClassifiedValidationError> {
    validate_attribute_value_within(value, depth, Some(MAX_NESTING_DEPTH), layer)
}

fn validate_attribute_value_within(
    value: &AttributeValue,
    depth: usize,
    nesting_limit: Option<usize>,
    layer: ValueLayer,
) -> std::result::Result<(), ClassifiedValidationError> {
    let number = |n: &str| {
        layer
            .check_number(n)
            .map_err(|e| layer.classify_number_error(e))
    };
    // PutItem wraps the nesting rejection in the envelope (us-east-1 and
    // eu-west-2, 2026-10-09); BatchWriteItem and a transaction's Put send it
    // bare, which the untagged callers keep.
    if nesting_limit.is_some_and(|limit| depth >= limit) {
        return Err(ClassifiedValidationError::enveloped(NESTING_LIMIT_MESSAGE));
    }
    match value {
        AttributeValue::SS(set) if set.is_empty() => Err(ClassifiedValidationError::enveloped(
            "One or more parameter values were invalid: An string set  may not be empty",
        )),
        AttributeValue::NS(set) if set.is_empty() => Err(ClassifiedValidationError::enveloped(
            "One or more parameter values were invalid: An number set  may not be empty",
        )),
        AttributeValue::BS(set) if set.is_empty() => Err(ClassifiedValidationError::enveloped(
            "One or more parameter values were invalid: Binary sets should not be empty",
        )),
        AttributeValue::SS(_) | AttributeValue::BS(_) => {
            match duplicate_set_message(value, layer) {
                Some(message) => Err(ClassifiedValidationError::enveloped(message)),
                None => Ok(()),
            }
        }
        AttributeValue::NS(set) => {
            for n in set {
                number(n)?;
            }
            match duplicate_set_message(value, layer) {
                Some(message) => Err(ClassifiedValidationError::enveloped(message)),
                None => Ok(()),
            }
        }
        AttributeValue::N(n) => {
            number(n)?;
            Ok(())
        }
        AttributeValue::L(list) => {
            for v in list {
                validate_attribute_value_within(v, depth + 1, nesting_limit, layer)?;
            }
            Ok(())
        }
        AttributeValue::M(map) => {
            for v in map.values() {
                validate_attribute_value_within(v, depth + 1, nesting_limit, layer)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Validate that a single `ExpressionAttributeValue` does not nest deeper than
/// DynamoDB allows.
///
/// Real DynamoDB rejects expression values whose document nesting exceeds 32 levels
/// up front, before the expression is evaluated, raising the same bare nesting
/// `ValidationException` it raises for over-deep stored items (no "ExpressionAttributeValues
/// contains invalid value" wrapper). Only the nesting depth is checked here; empty
/// strings and other shapes that are legal in comparisons are left untouched.
pub fn validate_nesting_depth(value: &AttributeValue) -> Result<()> {
    check_nesting_depth(value, 0)
}

fn check_nesting_depth(value: &AttributeValue, depth: usize) -> Result<()> {
    if depth >= MAX_NESTING_DEPTH {
        return Err(DynoxideError::ValidationException(
            NESTING_LIMIT_MESSAGE.to_string(),
        ));
    }
    match value {
        AttributeValue::L(list) => list
            .iter()
            .try_for_each(|v| check_nesting_depth(v, depth + 1)),
        AttributeValue::M(map) => map
            .values()
            .try_for_each(|v| check_nesting_depth(v, depth + 1)),
        _ => Ok(()),
    }
}

/// Validate Key attribute values before table-level checks.
///
/// This validates the attribute values in a Key map for:
/// - Invalid/empty numbers
/// - Empty sets, duplicate sets
/// - Multiple datatypes
///
/// These errors are returned with "One or more parameter values were invalid: " prefix.
pub fn validate_key_attribute_values(key: &Item) -> Result<()> {
    validate_key_values(key, ValueLayer::Older).map_err(Into::into)
}

/// Check a key's values by `layer`'s rules. A duplicated set member, and on
/// the current layer a number it can't store, are marked enveloped so
/// UpdateItem and DeleteItem can wrap them in the request-validation envelope;
/// the empty-set families stay bare.
pub(crate) fn validate_key_values(
    key: &Item,
    layer: ValueLayer,
) -> std::result::Result<(), ClassifiedValidationError> {
    for value in key.values() {
        validate_key_attr_value(value, layer)?;
    }
    Ok(())
}

fn validate_key_attr_value(
    value: &AttributeValue,
    layer: ValueLayer,
) -> std::result::Result<(), ClassifiedValidationError> {
    match value {
        AttributeValue::SS(set) if set.is_empty() => {
            return Err(ClassifiedValidationError::bare(
                "One or more parameter values were invalid: An string set  may not be empty",
            ));
        }
        AttributeValue::NS(set) if set.is_empty() => {
            return Err(ClassifiedValidationError::bare(
                "One or more parameter values were invalid: An number set  may not be empty",
            ));
        }
        AttributeValue::BS(set) if set.is_empty() => {
            return Err(ClassifiedValidationError::bare(
                "One or more parameter values were invalid: Binary sets should not be empty",
            ));
        }
        AttributeValue::SS(_) | AttributeValue::BS(_) => {
            if let Some(message) = duplicate_set_message(value, layer) {
                return Err(ClassifiedValidationError::enveloped(message));
            }
        }
        AttributeValue::NS(set) => {
            for n in set {
                layer
                    .check_number(n)
                    .map_err(|e| layer.classify_number_error(e))?;
            }
            if let Some(message) = duplicate_set_message(value, layer) {
                return Err(ClassifiedValidationError::enveloped(message));
            }
        }
        AttributeValue::N(n) => {
            layer
                .check_number(n)
                .map_err(|e| layer.classify_number_error(e))?;
        }
        _ => {}
    }
    Ok(())
}

/// Which of DynamoDB's two attribute-value validation layers an operation
/// runs. They word a number they can't read, and a set with a repeated member,
/// differently. Captured in eu-west-2, us-east-1, eu-west-1 and us-west-2,
/// 2026-10-09; where the regions split (the transactions, which eu-west-2
/// already runs on the current layer, and Scan, which 19 of 33 regions read on
/// it by 2026-10-20), the majority's layer is used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValueLayer {
    /// PutItem, UpdateItem and DeleteItem (inside the envelope), and GetItem,
    /// BatchGetItem, Query, Scan and TransactWriteItems (bare). Numbers by the
    /// stored-number rule, an empty one ending `: `; duplicates quoted, in the
    /// order described at [`duplicate_set_message`]; expression values with no
    /// `ExpressionAttributeValues contains invalid value` wrapper.
    Current,
    /// BatchWriteItem, TransactGetItems and the PartiQL operations. Numbers as
    /// Java's `BigDecimal` reads them, an empty one with no colon; duplicates
    /// unquoted, as sent.
    Older,
}

impl ValueLayer {
    /// The layer DynamoDB runs `operation` on, for the seams that read a
    /// request knowing only the operation's name.
    ///
    /// TransactWriteItems reads its items, keys and expression values on the
    /// current layer. On 2026-10-09 three of four regions (us-east-1,
    /// eu-west-1 and us-west-2) still read them on the older one and named a
    /// refused expression value's key; on 11 November 2026 eu-west-2,
    /// us-east-1, eu-west-1 and us-west-2 all answered bare, by the
    /// stored-number rule.
    #[cfg(any(
        feature = "http-server",
        feature = "mcp-server",
        feature = "wasm-sqlite",
        test
    ))]
    pub(crate) fn for_operation(operation: &str) -> Self {
        match operation {
            "BatchWriteItem"
            | "TransactGetItems"
            | "ExecuteStatement"
            | "BatchExecuteStatement"
            | "ExecuteTransaction" => Self::Older,
            _ => Self::Current,
        }
    }

    /// Check a number by this layer's rule.
    pub(crate) fn check_number(self, n: &str) -> Result<()> {
        match self {
            Self::Current => crate::types::validate_current_number(n),
            Self::Older => crate::types::validate_parameter_number(n),
        }
    }

    /// Classify a number this layer refuses: the current layer's go in the
    /// envelope on the single-item writes, the older layer's stay bare.
    pub(crate) fn classify_number_error(self, e: DynoxideError) -> ClassifiedValidationError {
        match (self, e) {
            (Self::Current, DynoxideError::ValidationException(m)) => {
                ClassifiedValidationError::enveloped(m)
            }
            (_, e) => ClassifiedValidationError::Bare(e),
        }
    }
}

/// The message DynamoDB refuses a string, number or binary set with a repeated
/// member in, by `layer`'s wording, or `None` when every member is distinct.
///
/// The current layer quotes the members and lists them in an odd order: they
/// are read into a byte-ordered set until the first repeat, and the message
/// prints that set, the repeated member, then the members not yet read, as
/// sent. `["b", "a", "b", "C"]` reads `["a", "b", "b", "C"]`. Binary members
/// are base64, joined with no space. A number set names no members. The older
/// layer prints every member as sent, unquoted, and words a number set
/// without the prefix or the full stop. Captured in eu-west-2, us-east-1,
/// eu-west-1 and us-west-2, 2026-10-09.
pub(crate) fn duplicate_set_message(value: &AttributeValue, layer: ValueLayer) -> Option<String> {
    const INVALID: &str = "One or more parameter values were invalid: ";
    /// The current layer's order: the sorted members read before the first
    /// repeat, the repeat, then the rest as sent. `None` without a repeat.
    fn current_order<T: Ord + Clone>(members: &[T]) -> Option<Vec<T>> {
        let mut read = std::collections::BTreeSet::new();
        for (i, member) in members.iter().enumerate() {
            if !read.insert(member.clone()) {
                let mut out: Vec<T> = read.into_iter().collect();
                out.push(member.clone());
                out.extend(members[i + 1..].iter().cloned());
                return Some(out);
            }
        }
        None
    }
    fn has_repeat<T: std::hash::Hash + Eq>(members: impl IntoIterator<Item = T>) -> bool {
        let mut seen = std::collections::HashSet::new();
        members.into_iter().any(|m| !seen.insert(m))
    }
    use base64::Engine;
    let b64 = |b: &Vec<u8>| base64::engine::general_purpose::STANDARD.encode(b);
    match (value, layer) {
        (AttributeValue::SS(set), ValueLayer::Current) => current_order(set).map(|ordered| {
            let quoted: Vec<String> = ordered
                .iter()
                .map(|s| serde_json::to_string(s).unwrap_or_default())
                .collect();
            format!(
                "{INVALID}Input collection [{}] contains duplicates.",
                quoted.join(", ")
            )
        }),
        (AttributeValue::SS(set), ValueLayer::Older) => has_repeat(set.iter()).then(|| {
            format!(
                "{INVALID}Input collection [{}] contains duplicates.",
                set.join(", ")
            )
        }),
        (AttributeValue::BS(set), ValueLayer::Current) => current_order(set).map(|ordered| {
            let quoted: Vec<String> = ordered.iter().map(|b| format!("\"{}\"", b64(b))).collect();
            format!(
                "{INVALID}Input collection [{}]of type BS contains duplicates.",
                quoted.join(",")
            )
        }),
        (AttributeValue::BS(set), ValueLayer::Older) => has_repeat(set.iter()).then(|| {
            let shown: Vec<String> = set.iter().map(b64).collect();
            format!(
                "{INVALID}Input collection [{}]of type BS contains duplicates.",
                shown.join(", ")
            )
        }),
        (AttributeValue::NS(set), _) => {
            has_repeat(set.iter().map(|n| crate::types::normalize_dynamo_number(n))).then(|| {
                match layer {
                    ValueLayer::Current => {
                        format!("{INVALID}Input collection contains duplicates.")
                    }
                    ValueLayer::Older => "Input collection contains duplicates".to_string(),
                }
            })
        }
        _ => None,
    }
}

/// Normalize sets within an item by deduplicating and sorting them in-place.
///
/// - SS: deduplicates by string value, then sorts by bytes
/// - NS: deduplicates by numeric value (e.g., "1.0" and "1" are the same),
///   then sorts by value
/// - BS: deduplicates by byte content, then sorts by bytes
///
/// The sorted order is the one DynamoDB stores and returns sets in.
///
/// Recursively normalizes sets inside L (list) and M (map) values.
pub fn normalize_item_sets(item: &mut Item) {
    for value in item.values_mut() {
        normalize_attribute_sets(value);
    }
}

fn normalize_attribute_sets(value: &mut AttributeValue) {
    match value {
        AttributeValue::N(n) => {
            *n = crate::types::normalize_dynamo_number(n);
        }
        AttributeValue::SS(set) => {
            let mut seen = std::collections::HashSet::new();
            set.retain(|s| seen.insert(s.clone()));
            crate::set_order::sort_string_set(set);
        }
        AttributeValue::NS(set) => {
            let mut seen = std::collections::HashSet::new();
            set.retain(|n| seen.insert(normalize_number_for_dedup(n)));
            // Normalize each number in the set
            for n in set.iter_mut() {
                *n = crate::types::normalize_dynamo_number(n);
            }
            crate::set_order::sort_number_set(set);
        }
        AttributeValue::BS(set) => {
            let mut seen = std::collections::HashSet::new();
            set.retain(|b| seen.insert(b.clone()));
            crate::set_order::sort_binary_set(set);
        }
        AttributeValue::L(list) => {
            for v in list.iter_mut() {
                normalize_attribute_sets(v);
            }
        }
        AttributeValue::M(map) => {
            for v in map.values_mut() {
                normalize_attribute_sets(v);
            }
        }
        _ => {}
    }
}

/// Produce a canonical string for a DynamoDB number for deduplication purposes.
/// Strips leading/trailing zeros and normalizes to a canonical form so that
/// "1.0", "1", "1.00", "01" all map to the same string.
fn normalize_number_for_dedup(n: &str) -> String {
    let trimmed = n.trim();
    let negative = trimmed.starts_with('-');
    let abs_str = if negative { &trimmed[1..] } else { trimmed };

    let (digits, exponent) = crate::types::parse_number_parts(abs_str);

    if digits.is_empty() {
        return "0".to_string();
    }

    let mantissa: String = digits.iter().map(|&d| (b'0' + d) as char).collect();
    let sign = if negative { "-" } else { "" };
    format!("{sign}{mantissa}E{exponent}")
}

/// Validate a Local Secondary Index definition.
pub fn validate_lsi(
    lsi: &crate::types::LocalSecondaryIndex,
    table_key_schema: &[KeySchemaElement],
    all_definitions: &[AttributeDefinition],
) -> Result<()> {
    // Validate index name length
    if !(3..=255).contains(&length_in_characters(&lsi.index_name)) {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'localSecondaryIndexes.1.member.indexName' \
             failed to satisfy constraint: Member must have length greater than or equal to 3",
            lsi.index_name
        )));
    }

    // Validate index name character set
    if !lsi
        .index_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'localSecondaryIndexes.1.member.indexName' \
             failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
            lsi.index_name
        )));
    }

    // Validate key schema
    validate_key_schema(&lsi.key_schema)?;

    // Validate projection (DynamoDB checks this before hash key / sort key checks)
    validate_projection(&lsi.projection, &lsi.index_name)?;

    // LSI must have a RANGE key (sort key)
    let lsi_pk = lsi
        .key_schema
        .iter()
        .find(|k| k.key_type == KeyType::HASH)
        .map(|k| k.attribute_name.as_str());
    let lsi_sk = lsi
        .key_schema
        .iter()
        .find(|k| k.key_type == KeyType::RANGE)
        .map(|k| k.attribute_name.as_str());

    let table_pk = partition_key_name(table_key_schema);
    let table_sk = sort_key_name(table_key_schema);

    // LSI partition key MUST match table partition key
    if lsi_pk != table_pk {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: Table KeySchema: The AttributeValue for a key attribute for the table must match the AttributeValue definition".to_string(),
        ));
    }

    // LSI sort key must be different from table sort key
    if lsi_sk.is_some() && lsi_sk == table_sk {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: Index KeySchema: The index KeySchema must not be the same as the table KeySchema".to_string(),
        ));
    }

    // LSI sort key must be in AttributeDefinitions
    validate_key_attributes_in_definitions(&lsi.key_schema, all_definitions)?;

    Ok(())
}

/// Extract the sort key name from a key schema (if present).
pub fn sort_key_name(key_schema: &[KeySchemaElement]) -> Option<&str> {
    key_schema
        .iter()
        .find(|k| k.key_type == KeyType::RANGE)
        .map(|k| k.attribute_name.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty GSI key schema is refused by the shape check itself, in the
    /// words both callers used to give before calling it, rather than relying
    /// on each caller to guard the first element.
    #[test]
    fn an_empty_gsi_key_schema_is_refused_by_the_shape_check() {
        assert_eq!(
            check_gsi_key_schema(&[]),
            Err(
                "1 validation error detected: Value null at 'keySchema' failed to satisfy \
                 constraint: Member must have length less than or equal to 2"
                    .to_string()
            )
        );
    }

    #[test]
    fn java_hash_order_matches_the_captured_lists() {
        // Definitions as requested, in DynamoDB's message order, eu-west-2 and
        // us-east-1, 2026-10-09: a table sized to the count, then a default one.
        assert_eq!(
            java_hash_order(&["PK", "h", "a", "unused"], true),
            ["h", "a", "unused", "PK"]
        );
        assert_eq!(
            java_hash_order(&["PK", "a", "b", "c", "d", "u1"], true),
            ["a", "b", "PK", "c", "d", "u1"]
        );
        assert_eq!(java_hash_order(&["h", "a", "zz"], true), ["h", "zz", "a"]);
        assert_eq!(
            java_hash_order(&["pk", "sk", "gpk", "gsk"], false),
            ["gpk", "sk", "pk", "gsk"]
        );
        assert_eq!(
            java_hash_order(&["orderId", "customerId", "createdAt", "status"], false),
            ["createdAt", "orderId", "customerId", "status"]
        );
    }

    fn hash_key(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_string(),
            key_type: KeyType::HASH,
        }
    }

    fn range_key(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_string(),
            key_type: KeyType::RANGE,
        }
    }

    fn attr_def(name: &str, attr_type: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_string(),
            attribute_type: attr_type,
        }
    }

    #[test]
    fn test_valid_table_name() {
        assert!(validate_table_name("MyTable").is_ok());
        assert!(validate_table_name("my-table.v2").is_ok());
        assert!(validate_table_name("a_b").is_ok());
    }

    #[test]
    fn test_short_table_name_accepted_for_read_write() {
        // ReadWrite context (the default for validate_table_name) only enforces min length 1,
        // matching AWS's per-operation rules. CreateTable's min-length-3 lives behind
        // table_name_constraint_errors with TableNameContext::CreateTable.
        assert!(validate_table_name("ab").is_ok());
        assert!(validate_table_name("a").is_ok());
    }

    #[test]
    fn test_empty_table_name_rejected_for_read_write() {
        let err = validate_table_name("").unwrap_err().to_string();
        assert!(err.contains("Member must have length greater than or equal to 1"));
        assert!(!err.contains("greater than or equal to 3"));
    }

    #[test]
    fn test_invalid_table_name_bad_chars() {
        assert!(validate_table_name("my table").is_err());
        assert!(validate_table_name("my@table").is_err());
    }

    #[test]
    fn test_create_table_context_keeps_min_length_3() {
        let errs = table_name_constraint_errors(Some("ab"), TableNameContext::CreateTable);
        assert!(
            errs.iter()
                .any(|e| e.contains("Member must have length greater than or equal to 3"))
        );
    }

    #[test]
    fn test_valid_key_schema() {
        let schema = vec![hash_key("pk")];
        assert!(validate_key_schema(&schema).is_ok());

        let schema = vec![hash_key("pk"), range_key("sk")];
        assert!(validate_key_schema(&schema).is_ok());
    }

    #[test]
    fn test_invalid_key_schema_empty() {
        assert!(validate_key_schema(&[]).is_err());
    }

    #[test]
    fn test_invalid_key_schema_no_hash() {
        let schema = vec![range_key("sk")];
        assert!(validate_key_schema(&schema).is_err());
    }

    #[test]
    fn test_valid_key_attributes_in_definitions() {
        let schema = vec![hash_key("pk"), range_key("sk")];
        let defs = vec![
            attr_def("pk", ScalarAttributeType::S),
            attr_def("sk", ScalarAttributeType::N),
        ];
        assert!(validate_key_attributes_in_definitions(&schema, &defs).is_ok());
    }

    #[test]
    fn test_missing_key_attribute_in_definitions() {
        let schema = vec![hash_key("pk"), range_key("sk")];
        let defs = vec![attr_def("pk", ScalarAttributeType::S)];
        assert!(validate_key_attributes_in_definitions(&schema, &defs).is_err());
    }

    #[test]
    fn test_partition_key_name() {
        let schema = vec![hash_key("pk"), range_key("sk")];
        assert_eq!(partition_key_name(&schema), Some("pk"));
    }

    #[test]
    fn test_sort_key_name() {
        let schema = vec![hash_key("pk"), range_key("sk")];
        assert_eq!(sort_key_name(&schema), Some("sk"));

        let schema = vec![hash_key("pk")];
        assert_eq!(sort_key_name(&schema), None);
    }

    #[test]
    fn test_envelope_request_validation_wraps_tagged_error() {
        let msg = "Value '' at 'expressionAttributeNames' failed to satisfy constraint: \
                   Map value must satisfy constraint";
        let err = envelope_request_validation(DynoxideError::EnvelopedValidation(msg.to_string()));
        match err {
            DynoxideError::ValidationException(m) => {
                assert_eq!(m, format!("1 validation error detected: {msg}"));
            }
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn test_envelope_request_validation_passes_other_errors_through() {
        let plain = envelope_request_validation(DynoxideError::ValidationException("msg".into()));
        assert!(matches!(
            &plain,
            DynoxideError::ValidationException(m) if m == "msg"
        ));

        let key_empty =
            envelope_request_validation(DynoxideError::KeyEmptyValueValidation("msg".into()));
        assert!(matches!(
            &key_empty,
            DynoxideError::KeyEmptyValueValidation(m) if m == "msg"
        ));

        let not_found =
            envelope_request_validation(DynoxideError::ResourceNotFoundException("msg".into()));
        assert!(matches!(
            &not_found,
            DynoxideError::ResourceNotFoundException(m) if m == "msg"
        ));
    }

    #[test]
    fn test_strip_request_validation_tag_untags_without_envelope() {
        let err = strip_request_validation_tag(DynoxideError::EnvelopedValidation("msg".into()));
        assert!(matches!(
            &err,
            DynoxideError::ValidationException(m) if m == "msg"
        ));
    }

    #[test]
    fn test_strip_request_validation_tag_passes_other_errors_through() {
        let plain = strip_request_validation_tag(DynoxideError::ValidationException("msg".into()));
        assert!(matches!(
            &plain,
            DynoxideError::ValidationException(m) if m == "msg"
        ));

        let not_found =
            strip_request_validation_tag(DynoxideError::ResourceNotFoundException("msg".into()));
        assert!(matches!(
            &not_found,
            DynoxideError::ResourceNotFoundException(m) if m == "msg"
        ));
    }

    #[test]
    fn normalising_an_item_sorts_its_sets_in_place_at_every_depth() {
        // Read the stored Vecs directly: every other set-order test reads back
        // through Serialize, which sorts on its own and would hide a write that
        // stopped sorting.
        let mut item: Item = [
            (
                "top".to_string(),
                AttributeValue::NS(vec!["3".into(), "-1".into(), "2".into()]),
            ),
            (
                "m".to_string(),
                AttributeValue::M(
                    [(
                        "ss".to_string(),
                        AttributeValue::SS(vec!["b".into(), "a".into(), "c".into()]),
                    )]
                    .into(),
                ),
            ),
            (
                "l".to_string(),
                AttributeValue::L(vec![
                    AttributeValue::NS(vec!["10".into(), "2".into(), "1e2".into()]),
                    AttributeValue::BS(vec![vec![2], vec![0xff], vec![1]]),
                ]),
            ),
        ]
        .into();
        normalize_item_sets(&mut item);

        assert_eq!(
            item["top"],
            AttributeValue::NS(vec!["-1".into(), "2".into(), "3".into()])
        );
        let AttributeValue::M(m) = &item["m"] else {
            panic!("m")
        };
        assert_eq!(
            m["ss"],
            AttributeValue::SS(vec!["a".into(), "b".into(), "c".into()])
        );
        let AttributeValue::L(l) = &item["l"] else {
            panic!("l")
        };
        assert_eq!(
            l[0],
            AttributeValue::NS(vec!["2".into(), "10".into(), "100".into()])
        );
        assert_eq!(l[1], AttributeValue::BS(vec![vec![1], vec![2], vec![0xff]]));
    }
}
