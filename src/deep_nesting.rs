//! Requests whose attribute values nest deeper than the JSON parser follows.
//!
//! serde_json refuses a document nested more than 127 levels. An attribute
//! value gets there at about 61 levels, because each map or list level costs
//! two JSON levels on top of the request's own wrappers. DynamoDB reads such a
//! request and answers with its nesting `ValidationException`, so a body that
//! serde refuses on depth alone comes here rather than being answered as
//! malformed.
//!
//! The body is walked once, iteratively, to find every attribute value and how
//! far it nests. Each value past DynamoDB's 32-level limit is swapped for a
//! shallow stand-in, so the operation's own request decoder can run its member
//! checks: DynamoDB reports a bad table name or enum ahead of the nesting
//! error. Then the nesting error is answered for the first deep value in the
//! request, ahead of everything else the operation would check, without
//! running it.
//!
//! The answer's form depends only on where the value sits, because by the
//! time a value is too deep for the parser DynamoDB has moved to its second
//! nesting check on every surface (captured in us-east-1 and eu-west-2 on
//! 2026-10-09): an `ExpressionAttributeValues` entry is named, an
//! `ExclusiveStartKey` takes the starting-key prefix, and every other position
//! (items, keys, `Expected`, `AttributeUpdates`, `ScanFilter`, PartiQL
//! parameters) answers the bare message.
//!
//! The PartiQL operations are the exception. They check their parameters
//! before anything runs (and BatchExecuteStatement fails only the statement
//! whose parameter is too deep), so their bodies are rewritten with a value
//! just past the limit and run as usual, which keeps every check DynamoDB
//! makes first, such as `Limit`, in its place.

mod parameter_order;

pub(crate) use parameter_order::in_walk_order;

use crate::errors::DynoxideError;
use crate::json_text::string_end;
use crate::validation::{NESTING_LIMIT_MESSAGE, RETURN_VALUES_NOT_HONOURED};

/// How far a value may nest inside its attribute: DynamoDB accepts a leaf
/// wrapped in 31 maps or lists and refuses one wrapped in 32.
const MAX_NESTING: u32 = 32;

/// Past this many levels DynamoDB's own parser gives up and answers a bare
/// `SerializationException`. Where it gives up varies by run and region,
/// between about 2,000 and 5,000 levels (captured 2026-10-09); this sits at the
/// bottom of that range.
const UNREADABLE_NESTING: u32 = 2000;

/// JSON nesting the walk follows before calling the body unreadable: enough
/// for a value at [`UNREADABLE_NESTING`] inside any request's wrappers. It
/// bounds the walk's own memory on a hostile body.
const MAX_JSON_DEPTH: usize = 2 * UNREADABLE_NESTING as usize + 32;

/// What to do with a body serde refused on depth.
#[derive(Debug)]
pub(crate) enum DeepBody {
    /// Answer a bare `SerializationException`, as DynamoDB does for a body it
    /// cannot read.
    Unreadable,
    /// Answer this error without running the operation.
    Refuse(DynoxideError),
    /// Run the operation on this body instead.
    Run(String),
}

/// Whether serde gave up on a body because of its nesting rather than its
/// syntax.
pub(crate) fn is_depth_failure(err: &serde_json::Error) -> bool {
    err.to_string().starts_with("recursion limit exceeded")
}

/// Decide what a body that serde refused on depth should get.
pub(crate) fn resolve(operation: &str, body: &str) -> DeepBody {
    // A body that is an array is never an object form: it is refused, or it
    // is a batch body DynamoDB reads by position. DynamoDB answers a form read
    // by position with no message from the depth at which its own parser
    // stops following the body, where the object form gets the nesting error
    // at every depth. That depth coincides with this parser's limit: a list
    // nested 61 levels in a BatchGetItem body given by position, or 62 in a
    // `Condition` given by position, is the first answered with no message,
    // and the BatchGetItem object form gets the nesting error from 45 levels
    // to 135 (captured eu-west-2 and us-east-1, 12 November 2026). A form
    // read by position inside the body holds no value the scan below finds,
    // so it ends up unreadable too.
    if body
        .trim_start_matches([' ', '\t', '\n', '\r'])
        .starts_with('[')
    {
        return DeepBody::Unreadable;
    }
    let Some(values) = scan(body) else {
        return DeepBody::Unreadable;
    };
    let run_as_usual = matches!(
        operation,
        "ExecuteStatement" | "ExecuteTransaction" | "BatchExecuteStatement"
    );
    // A PartiQL parameter is refused from 33 levels, one more than any other
    // value, so one at 32 is left to run.
    let limit = if run_as_usual {
        MAX_NESTING + 1
    } else {
        MAX_NESTING
    };
    let deep: Vec<&Value> = values.iter().filter(|v| v.nesting >= limit).collect();
    // The depth came from somewhere that isn't an attribute value, or a value
    // went past where DynamoDB itself stops reading.
    let Some(first) = deep.first() else {
        return DeepBody::Unreadable;
    };
    if deep.iter().any(|v| v.nesting > UNREADABLE_NESTING) {
        return DeepBody::Unreadable;
    }

    let stand_in = if run_as_usual {
        just_past_the_limit()
    } else {
        r#"{"NULL":true}"#.to_string()
    };
    // A value inside one already replaced goes with it.
    let rewritten = crate::json_text::splice(
        body,
        deep.iter()
            .map(|value| ((value.start, value.end), stand_in.as_str())),
    );
    if serde_json::from_str::<serde_json::Value>(&rewritten).is_err() {
        return DeepBody::Unreadable;
    }

    if run_as_usual {
        return DeepBody::Run(rewritten);
    }
    if let Err(e) = check_members(operation, &rewritten) {
        let e = older_return_values_wording(operation, &rewritten, e);
        let e = older_member_errors(operation, &rewritten, e);
        return DeepBody::Refuse(crate::validation::resolve_request_validation_tag(
            operation, e,
        ));
    }
    DeepBody::Refuse(DynoxideError::ValidationException(refusal(first, body)))
}

/// A body too deep to decode is checked by DynamoDB's older validation layer,
/// which names the value and the member when it refuses a `ReturnValues`,
/// where a shallow request now gets the generic form. Captured on PutItem and
/// UpdateItem with a value nested 100 levels, eu-west-2 and us-east-1,
/// 2026-10-09.
fn older_return_values_wording(operation: &str, body: &str, err: DynoxideError) -> DynoxideError {
    // DeleteItem refuses ALL_NEW, UPDATED_NEW and UPDATED_OLD as a request
    // member, in the envelope, where the older layer gives the bare sentence
    // ahead of the nesting error (captured eu-west-2 and us-east-1,
    // 2026-10-09, for ALL_NEW and UPDATED_NEW).
    if operation == "DeleteItem" {
        let enveloped = crate::validation::envelope_message(RETURN_VALUES_NOT_HONOURED);
        match &err {
            DynoxideError::ValidationException(message)
            | DynoxideError::EnvelopedValidation(message)
                if message == &enveloped || message == RETURN_VALUES_NOT_HONOURED =>
            {
                return DynoxideError::ValidationException(RETURN_VALUES_NOT_HONOURED.to_string());
            }
            _ => {}
        }
    }
    if !matches!(operation, "PutItem" | "UpdateItem" | "DeleteItem") {
        return err;
    }
    let generic = crate::validation::RETURN_VALUES_REJECTION;
    let reword = |message: String| {
        if !message.contains(generic) {
            return message;
        }
        let value = serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|request| request.get("ReturnValues")?.as_str().map(str::to_string));
        match value {
            Some(value) => message.replace(
                generic,
                &crate::validation::return_values_value_rejection(&value),
            ),
            None => message,
        }
    };
    match err {
        DynoxideError::ValidationException(message) => {
            DynoxideError::ValidationException(reword(message))
        }
        DynoxideError::EnvelopedValidation(message) => {
            DynoxideError::EnvelopedValidation(reword(message))
        }
        other => other,
    }
}

/// The older layer checks a deep write's members in an order of its own and
/// reports each one that fails, where the decoder stops at the first and
/// skips `ConditionalOperator`. Captured eu-west-2 and us-east-1, 2026-11-03
/// and 2026-11-04, with an `Expected` value nested 100 levels and every member
/// wrong, they come in the order of [`OLDER_MEMBERS`]. A DeleteItem
/// `ReturnValues` of `ALL_NEW` beside `XOR` gets the `XOR` error alone. A
/// table name that is missing or empty is a member error, and its length and
/// pattern are checked only once the members pass.
///
/// So when every error the decoder reported is one of those, the list is
/// rebuilt from the request in that order. Any other error in it leaves it as
/// it is.
fn older_member_errors(operation: &str, body: &str, err: DynoxideError) -> DynoxideError {
    let Some((_, order)) = OLDER_MEMBERS.iter().find(|(op, _)| *op == operation) else {
        return err;
    };
    let DynoxideError::ValidationException(message) = &err else {
        return err;
    };
    // A bare error's value can hold " detected: " too, so only a count
    // makes a header.
    let reported = match message.split_once(" detected: ") {
        Some((header, list))
            if header
                .strip_suffix(" validation errors")
                .or_else(|| header.strip_suffix(" validation error"))
                .is_some_and(|count| count.parse::<usize>().is_ok()) =>
        {
            list
        }
        _ => message.as_str(),
    };
    let Ok(request) = serde_json::from_str::<serde_json::Value>(body) else {
        return err;
    };
    let rebuilt: Vec<String> = order
        .iter()
        .filter_map(|member| member.rejection(request.get(member.name)))
        .collect();
    let table_name = request.get("TableName").and_then(serde_json::Value::as_str);
    // The table name's errors as the decoder words them, which the older
    // layer's take the place of.
    let decoded = match (operation, table_name) {
        (_, None) => Vec::new(),
        ("UpdateItem", name) => crate::validation::update_item_table_name_errors(name),
        (_, name) => crate::validation::item_table_name_errors(name),
    };
    // Matched whole rather than split on "; ", which a value can hold.
    let mut known: Vec<&str> = rebuilt.iter().chain(&decoded).map(String::as_str).collect();
    known.push(RETURN_VALUES_NOT_HONOURED);
    if !made_of(reported, &known) {
        return err;
    }
    // A name's length and pattern are checked once the members pass.
    let rebuilt = match table_name {
        Some(name) if rebuilt.is_empty() => crate::validation::older_table_name_errors(name),
        _ => rebuilt,
    };
    match crate::validation::format_validation_errors(&rebuilt) {
        Some(message) => DynoxideError::ValidationException(message),
        None => err,
    }
}

/// Whether `list` is some of `errors`, each at most once, joined by "; ".
fn made_of(list: &str, errors: &[&str]) -> bool {
    errors.iter().enumerate().any(|(i, error)| {
        list.strip_prefix(error).is_some_and(|rest| {
            let others: Vec<&str> = [&errors[..i], &errors[i + 1..]].concat();
            rest.is_empty()
                || rest
                    .strip_prefix("; ")
                    .is_some_and(|rest| made_of(rest, &others))
        })
    })
}

/// A request member the older layer checks, and how it refuses one.
struct OlderMember {
    name: &'static str,
    /// What the message calls it.
    wire: &'static str,
    check: OlderCheck,
}

enum OlderCheck {
    ReturnValues,
    ConditionalOperator,
    ReturnValuesOnConditionCheckFailure,
    ReturnConsumedCapacity,
    ReturnItemCollectionMetrics,
    Required,
    TableName,
}

impl OlderMember {
    const fn new(name: &'static str, wire: &'static str, check: OlderCheck) -> Self {
        Self { name, wire, check }
    }

    fn rejection(&self, value: Option<&serde_json::Value>) -> Option<String> {
        use crate::validation::*;
        let text = value.and_then(|v| v.as_str());
        match self.check {
            // `ALL_NEW` and the other values DeleteItem refuses are in the
            // enum, so the older layer doesn't reject them here.
            OlderCheck::ReturnValues => text
                .filter(|v| !RETURN_VALUES.contains(v))
                .map(return_values_value_rejection),
            OlderCheck::ConditionalOperator => conditional_operator_rejection(text),
            OlderCheck::ReturnValuesOnConditionCheckFailure => {
                return_values_on_condition_check_failure_rejection(text, self.wire)
            }
            OlderCheck::ReturnConsumedCapacity => return_consumed_capacity_rejection(text),
            OlderCheck::ReturnItemCollectionMetrics => {
                return_item_collection_metrics_rejection(text)
            }
            OlderCheck::TableName if text == Some("") => Some(older_empty_table_name_rejection()),
            // A null member is read as a missing one.
            OlderCheck::Required | OlderCheck::TableName => {
                value.is_none_or(serde_json::Value::is_null).then(|| {
                    format!(
                        "Value null at '{}' failed to satisfy constraint: Member must not be null",
                        self.wire
                    )
                })
            }
        }
    }
}

/// The members the older layer checks on each deep write, in the order it
/// reports them.
const OLDER_MEMBERS: [(&str, [OlderMember; 7]); 3] = {
    use OlderCheck::*;
    const RV: OlderMember = OlderMember::new("ReturnValues", "returnValues", ReturnValues);
    const CO: OlderMember = OlderMember::new(
        "ConditionalOperator",
        "conditionalOperator",
        ConditionalOperator,
    );
    const RVOCCF: OlderMember = OlderMember::new(
        "ReturnValuesOnConditionCheckFailure",
        "returnValuesOnConditionCheckFailure",
        ReturnValuesOnConditionCheckFailure,
    );
    const RCC: OlderMember = OlderMember::new(
        "ReturnConsumedCapacity",
        "returnConsumedCapacity",
        ReturnConsumedCapacity,
    );
    const RICM: OlderMember = OlderMember::new(
        "ReturnItemCollectionMetrics",
        "returnItemCollectionMetrics",
        ReturnItemCollectionMetrics,
    );
    const TABLE: OlderMember = OlderMember::new("TableName", "tableName", TableName);
    [
        (
            "PutItem",
            [
                RV,
                CO,
                OlderMember::new("Item", "item", Required),
                RVOCCF,
                RCC,
                RICM,
                TABLE,
            ],
        ),
        (
            "UpdateItem",
            [
                RCC,
                TABLE,
                RV,
                CO,
                RVOCCF,
                RICM,
                OlderMember::new("Key", "key", Required),
            ],
        ),
        (
            "DeleteItem",
            [
                RV,
                CO,
                RVOCCF,
                RCC,
                RICM,
                OlderMember::new("Key", "key", Required),
                TABLE,
            ],
        ),
    ]
};

/// A value wrapped in one more map than DynamoDB accepts in a PartiQL
/// parameter, which the operation refuses with the same message a far deeper
/// one gets.
fn just_past_the_limit() -> String {
    let wraps = MAX_NESTING as usize + 1;
    format!(
        "{}{{\"S\":\"leaf\"}}{}",
        "{\"M\":{\"n\":".repeat(wraps),
        "}}".repeat(wraps)
    )
}

/// The nesting error for a value, by where it sits in the request.
fn refusal(value: &Value, body: &str) -> String {
    match value.origin {
        Origin::ExpressionValue => {
            let raw = &body[value.key.0..value.key.1];
            let key = serde_json::from_str::<String>(&format!("\"{raw}\""))
                .unwrap_or_else(|_| raw.to_string());
            format!(
                "ExpressionAttributeValues contains invalid value: {NESTING_LIMIT_MESSAGE} for key {key}"
            )
        }
        Origin::StartKey => {
            format!("The provided starting key is invalid: {NESTING_LIMIT_MESSAGE}")
        }
        Origin::Plain => NESTING_LIMIT_MESSAGE.to_string(),
    }
}

/// Run the operation's request decoder, which is where its member checks
/// (table name, enums, limits) live, and discard the result. BatchWriteItem
/// and TransactWriteItems keep theirs in a request check after decoding, so
/// that runs too.
fn check_members(operation: &str, body: &str) -> crate::Result<()> {
    use crate::actions::*;
    use crate::serde_errors::deserialize;

    let checked = crate::serialization_checks::pre_check_serialization_types(operation, body)?;
    let body = checked.as_ref();
    match operation {
        "PutItem" => deserialize::<put_item::PutItemRequest>(body).and_then(|request| {
            check_older_conditional_operator(request.conditional_operator.as_deref())
        }),
        "GetItem" => deserialize::<get_item::GetItemRequest>(body).map(drop),
        "DeleteItem" => deserialize::<delete_item::DeleteItemRequest>(body).and_then(|request| {
            check_older_conditional_operator(request.conditional_operator.as_deref())
        }),
        "UpdateItem" => deserialize::<update_item::UpdateItemRequest>(body).and_then(|request| {
            check_older_conditional_operator(request.conditional_operator.as_deref())
        }),
        "Query" => deserialize::<query::QueryRequest>(body).map(drop),
        "Scan" => deserialize::<scan::ScanRequest>(body).map(drop),
        "BatchGetItem" => deserialize::<batch_get_item::BatchGetItemRequest>(body).map(drop),
        "BatchWriteItem" => deserialize::<batch_write_item::BatchWriteItemRequest>(body)
            .and_then(|request| batch_write_item::check_request(&request)),
        "TransactGetItems" => {
            deserialize::<transact_get_items::TransactGetItemsRequest>(body).map(drop)
        }
        "TransactWriteItems" => {
            deserialize::<transact_write_items::TransactWriteItemsRequest>(body)
                .and_then(|request| transact_write_items::check_request(&request))
        }
        _ => Ok(()),
    }
}

fn check_older_conditional_operator(operator: Option<&str>) -> crate::Result<()> {
    match crate::validation::conditional_operator_rejection(operator) {
        Some(message) => Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(&message),
        )),
        None => Ok(()),
    }
}

/// Where an attribute value sits, which decides the form of its nesting error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Origin {
    Plain,
    ExpressionValue,
    StartKey,
}

/// One attribute value at the top of its attribute.
#[derive(Debug)]
struct Value {
    /// Byte range of the value's object in the body.
    start: usize,
    end: usize,
    origin: Origin,
    /// Byte range of the member name it sits under, without quotes, when it is
    /// an `ExpressionAttributeValues` entry.
    key: (usize, usize),
    /// How many maps or lists deep it goes: a bare scalar is 0.
    nesting: u32,
}

/// What the members or elements of a JSON container are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    /// Request structure with no attribute values directly inside.
    Other,
    /// `RequestItems`: its members are per-table entries, named by the user.
    Tables,
    /// `Item`, `Key`, `ExclusiveStartKey`, `ExpressionAttributeValues`: its
    /// members are attribute values.
    Values(Origin),
    /// `Keys`: its elements are key maps.
    KeyList,
    /// `AttributeValueList`, `Parameters`: its elements are attribute values.
    ValueList,
    /// `Expected`, `AttributeUpdates` and the legacy filters: its members are
    /// condition objects.
    Conditions,
    /// One condition: its `Value` is an attribute value and its
    /// `AttributeValueList` a value list.
    Condition,
    /// An attribute value: its `M` holds members and its `L` elements.
    Attribute,
    /// The map under `M`: its members are attribute values one level down.
    MapMembers,
    /// The list under `L`: its elements are attribute values one level down.
    ListElements,
    /// Inside an attribute value but not under its `M` or `L`, or an `M` or
    /// `L` of the wrong shape: nothing in it is request structure or another
    /// attribute value, however its members are named.
    Opaque,
}

struct Frame {
    is_object: bool,
    role: Role,
    /// The member name being read, without quotes, for an object.
    key: (usize, usize),
    expect_key: bool,
    /// For the attribute-value roles: which value, and how deep this one is.
    value: usize,
    nesting: u32,
}

/// The role of a value that starts inside a container with role `parent`
/// (and whether it is an object) under member name `key`.
fn child_role(parent: Option<(Role, bool)>, key: &[u8]) -> Role {
    let Some((parent, parent_is_object)) = parent else {
        return Role::Other;
    };
    match parent {
        Role::Other if parent_is_object => match key {
            b"Item" | b"Key" => Role::Values(Origin::Plain),
            b"ExclusiveStartKey" => Role::Values(Origin::StartKey),
            b"ExpressionAttributeValues" => Role::Values(Origin::ExpressionValue),
            b"RequestItems" => Role::Tables,
            b"Keys" => Role::KeyList,
            b"Parameters" => Role::ValueList,
            b"Expected" | b"AttributeUpdates" | b"ScanFilter" | b"QueryFilter"
            | b"KeyConditions" => Role::Conditions,
            _ => Role::Other,
        },
        Role::Other | Role::Tables => Role::Other,
        Role::Values(_) | Role::ValueList | Role::MapMembers | Role::ListElements => {
            Role::Attribute
        }
        Role::KeyList => Role::Values(Origin::Plain),
        Role::Conditions => Role::Condition,
        Role::Condition => match key {
            b"Value" => Role::Attribute,
            b"AttributeValueList" => Role::ValueList,
            _ => Role::Other,
        },
        Role::Attribute => match key {
            b"M" => Role::MapMembers,
            b"L" => Role::ListElements,
            _ => Role::Opaque,
        },
        Role::Opaque => Role::Opaque,
    }
}

/// Walk the body once, without recursion, and list its attribute values.
/// `None` when the body is malformed or nests past [`MAX_JSON_DEPTH`].
fn scan(body: &str) -> Option<Vec<Value>> {
    let bytes = body.as_bytes();
    let mut stack: Vec<Frame> = Vec::new();
    let mut values: Vec<Value> = Vec::new();
    let mut seen_top = false;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b' ' | b'\t' | b'\n' | b'\r' | b':' => i += 1,
            b',' => {
                if let Some(top) = stack.last_mut() {
                    top.expect_key = top.is_object;
                }
                i += 1;
            }
            b'"' => {
                let end = string_end(bytes, i)?;
                match stack.last_mut() {
                    Some(top) if top.is_object && top.expect_key => {
                        top.key = (i + 1, end - 1);
                        top.expect_key = false;
                    }
                    Some(_) => {}
                    None => {
                        if seen_top {
                            return None;
                        }
                        seen_top = true;
                    }
                }
                i = end;
            }
            b'{' | b'[' => {
                if stack.len() >= MAX_JSON_DEPTH {
                    return None;
                }
                if stack.is_empty() {
                    if seen_top {
                        return None;
                    }
                    seen_top = true;
                }
                let parent = stack.last();
                let key = parent.map_or(&b""[..], |p| &bytes[p.key.0..p.key.1]);
                let mut role = child_role(parent.map(|p| (p.role, p.is_object)), key);
                let is_object = b == b'{';
                let (mut value, mut nesting) = (0, 0);
                match role {
                    Role::Attribute if is_object => match parent.map(|p| p.role) {
                        Some(Role::MapMembers | Role::ListElements) => {
                            let p = parent.expect("checked above");
                            value = p.value;
                            nesting = p.nesting + 1;
                            if nesting > values[value].nesting {
                                values[value].nesting = nesting;
                            }
                        }
                        parent_role => {
                            let origin = match parent_role {
                                Some(Role::Values(origin)) => origin,
                                _ => Origin::Plain,
                            };
                            let key = parent.map_or((0, 0), |p| p.key);
                            values.push(Value {
                                start: i,
                                end: 0,
                                origin,
                                key,
                                nesting: 0,
                            });
                            value = values.len() - 1;
                        }
                    },
                    Role::MapMembers if is_object => {
                        let p = parent.expect("an attribute value holds its map");
                        (value, nesting) = (p.value, p.nesting);
                    }
                    Role::ListElements if !is_object => {
                        let p = parent.expect("an attribute value holds its list");
                        (value, nesting) = (p.value, p.nesting);
                    }
                    Role::Attribute | Role::MapMembers | Role::ListElements => role = Role::Opaque,
                    _ => {}
                }
                stack.push(Frame {
                    is_object,
                    role,
                    key: (0, 0),
                    expect_key: is_object,
                    value,
                    nesting,
                });
                i += 1;
            }
            b'}' | b']' => {
                let frame = stack.pop()?;
                if frame.is_object != (b == b'}') {
                    return None;
                }
                let starts_value = frame.role == Role::Attribute
                    && !matches!(
                        stack.last().map(|p| p.role),
                        Some(Role::MapMembers | Role::ListElements)
                    );
                if starts_value {
                    values[frame.value].end = i + 1;
                }
                i += 1;
            }
            _ => {
                // A number, true, false or null.
                if stack.is_empty() {
                    if seen_top {
                        return None;
                    }
                    seen_top = true;
                }
                while i < bytes.len()
                    && !matches!(bytes[i], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
                {
                    i += 1;
                }
            }
        }
    }
    if !stack.is_empty() || !seen_top || values.iter().any(|v| v.end == 0) {
        return None;
    }
    Some(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deep_map(depth: usize) -> String {
        format!(
            "{}{{\"S\":\"leaf\"}}{}",
            "{\"M\":{\"n\":".repeat(depth),
            "}}".repeat(depth)
        )
    }

    fn deep_list(depth: usize) -> String {
        format!(
            "{}{{\"S\":\"leaf\"}}{}",
            "{\"L\":[".repeat(depth),
            "]}".repeat(depth)
        )
    }

    #[test]
    fn serde_gives_up_on_depth_where_this_module_takes_over() {
        // A PutItem value at 62 levels still parses and at 63 does not, which is
        // where DynamoDB moves to its second nesting check. Pinned so a change in
        // serde_json's limit shows up here rather than as a changed answer.
        let at = |n| {
            format!(
                r#"{{"TableName":"T","Item":{{"pk":{{"S":"p"}},"data":{}}}}}"#,
                deep_map(n)
            )
        };
        assert!(serde_json::from_str::<serde_json::Value>(&at(62)).is_ok());
        let err = serde_json::from_str::<serde_json::Value>(&at(63)).unwrap_err();
        assert!(is_depth_failure(&err), "{err}");
        let syntax = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        assert!(!is_depth_failure(&syntax), "{syntax}");
    }

    #[test]
    fn the_walk_finds_each_value_and_how_deep_it_goes() {
        let body = format!(
            r#"{{"TableName":"T","Item":{{"pk":{{"S":"p"}},"m":{},"l":{}}},"ExpressionAttributeValues":{{":v":{{"SS":["a","b"]}}}}}}"#,
            deep_map(3),
            deep_list(5)
        );
        let values = scan(&body).unwrap();
        let summary: Vec<(Origin, u32)> = values.iter().map(|v| (v.origin, v.nesting)).collect();
        assert_eq!(
            summary,
            [
                (Origin::Plain, 0),
                (Origin::Plain, 3),
                (Origin::Plain, 5),
                (Origin::ExpressionValue, 0)
            ]
        );
        assert_eq!(&body[values[1].start..values[1].end], deep_map(3));
        assert_eq!(&body[values[3].key.0..values[3].key.1], ":v");
    }

    #[test]
    fn strings_that_look_like_structure_are_skipped() {
        let body = r#"{"TableName":"T","Item":{"pk":{"S":"a}]\"{["},"Item":{"S":"x"}}}"#;
        let values = scan(body).unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.iter().all(|v| v.nesting == 0));
    }

    #[test]
    fn a_table_called_item_is_not_read_as_an_item() {
        let body = r#"{"RequestItems":{"Item":{"Keys":[{"pk":{"S":"a"}}]}}}"#;
        let values = scan(body).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(&body[values[0].start..values[0].end], r#"{"S":"a"}"#);
    }

    #[test]
    fn a_malformed_or_bottomless_body_is_unreadable() {
        assert!(scan(r#"{"Item":{"pk":{"S":"a"}}"#).is_none());
        assert!(scan(r#"{"Item":{"pk":{"S":"a"]}}"#).is_none());
        assert!(scan(r#"{} {}"#).is_none());
        assert!(scan(&"[".repeat(MAX_JSON_DEPTH + 1)).is_none());
    }

    /// A deep value whose innermost attribute hides request-shaped members:
    /// `{"S":{"Item":{"y":<deep>}}}`, or an `M` holding a list. The members
    /// aren't attribute values, so nothing inside them starts a second value.
    fn hostile(outer: &str) -> String {
        let inner = format!(r#"{{"Item":{{"y":{}}}}}"#, deep_map(40));
        let leaf = outer.replace("LEAF", &format!(r#"{{"S":{inner}}}"#));
        format!(r#"{{"TableName":"Tbl","Item":{{"pk":{{"S":"p"}},"data":{leaf}}}}}"#)
    }

    #[test]
    fn members_of_a_value_other_than_m_and_l_hold_no_values() {
        let in_maps = hostile(&format!(
            "{}LEAF{}",
            "{\"M\":{\"n\":".repeat(70),
            "}}".repeat(70)
        ));
        let in_lists = hostile(&format!("{}LEAF{}", "{\"L\":[".repeat(70), "]}".repeat(70)));
        let wrong_shape_value = format!(
            "{}{{\"M\":[{{\"Item\":{{\"y\":{}}}}}]}}{}",
            "{\"M\":{\"n\":".repeat(70),
            deep_map(40),
            "}}".repeat(70)
        );
        let wrong_shape = format!(
            r#"{{"TableName":"Tbl","Item":{{"pk":{{"S":"p"}},"data":{wrong_shape_value}}}}}"#
        );
        for body in [in_maps, in_lists, wrong_shape] {
            let values = scan(&body).unwrap();
            for pair in values.windows(2) {
                assert!(pair[0].end <= pair[1].start, "values overlap");
            }
            match resolve("PutItem", &body) {
                DeepBody::Refuse(e) => assert_eq!(e.to_string(), NESTING_LIMIT_MESSAGE),
                other => panic!("expected the nesting refusal, got {other:?}"),
            }
        }
    }

    #[test]
    fn depth_outside_any_attribute_value_is_unreadable() {
        let body = format!(
            r#"{{"TableName":"T","Junk":{}0{}}}"#,
            "[".repeat(200),
            "]".repeat(200)
        );
        assert!(matches!(resolve("PutItem", &body), DeepBody::Unreadable));
    }

    #[test]
    fn a_deep_value_alongside_deep_junk_is_still_unreadable() {
        let body = format!(
            r#"{{"TableName":"Tbl","Item":{{"pk":{{"S":"p"}},"data":{}}},"Junk":{}0{}}}"#,
            deep_map(70),
            "[".repeat(200),
            "]".repeat(200)
        );
        assert!(matches!(resolve("PutItem", &body), DeepBody::Unreadable));
    }

    #[test]
    fn the_partiql_stand_in_is_just_past_the_limit_and_parses() {
        let stand_in = just_past_the_limit();
        let values = scan(&format!(r#"{{"Parameters":[{stand_in}]}}"#)).unwrap();
        assert_eq!(values[0].nesting, MAX_NESTING + 1);
    }

    #[test]
    fn an_error_the_older_layer_does_not_give_leaves_the_list_alone() {
        let body = r#"{"TableName":"Deep","Key":{"pk":{"S":"a"}},"ConditionalOperator":"XOR"}"#;
        let rejected = |message: &str| match older_member_errors(
            "DeleteItem",
            body,
            DynoxideError::ValidationException(message.to_string()),
        ) {
            DynoxideError::ValidationException(message) => message,
            other => panic!("not a validation error: {other:?}"),
        };
        let xor = crate::validation::conditional_operator_rejection(Some("XOR")).unwrap();
        let other =
            "Value 'x' at 'somethingElse' failed to satisfy constraint: Member must not be odd";
        let mixed = format!("2 validation errors detected: {other}; {xor}");
        assert_eq!(rejected(&mixed), mixed);
        // And one it does give is rebuilt in the older layer's form.
        assert_eq!(
            rejected(&xor),
            format!("1 validation error detected: {xor}")
        );
    }

    /// A request too deep for the parser that also gives a write request by
    /// position is checked as its object form: a bad member is reported ahead
    /// of the nesting error in the same words for both. (A deep value inside
    /// a form given by position is never found, so that body is unreadable, as
    /// DynamoDB answers it.)
    #[test]
    fn a_deep_request_with_a_form_given_by_position_is_checked_as_its_object_form() {
        let deep = format!(
            r#"{{"PutRequest":{{"Item":{{"pk":{{"S":"a"}},"v":{}}}}}}}"#,
            deep_map(70)
        );
        let request = |shallow: &str| {
            format!(
                r#"{{"RequestItems":{{"tab":[{shallow},{deep}]}},"ReturnConsumedCapacity":"BOGUS"}}"#
            )
        };
        let by_position = request(r#"{"PutRequest":[{"pk":{"S":"b"}}]}"#);
        let as_object = request(r#"{"PutRequest":{"Item":{"pk":{"S":"b"}}}}"#);
        let refused = |body: &str| {
            assert!(serde_json::from_str::<serde_json::Value>(body).is_err());
            match resolve("BatchWriteItem", body) {
                DeepBody::Refuse(e) => e.to_string(),
                other => panic!("{other:?}"),
            }
        };
        assert!(
            refused(&as_object).contains("BOGUS"),
            "{}",
            refused(&as_object)
        );
        assert_eq!(refused(&by_position), refused(&as_object));
        let inside = format!(
            r#"{{"RequestItems":{{"tab":[{{"PutRequest":[{{"pk":{{"S":"a"}},"v":{}}}]}}]}}}}"#,
            deep_map(70)
        );
        assert!(matches!(
            resolve("BatchWriteItem", &inside),
            DeepBody::Unreadable
        ));
    }
}
