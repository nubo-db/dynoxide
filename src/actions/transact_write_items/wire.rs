//! The request as it arrives on the wire.
//!
//! A required member may be left out here, which the public types cannot
//! show, and a value refused in an item is held back rather than failing the
//! read, so that the member checks DynamoDB runs first can be answered first.

use super::{
    TransactConditionCheck, TransactDelete, TransactPut, TransactUpdate, TransactWriteItem,
    TransactWriteItemsRequest,
};
use crate::serde_errors::{SERIALIZATION_MARKER, VALIDATION_MARKER};
use crate::types::{
    HeldValues, deserialize_held_attributes, deserialize_held_older_expression_values,
};
use serde::Deserialize;
use serde::de::{self, Deserializer};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

#[derive(Deserialize)]
pub(super) struct Request {
    #[serde(rename = "TransactItems", default)]
    transact_items: Option<Vec<Option<Item>>>,
    #[serde(rename = "ClientRequestToken", default)]
    client_request_token: Option<String>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    return_item_collection_metrics: Option<String>,
}

#[derive(Deserialize)]
struct Item {
    #[serde(rename = "Put", default, deserialize_with = "action")]
    put: Option<Put>,
    #[serde(rename = "Update", default, deserialize_with = "action")]
    update: Option<Update>,
    #[serde(rename = "Delete", default, deserialize_with = "action")]
    delete: Option<Delete>,
    #[serde(rename = "ConditionCheck", default, deserialize_with = "action")]
    condition_check: Option<ConditionCheck>,
}

#[derive(Deserialize)]
struct Put {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(
        rename = "Item",
        default,
        deserialize_with = "deserialize_held_attributes"
    )]
    item: HeldValues,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "deserialize_held_older_expression_values"
    )]
    expression_attribute_values: HeldValues,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
}

#[derive(Deserialize)]
struct Update {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(
        rename = "Key",
        default,
        deserialize_with = "deserialize_held_attributes"
    )]
    key: HeldValues,
    #[serde(rename = "UpdateExpression", default)]
    update_expression: Option<String>,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "deserialize_held_older_expression_values"
    )]
    expression_attribute_values: HeldValues,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
}

#[derive(Deserialize)]
struct Delete {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(
        rename = "Key",
        default,
        deserialize_with = "deserialize_held_attributes"
    )]
    key: HeldValues,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "deserialize_held_older_expression_values"
    )]
    expression_attribute_values: HeldValues,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
}

#[derive(Deserialize)]
struct ConditionCheck {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(
        rename = "Key",
        default,
        deserialize_with = "deserialize_held_attributes"
    )]
    key: HeldValues,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "deserialize_held_older_expression_values"
    )]
    expression_attribute_values: HeldValues,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
}

/// The struct name an action's wrong JSON type is reported against, as the
/// derived reader would word it.
trait Action {
    const STRUCT: &'static str;
}

impl Action for Put {
    const STRUCT: &'static str = "struct Put";
}

impl Action for Update {
    const STRUCT: &'static str = "struct Update";
}

impl Action for Delete {
    const STRUCT: &'static str = "struct Delete";
}

impl Action for ConditionCheck {
    const STRUCT: &'static str = "struct ConditionCheck";
}

/// Read one of an item's actions: null as left out, an object as the action.
/// A list where the object belongs fails the request with a
/// `SerializationException` that has no message (captured eu-west-2 and
/// us-east-1, 2026-11-11), where the derived reader would take an empty list
/// as an action with every member left out.
fn action<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Action + Deserialize<'de>,
{
    struct Reader<T>(PhantomData<T>);

    impl<'de, T: Action + Deserialize<'de>> de::Visitor<'de> for Reader<T> {
        type Value = Option<T>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str(T::STRUCT)
        }

        fn visit_none<E: de::Error>(self) -> Result<Option<T>, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Option<T>, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Option<T>, D::Error> {
            deserializer.deserialize_any(self)
        }

        fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Option<T>, A::Error> {
            T::deserialize(de::value::MapAccessDeserializer::new(map)).map(Some)
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, _: A) -> Result<Option<T>, A::Error> {
            Err(de::Error::custom(SERIALIZATION_MARKER))
        }
    }

    deserializer.deserialize_option(Reader(PhantomData))
}

/// A required member left null, in the newer form.
fn null_member(path: &str) -> String {
    format!("Value null at '{path}' failed to satisfy constraint: Member must not be null")
}

/// One member the first stage of the member checks looks at in an action.
enum Member<'a> {
    /// A required member, and whether it was left null.
    Required(&'static str, bool),
    /// The action's `ReturnValuesOnConditionCheckFailure`, listed if refused.
    Mode(Option<&'a str>),
}

/// An action's name on the wire and the members the first stage lists for
/// it, in the order DynamoDB lists them.
struct ActionMembers<'a> {
    action: &'static str,
    members: Vec<Member<'a>>,
}

impl Request {
    /// The request, or the message it is refused with, marked for its error
    /// class.
    ///
    /// An item given as null fails the request as it is read. Otherwise a
    /// required member left null is the first stage of DynamoDB's member
    /// checks. It lists each action's members in request order (see
    /// [`Item::members`]), then the token if its length is wrong, in the
    /// newer form, then the request's enums. Then come the stages
    /// [`super::stages_before_values`] runs, and only then a value refused in
    /// an item (captured eu-west-2 and us-east-1, 2026-11-11).
    pub(super) fn into_request(self) -> Result<TransactWriteItemsRequest, String> {
        let marked = |msg: String| format!("{VALIDATION_MARKER}{msg}");

        let items = match self.transact_items {
            None => None,
            Some(items) => match items.into_iter().collect::<Option<Vec<Item>>>() {
                Some(items) => Some(items),
                None => return Err(marked("TransactItems can't be null".to_string())),
            },
        };

        let mut listed = Vec::new();
        let any_null = match items {
            None => {
                listed.push(null_member("transactItems"));
                true
            }
            Some(ref items) => items
                .iter()
                .enumerate()
                .fold(false, |any, (position, item)| {
                    item.first_stage(position + 1, &mut listed) || any
                }),
        };
        if any_null {
            listed.extend(crate::token_constraint_error(
                self.client_request_token.as_deref(),
            ));
            listed.extend(super::enum_errors(
                self.return_consumed_capacity.as_deref(),
                self.return_item_collection_metrics.as_deref(),
            ));
            let msg = crate::validation::format_validation_errors(&listed)
                .expect("a null member is always listed");
            return Err(marked(msg));
        }

        let mut refusal = None;
        let transact_items: Vec<TransactWriteItem> = items
            .unwrap_or_default()
            .into_iter()
            .map(|item| item.into_item(&mut refusal))
            .collect();
        let request = TransactWriteItemsRequest {
            transact_items,
            client_request_token: self.client_request_token,
            return_consumed_capacity: self.return_consumed_capacity,
            return_item_collection_metrics: self.return_item_collection_metrics,
        };
        match refusal {
            None => Ok(request),
            Some(refusal) => Err(super::stages_before_values(&request).map_or(refusal, marked)),
        }
    }
}

impl Item {
    /// Each action this item names, with the members the first stage lists
    /// for it. Put lists its item, its mode, then its table name; Update its
    /// mode, its expression, its key, then its table name; ConditionCheck its
    /// expression, its mode, its key, then its table name (captured eu-west-2
    /// and us-east-1, 2026-11-11). Delete, which has no required expression,
    /// is taken to list its members as the two other actions with a key do:
    /// its mode, its key, then its table name.
    fn members(&self) -> impl Iterator<Item = ActionMembers<'_>> {
        let put = self.put.as_ref().map(|a| ActionMembers {
            action: "put",
            members: vec![
                Member::Required("item", a.item.is_null()),
                Member::Mode(a.return_values_on_condition_check_failure.as_deref()),
                Member::Required("tableName", a.table_name.is_none()),
            ],
        });
        let update = self.update.as_ref().map(|a| ActionMembers {
            action: "update",
            members: vec![
                Member::Mode(a.return_values_on_condition_check_failure.as_deref()),
                Member::Required("updateExpression", a.update_expression.is_none()),
                Member::Required("key", a.key.is_null()),
                Member::Required("tableName", a.table_name.is_none()),
            ],
        });
        let delete = self.delete.as_ref().map(|a| ActionMembers {
            action: "delete",
            members: vec![
                Member::Mode(a.return_values_on_condition_check_failure.as_deref()),
                Member::Required("key", a.key.is_null()),
                Member::Required("tableName", a.table_name.is_none()),
            ],
        });
        let condition_check = self.condition_check.as_ref().map(|a| ActionMembers {
            action: "conditionCheck",
            members: vec![
                Member::Required("conditionExpression", a.condition_expression.is_none()),
                Member::Mode(a.return_values_on_condition_check_failure.as_deref()),
                Member::Required("key", a.key.is_null()),
                Member::Required("tableName", a.table_name.is_none()),
            ],
        });
        [put, update, delete, condition_check].into_iter().flatten()
    }

    /// This item's part of the first stage's list, pushed onto `out`: each
    /// action's required members left null and its refused mode, under the
    /// item's `position`. Returns whether any member was null, since a
    /// refused mode alone does not make the stage fire.
    fn first_stage(&self, position: usize, out: &mut Vec<String>) -> bool {
        let mut any_null = false;
        for ActionMembers { action, members } in self.members() {
            let at = |member: &str| format!("transactItems.{position}.member.{action}.{member}");
            for member in members {
                match member {
                    Member::Required(name, true) => {
                        out.push(null_member(&at(name)));
                        any_null = true;
                    }
                    Member::Required(_, false) => {}
                    Member::Mode(mode) => out.extend(
                        crate::validation::return_values_on_condition_check_failure_rejection(
                            mode,
                            &at("returnValuesOnConditionCheckFailure"),
                        ),
                    ),
                }
            }
        }
        any_null
    }

    /// The public item, once no required member is null, with the first value
    /// refused anywhere in the request kept in `refusal`. An action's item or
    /// key is taken ahead of its expression values, so a value refused in the
    /// item or key is the one reported whichever was written first (captured
    /// eu-west-2 and us-east-1, 2026-11-11).
    fn into_item(self, refusal: &mut Option<String>) -> TransactWriteItem {
        TransactWriteItem {
            put: self.put.map(|put| TransactPut {
                table_name: put.table_name.unwrap_or_default(),
                item: put.item.take(refusal).unwrap_or_default(),
                condition_expression: put.condition_expression,
                expression_attribute_names: put.expression_attribute_names,
                expression_attribute_values: put.expression_attribute_values.take(refusal),
                return_values_on_condition_check_failure: put
                    .return_values_on_condition_check_failure,
            }),
            update: self.update.map(|update| TransactUpdate {
                table_name: update.table_name.unwrap_or_default(),
                key: update.key.take(refusal).unwrap_or_default(),
                update_expression: update.update_expression.unwrap_or_default(),
                condition_expression: update.condition_expression,
                expression_attribute_names: update.expression_attribute_names,
                expression_attribute_values: update.expression_attribute_values.take(refusal),
                return_values_on_condition_check_failure: update
                    .return_values_on_condition_check_failure,
            }),
            delete: self.delete.map(|delete| TransactDelete {
                table_name: delete.table_name.unwrap_or_default(),
                key: delete.key.take(refusal).unwrap_or_default(),
                condition_expression: delete.condition_expression,
                expression_attribute_names: delete.expression_attribute_names,
                expression_attribute_values: delete.expression_attribute_values.take(refusal),
                return_values_on_condition_check_failure: delete
                    .return_values_on_condition_check_failure,
            }),
            condition_check: self.condition_check.map(|check| TransactConditionCheck {
                table_name: check.table_name.unwrap_or_default(),
                key: check.key.take(refusal).unwrap_or_default(),
                condition_expression: check.condition_expression.unwrap_or_default(),
                expression_attribute_names: check.expression_attribute_names,
                expression_attribute_values: check.expression_attribute_values.take(refusal),
                return_values_on_condition_check_failure: check
                    .return_values_on_condition_check_failure,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::IntoDeserializer;

    #[test]
    fn an_action_given_as_a_list_fails_the_read_unmarked_by_any_message() {
        let read: Result<Item, _> =
            Item::deserialize(serde_json::json!({"Put": []}).into_deserializer());
        let msg = read.err().expect("a list is refused").to_string();
        assert!(msg.starts_with(SERIALIZATION_MARKER), "{msg}");
    }
}
