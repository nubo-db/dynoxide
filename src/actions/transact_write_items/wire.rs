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
use crate::types::{self, AttributeValue, HeldValues};
use serde::Deserialize;
use serde::de::{self, Deserializer};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

#[derive(Deserialize)]
pub(super) struct Request {
    #[serde(rename = "TransactItems", default)]
    transact_items: Option<Vec<ListedItem>>,
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
    #[serde(rename = "Item", default)]
    item: Option<HeldValues>,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<crate::empty_names::WrittenNames>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values: Option<HeldValues>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
}

#[derive(Deserialize)]
struct Update {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "Key", default)]
    key: Option<HeldValues>,
    #[serde(rename = "UpdateExpression", default)]
    update_expression: Option<String>,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<crate::empty_names::WrittenNames>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values: Option<HeldValues>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
}

#[derive(Deserialize)]
struct Delete {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "Key", default)]
    key: Option<HeldValues>,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<crate::empty_names::WrittenNames>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values: Option<HeldValues>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
}

#[derive(Deserialize)]
struct ConditionCheck {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "Key", default)]
    key: Option<HeldValues>,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<crate::empty_names::WrittenNames>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values: Option<HeldValues>,
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

/// One entry of `TransactItems`: an item, or `None` for one given as null.
struct ListedItem(Option<Item>);

impl<'de> Deserialize<'de> for ListedItem {
    /// Read an entry: null as an item left null, an object as an item, and an
    /// empty list as an item naming no action. A list with anything in it is
    /// refused with a `SerializationException` that has no message, and a
    /// string, number or boolean with DynamoDB's message for a value of the
    /// wrong type in the list (captured eu-west-2, us-east-1, eu-west-1 and
    /// us-west-2, 2026-11-11).
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reader;

        fn refuse<E: de::Error>() -> Result<ListedItem, E> {
            Err(E::custom(format!(
                "{SERIALIZATION_MARKER}Unexpected value type in payload"
            )))
        }

        impl<'de> de::Visitor<'de> for Reader {
            type Value = ListedItem;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("struct Item")
            }

            fn visit_none<E: de::Error>(self) -> Result<ListedItem, E> {
                Ok(ListedItem(None))
            }

            fn visit_unit<E: de::Error>(self) -> Result<ListedItem, E> {
                Ok(ListedItem(None))
            }

            fn visit_some<D: Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<ListedItem, D::Error> {
                deserializer.deserialize_any(self)
            }

            fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<ListedItem, A::Error> {
                Item::deserialize(de::value::MapAccessDeserializer::new(map))
                    .map(|item| ListedItem(Some(item)))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<ListedItem, A::Error> {
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom(SERIALIZATION_MARKER));
                }
                Ok(ListedItem(Some(Item {
                    put: None,
                    update: None,
                    delete: None,
                    condition_check: None,
                })))
            }

            fn visit_bool<E: de::Error>(self, _: bool) -> Result<ListedItem, E> {
                refuse()
            }

            fn visit_i64<E: de::Error>(self, _: i64) -> Result<ListedItem, E> {
                refuse()
            }

            fn visit_u64<E: de::Error>(self, _: u64) -> Result<ListedItem, E> {
                refuse()
            }

            fn visit_f64<E: de::Error>(self, _: f64) -> Result<ListedItem, E> {
                refuse()
            }

            fn visit_str<E: de::Error>(self, _: &str) -> Result<ListedItem, E> {
                refuse()
            }
        }

        deserializer.deserialize_option(Reader)
    }
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
    /// A required member left null is the first stage of DynamoDB's member
    /// checks. It lists each action's members in request order (see
    /// [`Item::members`]), then the token if its length is wrong, in the
    /// newer form, then the request's enums. An item given as null makes the
    /// stage fire too, though it is not listed: when nothing else is, the
    /// request is refused with `TransactItems can't be null`. Then come the
    /// stages
    /// [`super::stages_before_values`] runs, and only then a value refused in
    /// an item (captured eu-west-2 and us-east-1, 2026-11-11). The values are
    /// read once the whole request has been, since a null anywhere in it
    /// changes how every action's values are read (see [`read_values`]).
    pub(super) fn into_request(self) -> Result<TransactWriteItemsRequest, String> {
        let marked = |msg: String| format!("{VALIDATION_MARKER}{msg}");

        let mut listed = Vec::new();
        let any_null = match self.transact_items {
            None => {
                listed.push(null_member("transactItems"));
                true
            }
            Some(ref items) => {
                items
                    .iter()
                    .enumerate()
                    .fold(false, |any, (position, ListedItem(item))| {
                        item.as_ref()
                            .is_none_or(|item| item.first_stage(position + 1, &mut listed))
                            || any
                    })
            }
        };
        if any_null {
            listed.extend(crate::token_constraint_error(
                self.client_request_token.as_deref(),
            ));
            listed.extend(super::enum_errors(
                self.return_consumed_capacity.as_deref(),
                self.return_item_collection_metrics.as_deref(),
            ));
            return Err(marked(
                crate::validation::format_validation_errors(&listed)
                    .unwrap_or_else(|| "TransactItems can't be null".to_string()),
            ));
        }

        let items: Vec<Item> = self
            .transact_items
            .unwrap_or_default()
            .into_iter()
            .flat_map(|ListedItem(item)| item)
            .collect();
        // A null where a value belongs puts the request's values on the older
        // layer, so they are read there, wherever the request came from.
        let null_in_request = items.iter().any(Item::holds_null);
        let read = || {
            let mut refusal = None;
            let items: Vec<TransactWriteItem> = items
                .into_iter()
                .map(|item| item.into_item(null_in_request, &mut refusal))
                .collect();
            (items, refusal)
        };
        let (transact_items, refusal) = if null_in_request {
            types::with_value_layer(crate::validation::ValueLayer::Older, read)
        } else {
            read()
        };
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
                Member::Required("item", a.item.is_none()),
                Member::Mode(a.return_values_on_condition_check_failure.as_deref()),
                Member::Required("tableName", a.table_name.is_none()),
            ],
        });
        let update = self.update.as_ref().map(|a| ActionMembers {
            action: "update",
            members: vec![
                Member::Mode(a.return_values_on_condition_check_failure.as_deref()),
                Member::Required("updateExpression", a.update_expression.is_none()),
                Member::Required("key", a.key.is_none()),
                Member::Required("tableName", a.table_name.is_none()),
            ],
        });
        let delete = self.delete.as_ref().map(|a| ActionMembers {
            action: "delete",
            members: vec![
                Member::Mode(a.return_values_on_condition_check_failure.as_deref()),
                Member::Required("key", a.key.is_none()),
                Member::Required("tableName", a.table_name.is_none()),
            ],
        });
        let condition_check = self.condition_check.as_ref().map(|a| ActionMembers {
            action: "conditionCheck",
            members: vec![
                Member::Required("conditionExpression", a.condition_expression.is_none()),
                Member::Mode(a.return_values_on_condition_check_failure.as_deref()),
                Member::Required("key", a.key.is_none()),
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

    /// Whether any action of this item holds a JSON null where an attribute
    /// value belongs, in its item, key or expression values (see
    /// [`types::holds_null`]), or a null name in its expression names, which
    /// puts the request on the older layer as it does a single-item write
    /// (captured eu-west-2 and us-east-1, 2026-11-12).
    fn holds_null(&self) -> bool {
        let held = |values: &Option<HeldValues>| {
            values.as_ref().is_some_and(|values| {
                values
                    .0
                    .0
                    .iter()
                    .any(|(_, value)| value.is_null() || types::holds_null(value))
            })
        };
        let null_name = |names: &Option<crate::empty_names::WrittenNames>| {
            names
                .as_ref()
                .is_some_and(|names| names.values().any(Option::is_none))
        };
        self.put.as_ref().is_some_and(|a| {
            held(&a.item)
                || held(&a.expression_attribute_values)
                || null_name(&a.expression_attribute_names)
        }) || self.update.as_ref().is_some_and(|a| {
            held(&a.key)
                || held(&a.expression_attribute_values)
                || null_name(&a.expression_attribute_names)
        }) || self.delete.as_ref().is_some_and(|a| {
            held(&a.key)
                || held(&a.expression_attribute_values)
                || null_name(&a.expression_attribute_names)
        }) || self.condition_check.as_ref().is_some_and(|a| {
            held(&a.key)
                || held(&a.expression_attribute_values)
                || null_name(&a.expression_attribute_names)
        })
    }

    /// The public item, once no required member is null, with the first value
    /// refused anywhere in the request kept in `refusal` (see [`read_values`]).
    fn into_item(self, null_in_request: bool, refusal: &mut Option<String>) -> TransactWriteItem {
        let read = |attributes, values, refusal: &mut Option<String>| {
            read_values(attributes, values, null_in_request, refusal)
        };
        TransactWriteItem {
            put: self.put.map(|put| {
                let (item, values) = read(put.item, put.expression_attribute_values, refusal);
                TransactPut {
                    table_name: put.table_name.unwrap_or_default(),
                    item,
                    expression_attribute_names: crate::empty_names::action_names(
                        put.expression_attribute_names,
                        put.condition_expression.is_some(),
                    ),
                    condition_expression: put.condition_expression,
                    expression_attribute_values: values,
                    return_values_on_condition_check_failure: put
                        .return_values_on_condition_check_failure,
                }
            }),
            update: self.update.map(|update| {
                let (key, values) = read(update.key, update.expression_attribute_values, refusal);
                TransactUpdate {
                    table_name: update.table_name.unwrap_or_default(),
                    key,
                    expression_attribute_names: crate::empty_names::action_names(
                        update.expression_attribute_names,
                        update.update_expression.is_some() || update.condition_expression.is_some(),
                    ),
                    update_expression: update.update_expression.unwrap_or_default(),
                    condition_expression: update.condition_expression,
                    expression_attribute_values: values,
                    return_values_on_condition_check_failure: update
                        .return_values_on_condition_check_failure,
                }
            }),
            delete: self.delete.map(|delete| {
                let (key, values) = read(delete.key, delete.expression_attribute_values, refusal);
                TransactDelete {
                    table_name: delete.table_name.unwrap_or_default(),
                    key,
                    expression_attribute_names: crate::empty_names::action_names(
                        delete.expression_attribute_names,
                        delete.condition_expression.is_some(),
                    ),
                    condition_expression: delete.condition_expression,
                    expression_attribute_values: values,
                    return_values_on_condition_check_failure: delete
                        .return_values_on_condition_check_failure,
                }
            }),
            condition_check: self.condition_check.map(|check| {
                let (key, values) = read(check.key, check.expression_attribute_values, refusal);
                TransactConditionCheck {
                    table_name: check.table_name.unwrap_or_default(),
                    key,
                    expression_attribute_names: crate::empty_names::action_names(
                        check.expression_attribute_names,
                        check.condition_expression.is_some(),
                    ),
                    condition_expression: check.condition_expression.unwrap_or_default(),
                    expression_attribute_values: values,
                    return_values_on_condition_check_failure: check
                        .return_values_on_condition_check_failure,
                }
            }),
        }
    }
}

/// One action's item or key and its expression values, read once the whole
/// request has been, with the action's refusal kept in `refusal` if none is
/// yet. The item or key is read as [`types::read_attribute_map`] reads it, a
/// null member named and its faults met in DynamoDB's map order, and the
/// values as [`types::read_transaction_values`] reads them.
///
/// Which of the two is reported turns on the null, whichever is written first
/// (captured eu-west-2 and us-east-1, 2026-11-11). Without a null anywhere in
/// the request, the item or key comes first and a bad number in the values is
/// reported bare. With one, the values come first and a bad number in them is
/// named by key, as a null is.
fn read_values(
    attributes: Option<HeldValues>,
    values: Option<HeldValues>,
    null_in_request: bool,
    refusal: &mut Option<String>,
) -> (
    HashMap<String, AttributeValue>,
    Option<HashMap<String, AttributeValue>>,
) {
    let attributes = attributes.map(|held| held.read(types::read_attribute_map));
    let values = values
        .map(|held| held.read(|members| types::read_transaction_values(members, null_in_request)));
    let attributes_refusal = attributes.as_ref().and_then(|read| read.as_ref().err());
    let values_refusal = values.as_ref().and_then(|read| read.as_ref().err());
    let ahead = if null_in_request {
        values_refusal.or(attributes_refusal)
    } else {
        attributes_refusal.or(values_refusal)
    };
    if let Some(ahead) = ahead
        && refusal.is_none()
    {
        *refusal = Some(ahead.clone());
    }
    (
        attributes.and_then(Result::ok).unwrap_or_default(),
        values.and_then(Result::ok),
    )
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
