//! PartiQL statement executor.
//!
//! Maps parsed PartiQL statements to internal DynamoDB operations.

use crate::actions::index_capacity::WriteCapacity;
use crate::errors::{DynoxideError, Result};
use crate::expressions::condition::compare_values;
use crate::expressions::key_condition::{ResolvedSortKeyCondition, sk_conditions_to_sql};
use crate::partiql::parser::{
    CompOp, Operand, OrderByTerm, PartiqlValue, ReturningVariant, SetTerm, SetValue, Statement,
    WhereClause, WhereCondition,
};
use crate::partiql::{PathSegment, names_attribute, split_path_segments};
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item, ScalarAttributeType};
use std::collections::HashMap;
use std::sync::Arc;

/// One page of a statement's result.
///
/// Only a `SELECT` paginates. Every other statement returns the whole result
/// and no continuation token.
#[derive(Debug, Default)]
pub struct StatementPage {
    /// The rows this page carries: `Some` for a SELECT (possibly empty) and
    /// for an UPDATE or DELETE with a `RETURNING` clause (the deleted item or
    /// the requested projection), `None` for a write without one.
    pub items: Option<Vec<Item>>,
    /// Total item bytes touched, for `ConsumedCapacity`.
    pub size: usize,
    /// What a write statement contributed to `ConsumedCapacity`: the images
    /// either side and the per-index units. `None` for a `SELECT`, which is
    /// charged from `size` at read granularity instead.
    pub capacity: Option<WriteCapacity>,
    /// Where to resume, when more rows matched than this page returned.
    pub next_token: Option<String>,
    /// The index a `SELECT` was served from. `None` for a base table read and
    /// for every write. Capacity lands on this index's arm rather than on the
    /// table's, which the caller cannot work out from the statement alone.
    pub read_index: Option<ReadIndex>,
    /// What an LSI reach-back cost on the base table, in read units, for the
    /// attributes the index does not project. Charged to the table arm, which is
    /// why an index read can report a non-zero one.
    ///
    /// Units rather than a row count, because each base read is charged on its
    /// own bytes at the request's consistency and the sizes are only known here.
    pub base_read_units: f64,
}

/// The index a `SELECT` was served from.
#[derive(Debug, Clone)]
pub struct ReadIndex {
    pub name: String,
    /// LSIs and GSIs land on different arms of `ConsumedCapacity`.
    pub is_lsi: bool,
}

/// The WHERE clause a statement carries, if any. `INSERT` never has one.
fn statement_where_clause(stmt: &Statement) -> Option<&WhereClause> {
    match stmt {
        Statement::Select { where_clause, .. }
        | Statement::Update { where_clause, .. }
        | Statement::Delete { where_clause, .. } => where_clause.as_ref(),
        Statement::Insert { .. } => None,
    }
}

/// A key value as stored, or DynamoDB's refusal of a value that can't be a key:
/// `Key value must be of type S, N, or B. Key name: pk, Key type: M` (captured
/// eu-west-2, 2026-10-09, for a map; other types are named the same way).
fn key_string(value: &AttributeValue, key_name: &str) -> Result<String> {
    value.to_key_string().ok_or_else(|| {
        DynoxideError::ValidationException(format!(
            "Key value must be of type S, N, or B. Key name: {key_name}, Key type: {}",
            value.type_name()
        ))
    })
}

/// Why a key value can't be used, in DynamoDB's words (captured eu-west-2 and
/// us-east-1, 2026-10-09).
#[derive(Debug)]
pub(crate) enum KeyFault {
    /// A type no key can have: a map, a list, a boolean, null or a set.
    NotAKeyType(String),
    /// A key type, but not the one the table declares for this key.
    WrongType(String),
}

impl From<KeyFault> for DynoxideError {
    fn from(fault: KeyFault) -> Self {
        match fault {
            KeyFault::NotAKeyType(message) | KeyFault::WrongType(message) => {
                DynoxideError::ValidationException(message)
            }
        }
    }
}

/// A key value as stored, checked against the type declared for the key: one
/// no key can have is `Key value must be of type S, N, or B. Key name: pk, Key
/// type: M`, and one of another key type is `Key attribute's data type should
/// match its data type in table's schema: Key pk`. With no declared type to
/// hand, only the first is checked.
fn typed_key(
    value: &AttributeValue,
    key_name: &str,
    declared: Option<&ScalarAttributeType>,
) -> std::result::Result<String, KeyFault> {
    let Some(stored) = value.to_key_string() else {
        return Err(KeyFault::NotAKeyType(format!(
            "Key value must be of type S, N, or B. Key name: {key_name}, Key type: {}",
            value.type_name()
        )));
    };
    let matches = match declared {
        None => true,
        Some(ScalarAttributeType::S) => matches!(value, AttributeValue::S(_)),
        Some(ScalarAttributeType::N) => matches!(value, AttributeValue::N(_)),
        Some(ScalarAttributeType::B) => matches!(value, AttributeValue::B(_)),
    };
    if !matches {
        return Err(KeyFault::WrongType(format!(
            "Key attribute's data type should match its data type in table's schema: Key {key_name}"
        )));
    }
    Ok(stored)
}

/// The type `AttributeDefinitions` declares for an attribute.
fn declared_attribute_type(
    meta: &crate::storage::TableMetadata,
    attr: &str,
) -> Option<ScalarAttributeType> {
    let defs: Vec<crate::types::AttributeDefinition> =
        serde_json::from_str(&meta.attribute_definitions).ok()?;
    defs.into_iter()
        .find(|d| d.attribute_name == attr)
        .map(|d| d.attribute_type)
}

/// One condition a branch of a WHERE clause puts on a key attribute: what it
/// does and the values it names, resolved. `=` and `IN` are one kind, so
/// `pk = 1` and `pk IN [1]` are the same condition.
struct KeyCondition {
    op: &'static str,
    values: Vec<AttributeValue>,
}

impl PartialEq for KeyCondition {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
            && if self.op == "=" {
                self.values.len() == other.values.len()
                    && self.values.iter().all(|v| other.values.contains(v))
            } else {
                self.values == other.values
            }
    }
}

/// The distinct conditions a branch puts on one key attribute, in the order
/// written; the same condition written twice counts once. A partition key is
/// read by `=` and `IN`, a sort key by those, the ordering comparisons,
/// `BETWEEN` and `begins_with`. `None` when a value can't be resolved, which is
/// reported elsewhere.
fn key_conditions(
    group: &[Arc<WhereCondition>],
    key: &str,
    sort_key: bool,
    parameters: &[AttributeValue],
) -> Option<Vec<KeyCondition>> {
    let resolve = |values: &[&PartiqlValue]| -> Option<Vec<AttributeValue>> {
        values
            .iter()
            .map(|v| resolve_value(v, parameters).ok())
            .collect()
    };
    let mut found: Vec<KeyCondition> = Vec::new();
    for cond in group {
        let (op, values): (&'static str, Vec<&PartiqlValue>) = match &**cond {
            WhereCondition::Comparison(c) if names_attribute(&c.path, key) => match c.op {
                CompOp::Eq => ("=", vec![&c.value]),
                CompOp::Lt if sort_key => ("<", vec![&c.value]),
                CompOp::Le if sort_key => ("<=", vec![&c.value]),
                CompOp::Gt if sort_key => (">", vec![&c.value]),
                CompOp::Ge if sort_key => (">=", vec![&c.value]),
                _ => continue,
            },
            WhereCondition::In(path, values) if names_attribute(path, key) => {
                ("=", values.iter().collect())
            }
            WhereCondition::Between(path, lo, hi) if sort_key && names_attribute(path, key) => {
                ("BETWEEN", vec![lo, hi])
            }
            WhereCondition::BeginsWith(path, prefix) if sort_key && names_attribute(path, key) => {
                ("begins_with", vec![prefix])
            }
            _ => continue,
        };
        let mut resolved = resolve(&values)?;
        if op == "=" {
            resolved = first_of_each(resolved, |value| {
                let mut key = Vec::new();
                encode_scalar(value, &mut key).then_some(key)
            });
        }
        found.push(KeyCondition {
            op,
            values: resolved,
        });
    }
    Some(first_of_each(found, |condition| {
        let mut encoded = Vec::with_capacity(condition.values.len());
        for value in &condition.values {
            let mut one = Vec::new();
            if !encode_scalar(value, &mut one) {
                return None;
            }
            encoded.push(one);
        }
        // An equality's values (an `IN` list) are a set, so the same list in
        // another order is the same condition; BETWEEN's two are not.
        if condition.op == "=" {
            encoded.sort_unstable();
        }
        let mut key = condition.op.as_bytes().to_vec();
        for one in encoded {
            key.extend_from_slice(&one);
        }
        Some(key)
    }))
}

/// Keep the first of each repeated item, in the order given. Items `key` can
/// encode are found through a hash set; any it can't (a non-scalar value,
/// which no key condition can use) are compared among themselves as before.
fn first_of_each<T: PartialEq>(
    items: impl IntoIterator<Item = T>,
    key: impl Fn(&T) -> Option<Vec<u8>>,
) -> Vec<T> {
    let mut seen = std::collections::HashSet::new();
    let mut out: Vec<T> = Vec::new();
    let mut unkeyed: Vec<usize> = Vec::new();
    for item in items {
        let fresh = match key(&item) {
            Some(k) => seen.insert(k),
            None => !unkeyed.iter().any(|&i| out[i] == item),
        };
        if fresh {
            if key(&item).is_none() {
                unkeyed.push(out.len());
            }
            out.push(item);
        }
    }
    out
}

/// Append an S, N or B value to `key` as a tag and its length-prefixed bytes,
/// so two encodings match exactly when the values are equal. `false` for any
/// other type.
fn encode_scalar(value: &AttributeValue, key: &mut Vec<u8>) -> bool {
    let (tag, bytes) = match value {
        AttributeValue::S(s) => (b'S', s.as_bytes()),
        AttributeValue::N(n) => (b'N', n.as_bytes()),
        AttributeValue::B(b) => (b'B', b.as_slice()),
        _ => return false,
    };
    key.push(tag);
    key.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    key.extend_from_slice(bytes);
    true
}

/// Refuse a SELECT key value DynamoDB can't read the key by, as
/// [`typed_key`] words it (captured eu-west-2 and us-east-1, 2026-10-09).
///
/// The OR branches are checked in the order written. A branch that names the
/// partition key with one `=` or `IN` has its values checked, then the sort
/// key's, whatever the condition on it. A branch that doesn't name the
/// partition key, or names it or the sort key two different ways, ends the
/// checking there, its own values unchecked, and what is left just filters:
/// `pk = true AND pk = 'x'` returns nothing, and `a = 1 OR pk = true` returns
/// nothing where `pk = true OR a = 1` is refused. `pk = 1 AND pk IN [1]` is one
/// condition, as is the same one written twice.
fn check_read_key_types(
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    read_pk: &str,
    read_sk: Option<&str>,
    declared: &dyn Fn(&str) -> Option<ScalarAttributeType>,
) -> Result<()> {
    let Some(wc) = where_clause else {
        return Ok(());
    };
    for group in &wc.groups {
        let Some(pk) = key_conditions(group, read_pk, false, parameters) else {
            return Ok(());
        };
        let pk = match pk.as_slice() {
            [one] => one,
            _ => return Ok(()),
        };
        let sk = match read_sk {
            Some(sk_name) => {
                let Some(conditions) = key_conditions(group, sk_name, true, parameters) else {
                    return Ok(());
                };
                if conditions.len() > 1 {
                    return Ok(());
                }
                conditions.into_iter().next().map(|c| (sk_name, c.values))
            }
            None => None,
        };
        for (name, values) in std::iter::once((read_pk, pk.values.clone())).chain(sk) {
            let declared = declared(name);
            for value in &values {
                typed_key(value, name, declared.as_ref())?;
            }
        }
    }
    Ok(())
}

/// What a batch or a transaction makes of a SELECT. Both serve it as a
/// GetItem, so its WHERE clause may name each key attribute with `=` and
/// nothing else (captured eu-west-2, 2026-10-09: `WHERE pk = 'a' AND data = ?`
/// is refused by both, where ExecuteStatement runs it).
pub(crate) enum KeyedSelect {
    /// It names exactly the primary key.
    ByPrimaryKey,
    /// It names something else as well, or not the whole key.
    NotByPrimaryKey,
    /// It names the key, with a value of a type no key can have; the message
    /// is the one ExecuteStatement gives.
    KeyType(String),
}

pub(crate) fn keyed_select(
    stmt: &Statement,
    parameters: &[AttributeValue],
    resolved: &ResolvedTable,
) -> KeyedSelect {
    let Statement::Select {
        where_clause: Some(wc),
        ..
    } = stmt
    else {
        return KeyedSelect::NotByPrimaryKey;
    };
    let [group] = wc.groups.as_slice() else {
        return KeyedSelect::NotByPrimaryKey;
    };
    let schema = &resolved.key_schema;
    let keys: Vec<(&str, Option<&ScalarAttributeType>)> = std::iter::once((
        schema.partition_key.as_str(),
        Some(&schema.partition_key_type),
    ))
    .chain(
        schema
            .sort_key
            .as_deref()
            .map(|sk| (sk, schema.sort_key_type.as_ref())),
    )
    .collect();
    // Each condition has to be an `=`, or an `IN` with one value, on a key
    // attribute; the same one written twice is still one (captured eu-west-2
    // and us-east-1, 2026-10-09).
    let names_a_key = |path: &str| keys.iter().any(|(name, _)| names_attribute(path, name));
    let single = group.iter().all(|cond| match &**cond {
        WhereCondition::Comparison(c) => c.op == CompOp::Eq && names_a_key(&c.path),
        WhereCondition::In(path, values) => values.len() == 1 && names_a_key(path),
        _ => false,
    });
    if !single {
        return KeyedSelect::NotByPrimaryKey;
    }
    let mut values = Vec::with_capacity(keys.len());
    for (name, declared) in &keys {
        let Some(conditions) = key_conditions(group, name, false, parameters) else {
            return KeyedSelect::NotByPrimaryKey;
        };
        // `pk = 'a' AND pk IN ['a']` names one value (captured eu-west-2 and
        // us-east-1, 2026-10-09).
        let [KeyCondition { values: named, .. }] = conditions.as_slice() else {
            return KeyedSelect::NotByPrimaryKey;
        };
        let [value] = named.as_slice() else {
            return KeyedSelect::NotByPrimaryKey;
        };
        values.push((*name, *declared, value.clone()));
    }
    for (name, declared, value) in values {
        if let Err(KeyFault::NotAKeyType(message) | KeyFault::WrongType(message)) =
            typed_key(&value, name, declared)
        {
            return KeyedSelect::KeyType(message);
        }
    }
    KeyedSelect::ByPrimaryKey
}

/// Refuse a SELECT in a batch or a transaction whose key conditions overlap,
/// which DynamoDB checks before it checks that the SELECT names one item
/// (captured eu-west-2 and us-east-1, 2026-10-09: `pk IN ['a', 'a'] AND sk =
/// 1` is refused for overlapping).
pub(crate) fn reject_overlapping_select(
    stmt: &Statement,
    parameters: &[AttributeValue],
    resolved: &ResolvedTable,
) -> Result<()> {
    let Statement::Select { where_clause, .. } = stmt else {
        return Ok(());
    };
    reject_overlapping_key_conditions(
        where_clause.as_ref(),
        parameters,
        &resolved.key_schema.partition_key,
        resolved.key_schema.sort_key.as_deref(),
    )
}

/// Refuse the key value of an UPDATE or DELETE that DynamoDB refuses, as
/// [`typed_key`] words it: what a transaction checks of a write's key before
/// it runs anything.
pub(crate) fn write_key_fault(
    stmt: &Statement,
    parameters: &[AttributeValue],
    resolved: &ResolvedTable,
) -> Option<KeyFault> {
    let (Statement::Update {
        where_clause: Some(wc),
        ..
    }
    | Statement::Delete {
        where_clause: Some(wc),
        ..
    }) = stmt
    else {
        return None;
    };
    let schema = &resolved.key_schema;
    let keys = std::iter::once((&schema.partition_key, Some(&schema.partition_key_type))).chain(
        schema
            .sort_key
            .as_ref()
            .map(|sk| (sk, schema.sort_key_type.as_ref())),
    );
    let keys: Vec<_> = keys.collect();
    for group in &wc.groups {
        for (name, declared) in &keys {
            let Some(cond) = find_comparison(group, name) else {
                continue;
            };
            let Ok(value) = resolve_value(&cond.value, parameters) else {
                continue;
            };
            if let Err(fault) = typed_key(&value, name, *declared) {
                return Some(fault);
            }
        }
    }
    None
}

/// Check an INSERT's key the way a transaction does, with PutItem's messages:
/// a key of the wrong type is `One or more parameter values were invalid: Type
/// mismatch for key pk expected: N actual: M`, where ExecuteStatement and
/// BatchExecuteStatement give `Key attribute's data type should match its data
/// type in table's schema` (captured eu-west-2 and us-east-1, 2026-10-09).
pub(crate) fn check_transaction_insert_keys(
    stmt: &Statement,
    parameters: &[AttributeValue],
    resolved: &ResolvedTable,
) -> Result<()> {
    let Statement::Insert { item, .. } = stmt else {
        return Ok(());
    };
    let item = resolve_insert_item(item, parameters)?;
    crate::actions::helpers::validate_item_keys(&item, &resolved.key_schema, &resolved.meta)
}

/// Whether a DELETE's WHERE clause asks anything of the item beyond naming its
/// key. A transaction that is cancelled reports such a DELETE of a missing
/// item as a failed condition, where one naming only the key reports `None`
/// (captured eu-west-2 and us-east-1, 2026-10-10, for `v = 1` beside the key;
/// any other condition is inferred to count the same).
pub(crate) fn delete_has_condition(stmt: &Statement, resolved: &ResolvedTable) -> bool {
    let Statement::Delete {
        where_clause: Some(wc),
        ..
    } = stmt
    else {
        return false;
    };
    let schema = &resolved.key_schema;
    let is_key = |path: &str| {
        names_attribute(path, &schema.partition_key)
            || schema
                .sort_key
                .as_deref()
                .is_some_and(|sk| names_attribute(path, sk))
    };
    let names_key = |condition: &WhereCondition| match condition {
        WhereCondition::Comparison(c) => c.op == CompOp::Eq && is_key(&c.path),
        WhereCondition::In(path, values) => values.len() == 1 && is_key(path),
        _ => false,
    };
    !matches!(wc.groups.as_slice(), [group] if group.iter().all(|c| names_key(c)))
}

/// Check one statement parameter as any attribute value is checked, with no
/// path in the message: its numbers, and empty or duplicated sets. DynamoDB
/// checks a statement's parameters before it reads the statement, whether it
/// uses them or not, on `ExecuteStatement`, `BatchExecuteStatement` and
/// `ExecuteTransaction` alike (captured eu-west-2 and us-east-1, 2026-10-09).
pub(crate) fn check_parameter_value(value: &AttributeValue) -> Result<()> {
    crate::validation::validate_parameter_value(value)
}

/// One thing a branch of a WHERE clause says about the sort key, as a stored
/// key, so values compare in the key's own order.
#[derive(PartialEq)]
enum SortKeyAtom {
    /// Equal to one of these.
    Points(Vec<String>),
    /// At least, or more than, this.
    Above(String, bool),
    /// At most, or less than, this.
    Below(String, bool),
    /// Starting with this.
    Prefix(String),
}

/// Refuse a SELECT whose key conditions overlap, as DynamoDB does (captured
/// eu-west-2 and us-east-1, 2026-10-09).
///
/// When every OR branch pins the partition key with `=` or `IN`, DynamoDB
/// reads the clause as one key range per branch and partition, the sort key
/// conditions narrowing each, and refuses it when two ranges share a key: the
/// same partition named twice with no sort key condition between them, sort
/// key ranges that meet (closed ends touching count), a key repeated in an
/// `IN` list, or `1` and `1.0`. Other conditions don't narrow a range. A branch
/// that doesn't pin the partition key makes the read a scan, and nothing is
/// checked; nor is anything when a branch puts two different conditions on
/// the sort key (`sk > 0 AND sk < 3`), though the same one twice counts once,
/// or a `NOT ... BETWEEN` on it.
fn reject_overlapping_key_conditions(
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    read_pk: &str,
    read_sk: Option<&str>,
) -> Result<()> {
    let overlap = || {
        DynoxideError::ValidationException(
            "Overlapping conditions with range keys are not supported in where clause".to_string(),
        )
    };
    let Some(wc) = where_clause else {
        return Ok(());
    };
    let key = |value: &PartiqlValue| {
        resolve_value(value, parameters)
            .ok()
            .and_then(|av| av.to_key_string())
    };
    let keys_of = |values: &[PartiqlValue]| -> Result<Vec<String>> {
        let keys: Vec<String> = values.iter().filter_map(key).collect();
        let mut seen = std::collections::HashSet::new();
        if keys.iter().any(|k| !seen.insert(k)) {
            return Err(overlap());
        }
        Ok(keys)
    };

    let groups = key_groups(wc, read_pk, read_sk);
    let mut branches = Vec::with_capacity(groups.len());
    for group in &groups {
        let mut partitions: Option<Vec<String>> = None;
        let mut narrow = |keys: Vec<String>| {
            partitions = Some(match partitions.take() {
                None => keys,
                Some(held) => {
                    let wanted: std::collections::HashSet<&str> =
                        keys.iter().map(String::as_str).collect();
                    held.into_iter()
                        .filter(|k| wanted.contains(k.as_str()))
                        .collect()
                }
            });
        };
        // Each sort key condition as the atoms it stands for.
        let mut conditions: Vec<Vec<SortKeyAtom>> = Vec::new();
        let mut seen_conditions = std::collections::HashSet::new();
        let mut add = |atoms: Vec<SortKeyAtom>| {
            if seen_conditions.insert(encode_atoms(&atoms)) {
                conditions.push(atoms);
            }
        };
        for cond in group {
            match &**cond {
                WhereCondition::Comparison(c)
                    if names_attribute(&c.path, read_pk) && c.op == CompOp::Eq =>
                {
                    narrow(key(&c.value).into_iter().collect());
                }
                WhereCondition::In(path, values) if names_attribute(path, read_pk) => {
                    narrow(keys_of(values)?)
                }
                WhereCondition::Comparison(c)
                    if read_sk.is_some_and(|sk| names_attribute(&c.path, sk)) =>
                {
                    if let Some(v) = key(&c.value) {
                        match c.op {
                            CompOp::Eq => add(vec![SortKeyAtom::Points(vec![v])]),
                            CompOp::Lt => add(vec![SortKeyAtom::Below(v, false)]),
                            CompOp::Le => add(vec![SortKeyAtom::Below(v, true)]),
                            CompOp::Gt => add(vec![SortKeyAtom::Above(v, false)]),
                            CompOp::Ge => add(vec![SortKeyAtom::Above(v, true)]),
                            // Like a negated sort key condition, below.
                            CompOp::Ne => return Ok(()),
                        }
                    }
                }
                WhereCondition::Between(path, lo, hi)
                    if read_sk.is_some_and(|sk| names_attribute(path, sk)) =>
                {
                    if let (Some(lo), Some(hi)) = (key(lo), key(hi)) {
                        add(vec![
                            SortKeyAtom::Above(lo, true),
                            SortKeyAtom::Below(hi, true),
                        ]);
                    }
                }
                WhereCondition::In(path, values)
                    if read_sk.is_some_and(|sk| names_attribute(path, sk)) =>
                {
                    add(vec![SortKeyAtom::Points(keys_of(values)?)]);
                }
                WhereCondition::BeginsWith(path, prefix)
                    if read_sk.is_some_and(|sk| names_attribute(path, sk)) =>
                {
                    if let Some(prefix) = key(prefix) {
                        add(vec![SortKeyAtom::Prefix(prefix)]);
                    }
                }
                // Captured eu-west-2 and us-east-1, 2026-10-09: `pk = 'a' AND
                // NOT sk BETWEEN 1 AND 2 OR pk = 'a' AND sk = 3` runs.
                WhereCondition::NotBetween(path, _, _)
                    if read_sk.is_some_and(|sk| names_attribute(path, sk)) =>
                {
                    return Ok(());
                }
                // Captured eu-west-2 and us-east-1, 2026-10-09: a negated or
                // `<>` sort key condition in any branch stops the check, as
                // `NOT ... BETWEEN` does: `pk = 'a' AND NOT sk = '1' OR pk =
                // 'a' AND sk = '2'` and `pk = 'a' OR pk = 'a' AND NOT sk =
                // '1'` both run.
                WhereCondition::NotComparison(c)
                    if read_sk.is_some_and(|sk| names_attribute(&c.path, sk)) =>
                {
                    return Ok(());
                }
                WhereCondition::NotBeginsWith(path, _)
                    if read_sk.is_some_and(|sk| names_attribute(path, sk)) =>
                {
                    return Ok(());
                }
                _ => {}
            }
        }
        if conditions.len() > 1 {
            return Ok(());
        }
        let atoms = conditions.into_iter().next().unwrap_or_default();
        // A branch that doesn't pin the partition key is a scan, and so is the
        // read as a whole.
        let Some(partitions) = partitions else {
            return Ok(());
        };
        branches.push((partitions, atoms));
    }

    for (i, (partitions, atoms)) in branches.iter().enumerate() {
        for (other_partitions, other_atoms) in &branches[i + 1..] {
            let shared = partitions.iter().any(|p| other_partitions.contains(p));
            let both: Vec<&SortKeyAtom> = atoms.iter().chain(other_atoms).collect();
            if shared && sort_keys_meet(&both) {
                return Err(overlap());
            }
        }
    }
    Ok(())
}

/// The OR-groups a read's key ranges come from: the clause flattened as
/// `groups` is, except that an OR with no condition on a key attribute under
/// it stays whole, as a filter, rather than splitting the read. `pk = 'a' AND
/// (x = 1 OR x = 2)` is one range, and runs; `pk = 'a' AND (sk = 1 OR x = 2)`
/// is two that meet, and is refused (captured eu-west-2 and us-east-1,
/// 2026-10-09).
fn key_groups(
    wc: &WhereClause,
    read_pk: &str,
    read_sk: Option<&str>,
) -> Vec<Vec<Arc<WhereCondition>>> {
    use crate::partiql::parser::ConditionTree;
    let is_key = |cond: &WhereCondition| {
        condition_path(cond).is_some_and(|path| {
            let root = root_attribute(path);
            root == read_pk || read_sk == Some(&*root)
        })
    };
    fn mentions_key(tree: &ConditionTree, is_key: &dyn Fn(&WhereCondition) -> bool) -> bool {
        match tree {
            ConditionTree::Leaf(cond) => is_key(cond),
            ConditionTree::And(terms) | ConditionTree::Or(terms) => {
                terms.iter().any(|t| mentions_key(t, is_key))
            }
        }
    }
    fn flatten(
        tree: &ConditionTree,
        is_key: &dyn Fn(&WhereCondition) -> bool,
    ) -> Vec<Vec<Arc<WhereCondition>>> {
        match tree {
            ConditionTree::Leaf(cond) => vec![vec![cond.clone()]],
            // A filter: it narrows no key range, so it contributes nothing.
            ConditionTree::Or(terms) if !terms.iter().any(|t| mentions_key(t, is_key)) => {
                vec![Vec::new()]
            }
            ConditionTree::Or(terms) => terms.iter().flat_map(|t| flatten(t, is_key)).collect(),
            ConditionTree::And(terms) => {
                let mut out = vec![Vec::new()];
                for term in terms {
                    let term_groups = flatten(term, is_key);
                    out = out
                        .iter()
                        .flat_map(|held: &Vec<Arc<WhereCondition>>| {
                            term_groups.iter().map(move |group| {
                                let mut joined = held.clone();
                                joined.extend(group.iter().cloned());
                                joined
                            })
                        })
                        .collect();
                }
                out
            }
        }
    }
    flatten(&wc.tree, &is_key)
}

/// The atoms of one sort key condition as bytes, equal exactly when the atoms
/// are, for finding a repeated condition through a hash set.
fn encode_atoms(atoms: &[SortKeyAtom]) -> Vec<u8> {
    fn text(tag: u8, s: &str, key: &mut Vec<u8>) {
        key.push(tag);
        key.extend_from_slice(&(s.len() as u64).to_le_bytes());
        key.extend_from_slice(s.as_bytes());
    }
    let mut key = Vec::new();
    for atom in atoms {
        match atom {
            SortKeyAtom::Points(values) => {
                key.push(b'P');
                key.extend_from_slice(&(values.len() as u64).to_le_bytes());
                for v in values {
                    text(b'v', v, &mut key);
                }
            }
            SortKeyAtom::Above(v, inclusive) => {
                text(b'A', v, &mut key);
                key.push(u8::from(*inclusive));
            }
            SortKeyAtom::Below(v, inclusive) => {
                text(b'W', v, &mut key);
                key.push(u8::from(*inclusive));
            }
            SortKeyAtom::Prefix(v) => text(b'X', v, &mut key),
        }
    }
    key
}

/// Whether some sort key satisfies every atom at once.
fn sort_keys_meet(atoms: &[&SortKeyAtom]) -> bool {
    let mut low: Option<(&str, bool)> = None;
    let mut high: Option<(&str, bool)> = None;
    let mut points: Option<Vec<&str>> = None;
    let mut prefix: Option<&str> = None;
    for atom in atoms {
        match atom {
            SortKeyAtom::Above(v, inclusive) => {
                low = Some(match low {
                    Some((held, held_inclusive)) if held > v.as_str() => (held, held_inclusive),
                    Some((held, held_inclusive)) if held == v.as_str() => {
                        (held, held_inclusive && *inclusive)
                    }
                    _ => (v.as_str(), *inclusive),
                });
            }
            SortKeyAtom::Below(v, inclusive) => {
                high = Some(match high {
                    Some((held, held_inclusive)) if held < v.as_str() => (held, held_inclusive),
                    Some((held, held_inclusive)) if held == v.as_str() => {
                        (held, held_inclusive && *inclusive)
                    }
                    _ => (v.as_str(), *inclusive),
                });
            }
            SortKeyAtom::Points(values) => {
                points = Some(match points {
                    None => values.iter().map(String::as_str).collect(),
                    Some(held) => {
                        let wanted: std::collections::HashSet<&str> =
                            values.iter().map(String::as_str).collect();
                        held.into_iter().filter(|p| wanted.contains(p)).collect()
                    }
                });
            }
            SortKeyAtom::Prefix(p) => {
                prefix = match prefix {
                    None => Some(p.as_str()),
                    Some(held) if p.starts_with(held) => Some(p.as_str()),
                    Some(held) if held.starts_with(p.as_str()) => Some(held),
                    Some(_) => return false,
                };
            }
        }
    }
    let below_high = |v: &str| high.is_none_or(|(h, inclusive)| v < h || (inclusive && v == h));
    let within = |v: &str| {
        low.is_none_or(|(l, inclusive)| v > l || (inclusive && v == l))
            && below_high(v)
            && prefix.is_none_or(|p| v.starts_with(p))
    };
    if let Some(points) = points {
        return points.into_iter().any(within);
    }
    match prefix {
        // The smallest key that carries the prefix and clears the lower bound,
        // then whether the upper bound lets it through.
        Some(p) => {
            let smallest = match low {
                None => p.to_string(),
                Some((l, inclusive)) if l.starts_with(p) => {
                    if inclusive {
                        l.to_string()
                    } else {
                        format!("{l}\0")
                    }
                }
                Some((l, _)) if l < p => p.to_string(),
                Some(_) => return false,
            };
            below_high(&smallest)
        }
        None => match (low, high) {
            (Some((l, l_inclusive)), Some((h, h_inclusive))) => {
                l < h || (l == h && l_inclusive && h_inclusive)
            }
            _ => true,
        },
    }
}

/// How a SELECT with an `ORDER BY` reads: which partitions, in what order, and
/// which way through each.
struct OrderPlan {
    /// The partition keys to read, as stored, in the order to read them.
    partitions: Vec<String>,
    /// Whether each partition is read in ascending sort-key order.
    forward: bool,
}

/// Check a SELECT's `ORDER BY` the way DynamoDB does, and work out the read it
/// asks for (captured eu-west-2 and us-east-1, 2026-10-09).
///
/// DynamoDB serves `ORDER BY` as one Query per partition, so the clause can
/// only name the read's key, and the WHERE clause has to pin the partition
/// key with `=` or `IN` in every OR branch. The checks run in this order, each
/// with its own message: a WHERE clause at all; each term in turn, first that
/// it is a plain name and then that it is a key attribute; the partition key
/// pinned in every branch, once; and, when more than one partition is named,
/// the partition key among the terms.
///
/// The partitions are read in partition-key order, descending if that term
/// says so, and each one by the sort key term's direction. With no sort key
/// term each partition is read descending, whatever direction the partition
/// key term gives: `ORDER BY pk ASC` returns 3, 2, 1, where `ORDER BY pk, sk`
/// returns 1, 2, 3.
fn check_order_terms(
    order_by: &[OrderByTerm],
    where_clause: Option<&WhereClause>,
    read_pk: &str,
    read_sk: Option<&str>,
) -> Result<()> {
    let reject = |message: String| DynoxideError::ValidationException(message);
    if where_clause.is_none() {
        return Err(reject(
            "Must have WHERE clause in the statement when using ORDER BY clause.".to_string(),
        ));
    }
    for term in order_by {
        if let Some(rejection) = &term.rejection {
            return Err(reject(rejection.clone()));
        }
        if !names_attribute(&term.path, read_pk)
            && !read_sk.is_some_and(|sk| names_attribute(&term.path, sk))
        {
            return Err(reject(format!(
                "Attribute {} in ORDER BY clause must be part of the primary key",
                term.path
            )));
        }
    }
    Ok(())
}

/// The rest of [`check_order_terms`]'s checks, once the key values and any
/// overlap have been checked, and the read the clause asks for.
fn plan_order(
    order_by: &[OrderByTerm],
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    read_pk: &str,
    read_sk: Option<&str>,
) -> Result<OrderPlan> {
    let reject = |message: String| DynoxideError::ValidationException(message);
    let Some(wc) = where_clause else {
        return Err(reject(
            "Must have WHERE clause in the statement when using ORDER BY clause.".to_string(),
        ));
    };
    let mut partition_values = Vec::new();
    for group in &wc.groups {
        let pinned: Vec<Vec<&PartiqlValue>> = group
            .iter()
            .filter_map(|cond| match &**cond {
                WhereCondition::Comparison(c)
                    if names_attribute(&c.path, read_pk) && c.op == CompOp::Eq =>
                {
                    Some(vec![&c.value])
                }
                WhereCondition::In(path, values) if names_attribute(path, read_pk) => {
                    Some(values.iter().collect())
                }
                _ => None,
            })
            .collect();
        let [values] = pinned.as_slice() else {
            return Err(reject(
                "Must have at least one non-optional hash key condition in WHERE clause \
                 when using ORDER BY clause."
                    .to_string(),
            ));
        };
        partition_values.extend(values.iter().copied());
    }
    let pk_term = order_by
        .iter()
        .find(|term| names_attribute(&term.path, read_pk));
    if partition_values.len() > 1 && pk_term.is_none() {
        return Err(reject(
            "Must have hash key in ORDER BY clause when more than one hash key condition \
             specified in WHERE clause."
                .to_string(),
        ));
    }

    // A stored partition key sorts the way its type orders, so sorting the
    // stored form is sorting the keys.
    let mut partitions = Vec::with_capacity(partition_values.len());
    for value in partition_values {
        partitions.push(key_string(&resolve_value(value, parameters)?, read_pk)?);
    }
    partitions.sort();
    partitions.dedup();
    if pk_term.is_some_and(|term| term.descending) {
        partitions.reverse();
    }

    let forward = order_by
        .iter()
        .find(|term| read_sk.is_some_and(|sk| names_attribute(&term.path, sk)))
        .is_some_and(|term| !term.descending);
    Ok(OrderPlan {
        partitions,
        forward,
    })
}

/// Read the partitions an ordered SELECT names, one Query each, in the plan's
/// order, starting after `cursor` and stopping once `limit` rows are read.
///
/// A continuation resumes in the partition it stopped in and carries on
/// through the rest, so the pages together hold every row once, in order.
#[allow(clippy::too_many_arguments)]
async fn read_in_order<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    index: Option<&ResolvedIndex>,
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    read_sk: Option<&str>,
    cursor: Option<&Cursor>,
    limit: Option<usize>,
    plan: &OrderPlan,
) -> Result<Vec<(String, String, String)>> {
    // Sort-key conditions are pushed into each Query only when the clause is a
    // single AND group, so they hold in every partition the read visits.
    let sk_conditions = match (read_sk, where_clause) {
        (Some(sk_name), Some(wc)) if wc.groups.len() == 1 => {
            translate_sk_conditions(&wc.groups[0], sk_name, parameters)
        }
        _ => None,
    }
    .unwrap_or_default();
    let (sk_condition_sql, sk_param_values) = sk_conditions_to_sql(&sk_conditions);
    let sk_params_refs: Vec<&str> = sk_param_values.iter().map(|s| s.as_str()).collect();

    let first = match cursor {
        Some(c) => plan
            .partitions
            .iter()
            .position(|pk| *pk == c.pk)
            .ok_or_else(|| DynoxideError::ValidationException("Invalid NextToken".to_string()))?,
        None => 0,
    };

    let mut rows = Vec::new();
    for (i, pk_str) in plan.partitions.iter().enumerate().skip(first) {
        let remaining = limit.map(|lim| lim.saturating_sub(rows.len()));
        if remaining == Some(0) {
            break;
        }
        let resume = cursor.filter(|_| i == first);
        let params = crate::storage::QueryParams {
            sk_condition: sk_condition_sql.as_deref(),
            sk_params: &sk_params_refs,
            forward: plan.forward,
            limit: remaining,
            exclusive_start_sk: resume.map(|c| c.sk.as_str()),
            exclusive_start_base_pk: resume.and_then(|c| c.base_pk.as_deref()),
            exclusive_start_base_sk: resume.and_then(|c| c.base_sk.as_deref()),
        };
        let page = match index {
            Some(idx) if idx.is_lsi => {
                storage
                    .query_lsi_items(table_name, idx.name(), pk_str, &params)
                    .await?
            }
            Some(idx) => {
                storage
                    .query_gsi_items(table_name, idx.name(), pk_str, &params)
                    .await?
            }
            None => storage.query_items(table_name, pk_str, &params).await?,
        };
        rows.extend(page);
    }
    Ok(rows)
}

/// Refuse an item nested exactly as deep as DynamoDB allows a value to reach,
/// with the short message PartiQL gives it. A literal that deep is refused as
/// the statement is read, so this is a parameter's. One level deeper gets the
/// usual long message from the item checks (captured eu-west-2 and us-east-1,
/// 2026-10-09: a parameter 32 levels deep is `Nesting Levels have exceeded
/// supported limits`, and 33 or more the long form, on INSERT and UPDATE).
fn reject_item_nested_to_the_limit(item: &Item) -> Result<()> {
    if item_nested_to_the_limit(item) {
        return Err(nested_to_the_limit());
    }
    Ok(())
}

/// Whether an item reaches exactly 32 levels, the depth PartiQL refuses with
/// its short message.
fn item_nested_to_the_limit(item: &Item) -> bool {
    fn deepest(value: &AttributeValue) -> usize {
        let children: Box<dyn Iterator<Item = &AttributeValue>> = match value {
            AttributeValue::M(map) => Box::new(map.values()),
            AttributeValue::L(items) => Box::new(items.iter()),
            _ => return 0,
        };
        children.map(|child| 1 + deepest(child)).max().unwrap_or(0)
    }
    item.values().map(deepest).max() == Some(32)
}

fn nested_to_the_limit() -> DynoxideError {
    DynoxideError::ValidationException("Nesting Levels have exceeded supported limits".to_string())
}

/// A BETWEEN bound as DynamoDB writes it in a message: `S:x`, `N:3`, `B:[MQ==]`.
fn bound_operand(value: &AttributeValue) -> String {
    match value {
        AttributeValue::S(s) => format!("S:{s}"),
        AttributeValue::N(n) => format!("N:{n}"),
        AttributeValue::B(b) => {
            use base64::Engine;
            format!(
                "B:[{}]",
                base64::engine::general_purpose::STANDARD.encode(b)
            )
        }
        other => other.type_name().to_string(),
    }
}

/// Refuse a type given to `attribute_type` as a parameter that isn't the name
/// of a DynamoDB type.
fn check_attribute_type_parameter(
    kind: &PartiqlValue,
    parameters: &[AttributeValue],
    reject: fn(&str, &AttributeValue) -> DynoxideError,
) -> Result<()> {
    match resolve_value(kind, parameters)? {
        AttributeValue::S(name) => match crate::partiql::parser::attribute_type_name_refusal(&name)
        {
            Some(message) => Err(DynoxideError::ValidationException(message)),
            None => Ok(()),
        },
        other => Err(reject("attribute_type", &other)),
    }
}

/// Reject an operand of a type its operator does not take.
///
/// DynamoDB orders `S`, `N` and `B` and nothing else, and it rejects the
/// statement outright rather than declining to match: the check fires before the
/// table is resolved, so a statement naming a table that does not exist still
/// reports this. `=` and `<>` are unaffected, being defined for every type.
/// Captured eu-west-2 2026-08-15.
///
/// `begins_with` takes a string or binary, on any attribute, negated or not.
/// An AND chain is checked in the order written, so its first offending operand
/// is the one reported, as DynamoDB reports it. Captured eu-west-2, 2026-10-08.
fn validate_operand_types(
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
) -> Result<()> {
    fn orderable(v: &AttributeValue) -> bool {
        matches!(
            v,
            AttributeValue::S(_) | AttributeValue::N(_) | AttributeValue::B(_)
        )
    }
    fn reject(op: &str, v: &AttributeValue) -> DynoxideError {
        DynoxideError::ValidationException(format!(
            "Incorrect operand type for operator or function; \
             operator or function: {op}, operand type: {}",
            v.type_name()
        ))
    }

    fn ordering_op(op: &CompOp) -> Option<&'static str> {
        match op {
            CompOp::Lt => Some("<"),
            CompOp::Le => Some("<="),
            CompOp::Gt => Some(">"),
            CompOp::Ge => Some(">="),
            CompOp::Eq | CompOp::Ne => None,
        }
    }
    // Bounds of one type, the lower first (captured eu-west-2 and us-east-1,
    // 2026-10-09 for a path, and 2026-10-10 for `size()`).
    fn check_bounds(low: &AttributeValue, high: &AttributeValue) -> Result<()> {
        for value in [low, high] {
            if !orderable(value) {
                return Err(reject("BETWEEN", value));
            }
        }
        let bounds = |rule: &str| {
            DynoxideError::ValidationException(format!(
                "The BETWEEN operator requires {rule}; lower bound operand: \
                 AttributeValue: {{{}}}, upper bound operand: AttributeValue: {{{}}}",
                bound_operand(low),
                bound_operand(high)
            ))
        };
        if std::mem::discriminant(low) != std::mem::discriminant(high) {
            return Err(bounds("same data type for lower and upper bounds"));
        }
        if compare_values(low, &CompOp::Gt, high) {
            return Err(bounds(
                "upper bound to be greater than or equal to lower bound",
            ));
        }
        Ok(())
    }
    fn check(condition: &WhereCondition, parameters: &[AttributeValue]) -> Result<()> {
        match condition {
            // A negated comparison is checked too: `NOT a < ?` reads the
            // same operand and DynamoDB rejects it on the same terms.
            WhereCondition::Comparison(c) | WhereCondition::NotComparison(c) => {
                let Some(op) = ordering_op(&c.op) else {
                    return Ok(());
                };
                let value = resolve_value(&c.value, parameters)?;
                if !orderable(&value) {
                    return Err(reject(op, &value));
                }
            }
            WhereCondition::Between(_, low, high) | WhereCondition::NotBetween(_, low, high) => {
                check_bounds(
                    &resolve_value(low, parameters)?,
                    &resolve_value(high, parameters)?,
                )?;
            }
            // A comparison between operands that aren't a path and a value,
            // `size(s) < true` among them: each side that is a value takes the
            // check the value of a `path op value` comparison does (captured
            // eu-west-2 and us-east-1, 2026-10-10, for `size()`). A size is a
            // number, so a string or boolean against it under `=` or `<>` is
            // simply unequal rather than refused.
            WhereCondition::Compare(left, op, right) => {
                // A condition function compared is checked as it would be on
                // its own: `attribute_type(a, ?) = b` checks the type.
                for side in [left, right] {
                    if let Operand::Condition(inner) = side {
                        check(inner, parameters)?;
                    }
                }
                let Some(op) = ordering_op(op) else {
                    return Ok(());
                };
                for side in [left, right] {
                    if let Operand::Value(value) = side {
                        let value = resolve_value(value, parameters)?;
                        if !orderable(&value) {
                            return Err(reject(op, &value));
                        }
                    }
                }
            }
            // BETWEEN on an operand that isn't a path checks its bounds as
            // BETWEEN on a path does, when both are values (captured for
            // `size()`, 2026-10-10). A bound that isn't a value, such as a path,
            // can't be checked until an item is read; a value beside it is still
            // held to the orderable types (inferred, not captured).
            WhereCondition::CompareBetween(_, low, high) => match (low, high) {
                (Operand::Value(low), Operand::Value(high)) => check_bounds(
                    &resolve_value(low, parameters)?,
                    &resolve_value(high, parameters)?,
                )?,
                _ => {
                    for bound in [low, high] {
                        if let Operand::Value(value) = bound {
                            let value = resolve_value(value, parameters)?;
                            if !orderable(&value) {
                                return Err(reject("BETWEEN", &value));
                            }
                        }
                    }
                }
            },
            WhereCondition::CompareIn(operand, members) => {
                for side in std::iter::once(operand).chain(members) {
                    if let Operand::Condition(inner) = side {
                        check(inner, parameters)?;
                    }
                }
            }
            // A type given to `attribute_type` as a parameter is checked
            // once it is bound, negated or not (captured eu-west-2 and
            // us-east-1, 2026-10-09).
            WhereCondition::AttributeType(_, kind @ PartiqlValue::Parameter(_)) => {
                check_attribute_type_parameter(kind, parameters, reject)?;
            }
            // A condition with no negated form of its own is checked as it
            // would be without the NOT. Inferred, not captured: no capture
            // holds a refused negated size or BETWEEN.
            WhereCondition::Not(inner) => check(inner, parameters)?,
            // `begins_with` takes a string or binary operand (captured
            // eu-west-2 and us-east-1, 2026-10-09).
            WhereCondition::BeginsWith(_, prefix) | WhereCondition::NotBeginsWith(_, prefix) => {
                let value = resolve_value(prefix, parameters)?;
                if !matches!(value, AttributeValue::S(_) | AttributeValue::B(_)) {
                    return Err(reject("begins_with", &value));
                }
            }
            _ => {}
        }
        Ok(())
    }

    let Some(wc) = where_clause else {
        return Ok(());
    };
    for group in &wc.groups {
        for condition in group {
            check(condition, parameters)?;
        }
    }
    Ok(())
}

/// Run one page of a parsed statement, `written` being its text as the
/// request carried it. A continuation token is bound to that text exactly as
/// written, as DynamoDB binds it.
///
/// `next_token` continues a previous page; the returned token, when present,
/// continues this one. A statement without a `limit` returns every matching
/// row and no token.
///
/// `old_on_condition_failure` is `ReturnValuesOnConditionCheckFailure` set to
/// `ALL_OLD`: an UPDATE or DELETE whose WHERE clause is false on an item that
/// exists then fails carrying that item, as DynamoDB's does on all three
/// PartiQL operations. A missing item never carries one (captured eu-west-2
/// and us-east-1, 2026-10-10).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_written_page<S: StorageBackend>(
    storage: &S,
    written: &str,
    stmt: &Statement,
    parameters: &[AttributeValue],
    limit: Option<usize>,
    next_token: Option<&str>,
    consistent_read: bool,
    capacity_mode: Option<&str>,
    resolved: Option<&ResolvedTable>,
    old_on_condition_failure: bool,
) -> Result<StatementPage> {
    if next_token.is_some() && !matches!(stmt, Statement::Select { .. }) {
        return Err(DynoxideError::ValidationException(
            "NextToken is only valid on a SELECT statement".to_string(),
        ));
    }
    // A read of an index whose key has more than one attribute on either side
    // is refused before anything else about the read is checked, its operands
    // included. The table is loaded for it here and passed on, so the read
    // resolves it once; a table that cannot be loaded is left to the checks
    // that report it, in their usual order (captured eu-west-2 and us-east-1,
    // 2026-10-09).
    let loaded;
    let resolved = match (stmt, resolved) {
        (
            Statement::Select {
                table_name,
                index_name: Some(_),
                ..
            },
            None,
        ) => {
            loaded = ResolvedTable::load(storage, table_name).await.ok();
            loaded.as_ref()
        }
        (_, resolved) => resolved,
    };
    if let (
        Statement::Select {
            table_name,
            index_name: Some(index_name),
            ..
        },
        Some(table),
    ) = (stmt, resolved)
        && table.table_name == *table_name
    {
        reject_multi_attribute_index(table, index_name)?;
    }

    validate_operand_types(statement_where_clause(stmt), parameters)?;

    // DynamoDB rejects an index qualifier on a write statement before it
    // resolves the table, so a qualified UPDATE against a table that does not
    // exist reports the index problem rather than the missing table. Captured
    // eu-west-2 2026-08-15.
    if !matches!(stmt, Statement::Select { .. })
        && crate::partiql::parser::index_name(stmt).is_some()
    {
        return Err(DynoxideError::ValidationException(
            "This operation is not supported on an index".to_string(),
        ));
    }

    // A resolution is an optimisation the caller passes in, and taking it on
    // trust means applying whatever key schema it holds. A caller that handed
    // back the wrong table's would have this statement read and write by another
    // table's keys, silently. Dropping a mismatch costs only the resolution this
    // was meant to save: the executor loads the table itself instead, which is
    // slower and right. Every caller in the tree matches, so nothing pays today.
    let resolved = resolved.filter(|r| {
        crate::partiql::parser::table_name(stmt).is_some_and(|name| r.table_name == name)
    });

    match stmt {
        Statement::Select {
            table_name,
            index_name,
            projections,
            where_clause,
            order_by,
            ..
        } => {
            let outcome = execute_select(
                storage,
                written,
                table_name,
                index_name.as_deref(),
                projections,
                where_clause.as_ref(),
                order_by,
                parameters,
                limit,
                next_token,
                consistent_read,
                resolved,
            )
            .await?;
            Ok(StatementPage {
                items: Some(outcome.items),
                size: outcome.evaluated_bytes,
                capacity: None,
                next_token: outcome.next_token,
                read_index: outcome.index.map(|i| ReadIndex {
                    name: i.def.index_name,
                    is_lsi: i.is_lsi,
                }),
                base_read_units: outcome.base_read_units,
            })
        }
        Statement::Insert {
            table_name,
            item,
            if_not_exists,
        } => {
            let capacity = execute_insert(
                storage,
                table_name,
                item,
                parameters,
                *if_not_exists,
                capacity_mode,
                resolved,
            )
            .await?;
            Ok(StatementPage {
                items: None,
                // An insert is measured on the item it wrote, which is 0 when an
                // `if_not_exists` duplicate made it a no-op.
                size: capacity.new_size.unwrap_or(0),
                capacity: Some(capacity),
                ..Default::default()
            })
        }
        Statement::Update {
            table_name,
            set_clauses,
            remove_paths,
            where_clause,
            returning,
            ..
        } => {
            let (projection, capacity) = execute_update(
                storage,
                table_name,
                set_clauses,
                remove_paths,
                where_clause.as_ref(),
                parameters,
                *returning,
                capacity_mode,
                resolved,
                old_on_condition_failure,
            )
            .await?;
            // RETURNING surfaces the requested projection of the updated item;
            // without a clause an UPDATE returns no items. An empty MODIFIED
            // projection surfaces as a present but empty Items array (no row),
            // matching DynamoDB, rather than a row holding an empty object.
            let items = projection.map(|item| {
                if item.is_empty() {
                    Vec::new()
                } else {
                    vec![item]
                }
            });
            Ok(StatementPage {
                items,
                // An update is measured on the item it left behind.
                size: capacity.new_size.unwrap_or(0),
                capacity: Some(capacity),
                ..Default::default()
            })
        }
        Statement::Delete {
            table_name,
            where_clause,
            returning,
            ..
        } => {
            // DynamoDB permits only RETURNING ALL OLD * on DELETE; the other
            // well-formed variants are rejected with a ValidationException whose
            // message echoes the offending variant.
            if let Some(variant) = returning
                && *variant != ReturningVariant::AllOld
            {
                return Err(DynoxideError::ValidationException(format!(
                    "Invalid returning clause: RETURNING {} *. Only RETURNING ALL OLD * is allowed in DELETE statements.",
                    variant.as_sql()
                )));
            }
            let (old_item, capacity) = execute_delete(
                storage,
                table_name,
                where_clause.as_ref(),
                parameters,
                capacity_mode,
                resolved,
                old_on_condition_failure,
            )
            .await?;
            // RETURNING ALL OLD * always surfaces an Items array: the deleted
            // item on a hit, an empty array on a miss (a no-op success). This
            // differs from the classic DeleteItem ReturnValues path, which omits
            // Attributes on a miss.
            let items = if returning.is_some() {
                Some(old_item.map(|item| vec![item]).unwrap_or_default())
            } else {
                None
            };
            Ok(StatementPage {
                items,
                // A delete is measured on the item it removed, which is 0 when
                // the target was missing.
                size: capacity.old_size.unwrap_or(0),
                capacity: Some(capacity),
                ..Default::default()
            })
        }
    }
}

/// Insert a projected value into a result item.
///
/// For dotted paths (e.g. `a.b.c`), DynamoDB PartiQL returns the resolved value
/// keyed by the leaf segment name (`c`), not the full path or reconstructed
/// nested structure. For simple paths and array index paths, the key is used as-is.
fn insert_nested_projection(result: &mut Item, path: &str, val: AttributeValue) {
    result.insert(projection_key(path), val);
}

/// The key a projected path is returned under: its last name, with any list
/// indexes after it. A quoted name is the name it quotes, so `"a.b"` comes
/// back as `a.b` (captured eu-west-2 and us-east-1, 2026-10-10).
fn projection_key(path: &str) -> String {
    if !path.contains('"') {
        // Use the leaf segment as the key
        return path.rsplit('.').next().unwrap_or(path).to_string();
    }
    let Some(segments) = split_path_segments(path) else {
        return path.to_string();
    };
    let last_name = segments
        .iter()
        .rposition(|segment| matches!(segment, PathSegment::Key(_)))
        .unwrap_or(0);
    segments[last_name..]
        .iter()
        .map(|segment| match segment {
            PathSegment::Key(name) => name.to_string(),
            PathSegment::Index(index) => format!("[{index}]"),
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn execute_select<S: StorageBackend>(
    storage: &S,
    written: &str,
    table_name: &str,
    index_name: Option<&str>,
    projections: &[String],
    where_clause: Option<&WhereClause>,
    order_by: &[OrderByTerm],
    parameters: &[AttributeValue],
    limit: Option<usize>,
    next_token: Option<&str>,
    consistent_read: bool,
    resolved: Option<&ResolvedTable>,
) -> Result<SelectOutcome> {
    // The table is resolved before the index: a qualified SELECT against a
    // table that does not exist reports the missing table, not the missing
    // index. Captured eu-west-2 2026-08-15.
    // Resolved by the caller when a preparation pass already did it, which is
    // what stops a batch reading the same table's metadata and parsing the same
    // key schema once per statement on top of once per batch member.
    let owned;
    let resolved = match resolved {
        Some(r) => r,
        None => {
            owned = ResolvedTable::load(storage, table_name).await?;
            &owned
        }
    };
    let meta = &resolved.meta;
    let table_key_schema = &resolved.key_schema;
    let vector_defs = crate::actions::vector_index::parse_vector_defs(meta)?;

    let index = index_name
        .map(|name| resolve_index(meta, name))
        .transpose()?;

    // The keys the row walk is paced by: the index's when one is named, the
    // table's otherwise. An LSI's partition key is the table's, so only the
    // sort key moves in that case.
    let (read_pk, read_sk) = match index.as_ref() {
        Some(idx) => (idx.pk_attr(), idx.sk_attr()),
        None => (
            table_key_schema.partition_key.as_str(),
            table_key_schema.sort_key.as_deref(),
        ),
    };

    // ORDER BY is checked once the table and any index are known, since what
    // it may name is their key; a missing table or index is reported first.
    // Its WHERE and term checks come before the key values are checked, which
    // come before overlapping conditions, which come before the rest of the
    // ORDER BY checks (captured eu-west-2 and us-east-1, 2026-10-09).
    if !order_by.is_empty() {
        check_order_terms(order_by, where_clause, read_pk, read_sk)?;
    }
    let declared = |attr: &str| match index.as_ref() {
        None if attr == table_key_schema.partition_key => {
            Some(table_key_schema.partition_key_type.clone())
        }
        None => table_key_schema.sort_key_type.clone(),
        Some(_) => declared_attribute_type(meta, attr),
    };
    check_read_key_types(where_clause, parameters, read_pk, read_sk, &declared)?;
    reject_overlapping_key_conditions(where_clause, parameters, read_pk, read_sk)?;
    let order = if order_by.is_empty() {
        None
    } else {
        Some(plan_order(
            order_by,
            where_clause,
            parameters,
            read_pk,
            read_sk,
        )?)
    };

    // A strongly consistent read cannot be served from a GSI. PartiQL words
    // this differently from Query, which says "Consistent reads are not
    // supported on global secondary indexes"; both wordings were captured on
    // the same day, so neither is a stale copy of the other.
    if consistent_read && index.as_ref().is_some_and(|i| !i.is_lsi) {
        return Err(DynoxideError::ValidationException(
            "Strongly consistent read is not supported on Global Secondary Indexes".to_string(),
        ));
    }

    // What an index does not carry, a statement may not always name. Two
    // separate rules, and neither splits the way it first appears to:
    //
    //   - A projection naming an unprojected attribute is rejected on a GSI and
    //     accepted on an LSI, which serves it by reading the base table. That
    //     reach-back is not implemented here, so such a projection comes back
    //     empty rather than rejected.
    //   - A filter on an unprojected attribute is rejected on either kind, but
    //     only when the read is keyed on the index partition key. An unkeyed
    //     read is a scan, and a scan simply matches nothing.
    //
    // The filter rule looked like a GSI/LSI split on the first two cases
    // measured. It is not: an unkeyed LSI filter is accepted and a keyed GSI
    // filter is rejected, and a two-condition unkeyed filter is accepted, which
    // rules out the condition count as well. Captured eu-west-2 2026-08-15.
    if let Some(idx) = index.as_ref() {
        if !idx.is_lsi {
            let missing: Vec<String> = projections
                .iter()
                .map(|p| root_attribute(p).to_string())
                .filter(|attr| !idx.projects(attr, table_key_schema))
                .collect();
            if !missing.is_empty() {
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values were invalid: Global secondary index {} \
                     does not project [{}]",
                    idx.name(),
                    missing.join(", ")
                )));
            }
        }

        if keys_index(where_clause, read_pk) {
            let missing: Vec<String> = where_attributes(where_clause)
                .into_iter()
                .filter(|attr| !idx.projects(attr, table_key_schema))
                .collect();
            if !missing.is_empty() {
                // "Secondary index", with no Global or Local in front of it,
                // on both kinds.
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values were invalid: Secondary index {} \
                     does not project one or more filter attributes: [{}]",
                    idx.name(),
                    missing.join(", ")
                )));
            }
        }
    }

    let fingerprint = statement_fingerprint(written, parameters);
    let cursor = next_token
        .map(|token| decode_next_token(token, table_name, fingerprint))
        .transpose()?;
    // A statement with a list or map parameter is never resumed, even with
    // the same parameters sent again unchanged; the first page still hands
    // out a token (captured eu-west-2 and us-east-1, 2026-10-10).
    if cursor.is_some()
        && parameters
            .iter()
            .any(|p| matches!(p, AttributeValue::L(_) | AttributeValue::M(_)))
    {
        return Err(token_mismatch());
    }

    // An index read's cursor is meaningless without the base table key: the
    // backend falls back to a two-column comparison that cannot advance past
    // rows sharing an index key, so the walk ends early and silently. The
    // fingerprint alone does not catch a token that was truncated rather than
    // minted for another statement, so the halves are checked separately.
    if index.is_some()
        && cursor
            .as_ref()
            .is_some_and(|c| c.base_pk.is_none() || c.base_sk.is_none())
    {
        return Err(DynoxideError::ValidationException(
            "Invalid NextToken".to_string(),
        ));
    }

    // An LSI shares its partition with the table, so DynamoDB serves a
    // projection naming an attribute the index does not carry by reading the
    // base item. A GSI cannot, and rejects the statement above instead. The
    // index entry already holds the base key, so the fetch needs nothing the
    // row does not carry. Captured eu-west-2 2026-08-15 (case Q27).
    let reach_back = index.as_ref().is_some_and(|idx| {
        idx.is_lsi
            && projections
                .iter()
                .any(|p| !idx.projects(&root_attribute(p), table_key_schema))
    });

    let window = evaluate_window(
        storage,
        table_name,
        index.as_ref(),
        where_clause,
        parameters,
        read_pk,
        read_sk,
        table_key_schema,
        cursor.as_ref(),
        limit,
        reach_back,
        consistent_read,
        order.as_ref(),
        &vector_defs,
    )
    .await?;

    // A continuation is owed whenever the read stopped because it hit the
    // limit, whether or not anything in the window matched. That is why a page
    // can come back short, or empty, and still carry a token.
    let token = match (limit, &window.last_evaluated) {
        (Some(lim), Some(stop)) if window.evaluated >= lim => {
            Some(encode_next_token(table_name, fingerprint, stop))
        }
        _ => None,
    };

    let base_read_units = window.base_read_units;

    // Projections run last, so one that drops the key cannot break the
    // continuation: the cursor comes from the row as it was read.
    let items = window
        .matched
        .into_iter()
        .map(|item| {
            if projections.is_empty() {
                item
            } else {
                let mut projected = HashMap::new();
                for proj in projections {
                    if let Some(val) = resolve_nested_path(&item, proj) {
                        insert_nested_projection(&mut projected, proj, val.clone());
                    }
                }
                projected
            }
        })
        .collect();

    Ok(SelectOutcome {
        items,
        evaluated_bytes: window.evaluated_bytes,
        next_token: token,
        index,
        base_read_units,
    })
}

/// What a `SELECT` produced: its rows, where to resume, and the index it was
/// served from. The index rides along because capacity is attributed to it
/// rather than to the table, and the caller has no other way to know.
struct SelectOutcome {
    items: Vec<Item>,
    /// What the read walked, which is what it is charged on. Not the weight of
    /// `items`: a filter and a projection both narrow the rows after the read
    /// has already paid for them.
    evaluated_bytes: usize,
    next_token: Option<String>,
    index: Option<ResolvedIndex>,
    /// What an LSI reach-back cost on the base table, in read units. It lands
    /// on the table arm, leaving the index arm to cover the index read alone.
    base_read_units: f64,
}

/// One read of the table: the rows that matched, plus where the read stopped.
struct Window {
    matched: Vec<Item>,
    /// Where the last row read sat, matching or not. The cursor a continuation
    /// resumes from, so no row is evaluated twice.
    last_evaluated: Option<Cursor>,
    /// How many rows were read, matching or not.
    evaluated: usize,
    /// What those rows weighed as they were read, before the WHERE clause and
    /// before any projection. This is what the read is charged on.
    evaluated_bytes: usize,
    /// What an LSI reach-back cost on the base table, in read units, summed over
    /// every row walked rather than every row kept.
    base_read_units: f64,
}

/// Read up to `limit` rows starting after `cursor`, and keep the ones the WHERE
/// clause accepts.
///
/// `limit` bounds rows *read*, not rows returned, which is what DynamoDB means
/// by `Limit` and what `Query` and `Scan` already do here. Pushing both the
/// cursor and the bound into the backend keeps a page's cost proportional to
/// the page rather than to the table.
#[allow(clippy::too_many_arguments)]
async fn evaluate_window<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    index: Option<&ResolvedIndex>,
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    read_pk: &str,
    read_sk: Option<&str>,
    table_key_schema: &crate::actions::helpers::KeySchema,
    cursor: Option<&Cursor>,
    limit: Option<usize>,
    reach_back: bool,
    consistent_read: bool,
    order: Option<&OrderPlan>,
    vector_defs: &[crate::types::VectorIndex],
) -> Result<Window> {
    // A keyed read needs an equality on the partition key. An index whose key
    // has more than one attribute never gets here: its reads are refused first
    // (see `reject_multi_attribute_index`).
    let pk_condition = where_clause.and_then(|wc| find_pk_condition(wc, read_pk));
    // A partition key value no key can have only gets this far when the clause
    // isn't read by key (see `check_read_key_types`), and it matches no row.
    let pk_key = match pk_condition {
        Some(value) => {
            let value = resolve_value(value, parameters)?;
            // A key read with a key value no key can be is refused as GetItem
            // and Query refuse it, on the table and on either kind of index:
            // the partition key first, then each value the sort key's
            // conditions name, whatever the condition (captured eu-west-2 and
            // us-east-1, 2026-10-10).
            if let Some(refusal) = crate::actions::helpers::key_size_refusal(&value, false) {
                return Err(refusal);
            }
            if let (Some(sk_name), Some(wc)) = (read_sk, where_clause) {
                let sk_conditions =
                    translate_sk_conditions(&wc.groups[0], sk_name, parameters).unwrap_or_default();
                for sk_value in sk_conditions.iter().flat_map(|c| c.values()) {
                    if let Some(refusal) = crate::actions::helpers::key_size_refusal(sk_value, true)
                    {
                        return Err(refusal);
                    }
                }
            }
            Some(value.to_key_string())
        }
        None => {
            // An OR beside the key condition leaves the key in every group the
            // clause expands to, so the statement still reads one partition
            // and an oversized key is refused as it is without the OR
            // (captured eu-west-2 and us-east-1, 2026-10-10:
            // `pk = <2,049 bytes> AND (x = 1 OR x = 2)` on a table; an index's
            // key is inferred to follow suit, as its key reads do). The read
            // itself stays a scan here.
            if let Some(value) = where_clause.and_then(|wc| pk_in_every_group(wc, read_pk)) {
                let value = resolve_value(value, parameters)?;
                if let Some(refusal) = crate::actions::helpers::key_size_refusal(&value, false) {
                    return Err(refusal);
                }
            }
            None
        }
    };

    let rows: Vec<(String, String, String)> = if let Some(plan) = order {
        read_in_order(
            storage,
            table_name,
            index,
            where_clause,
            parameters,
            read_sk,
            cursor,
            limit,
            plan,
        )
        .await?
    } else if let Some(None) = pk_key {
        Vec::new()
    } else if let Some(Some(pk_str)) = pk_key {
        // Sort-key conditions that a key condition can express are pushed into
        // the SQL, so `limit` counts rows within the key-condition range rather
        // than rows in the whole partition, which is how DynamoDB paces a
        // key-bound read. find_pk_condition only fires on a single-OR-group
        // WHERE, so the group holding the sort-key conditions is unambiguous.
        let sk_conditions = match (read_sk, where_clause) {
            (Some(sk_name), Some(wc)) => {
                translate_sk_conditions(&wc.groups[0], sk_name, parameters)
            }
            _ => None,
        }
        .unwrap_or_default();

        // The shared helper numbers placeholders after the pk (?1), and the
        // SQL builder appends the cursor's exclusive-start comparison after
        // them, so a pushed-down range and a continuation compose the same
        // way they do for Query.
        let (sk_condition_sql, sk_param_values) = sk_conditions_to_sql(&sk_conditions);
        let sk_params_refs: Vec<&str> = sk_param_values.iter().map(|s| s.as_str()).collect();

        // Ascending, so this path and the scan below agree on row order and a
        // continuation resumes the same way on either.
        let params = crate::storage::QueryParams {
            sk_condition: sk_condition_sql.as_deref(),
            sk_params: &sk_params_refs,
            forward: true,
            limit,
            exclusive_start_sk: cursor.map(|c| c.sk.as_str()),
            exclusive_start_base_pk: cursor.and_then(|c| c.base_pk.as_deref()),
            exclusive_start_base_sk: cursor.and_then(|c| c.base_sk.as_deref()),
        };
        match index {
            Some(idx) if idx.is_lsi => {
                storage
                    .query_lsi_items(table_name, idx.name(), &pk_str, &params)
                    .await?
            }
            Some(idx) => {
                storage
                    .query_gsi_items(table_name, idx.name(), &pk_str, &params)
                    .await?
            }
            None => storage.query_items(table_name, &pk_str, &params).await?,
        }
    } else {
        let params = crate::storage::ScanParams {
            limit,
            exclusive_start_pk: cursor.map(|c| c.pk.as_str()),
            exclusive_start_sk: cursor.map(|c| c.sk.as_str()),
            exclusive_start_base_pk: cursor.and_then(|c| c.base_pk.as_deref()),
            exclusive_start_base_sk: cursor.and_then(|c| c.base_sk.as_deref()),
            ..Default::default()
        };
        match index {
            Some(idx) if idx.is_lsi => {
                storage
                    .scan_lsi_items(table_name, idx.name(), &params)
                    .await?
            }
            Some(idx) => {
                storage
                    .scan_gsi_items(table_name, idx.name(), &params)
                    .await?
            }
            None => storage.scan_items(table_name, &params).await?,
        }
    };

    let evaluated = rows.len();
    // The stop position is read off the last row whether or not it matched.
    // An index read needs the base table key with it, and the index row holds
    // it: an index entry always carries the base key, whatever it projects.
    let last_evaluated = rows.last().map(|(pk, sk, json)| {
        let (base_pk, base_sk) = if index.is_some() {
            let item: Option<Item> = serde_json::from_str(json).ok();
            let base_pk = item
                .as_ref()
                .and_then(|i| i.get(&table_key_schema.partition_key))
                .and_then(|v| v.to_key_string());
            // A hash-only base table still stores the empty-string default in
            // the index row's table_sk column, so the cursor keeps its full
            // width and ties are broken by the base key rather than collapsing.
            let base_sk = match table_key_schema.sort_key.as_deref() {
                Some(name) => item
                    .as_ref()
                    .and_then(|i| i.get(name))
                    .and_then(|v| v.to_key_string()),
                None => Some(String::new()),
            };
            (base_pk, base_sk)
        } else {
            (None, None)
        };
        Cursor {
            pk: pk.clone(),
            sk: sk.clone(),
            base_pk,
            base_sk,
        }
    });
    // Sized as read, before the WHERE clause narrows it. DynamoDB charges a
    // read on the rows it walked, so a read matching one row costs what a read
    // matching every row costs, and one matching nothing costs the same again.
    // Captured eu-west-2 2026-08-15 and 2026-08-16 on ten fixtures.
    let mut evaluated_bytes = 0usize;
    let mut base_read_units = 0f64;
    let mut matched = Vec::new();
    for (_, _, json) in rows {
        // A row that will not deserialise is a storage fault, and it is reported
        // rather than walked past. Skipping it left the row counted in
        // `evaluated`, which paces the limit and mints the continuation, but not
        // in `evaluated_bytes`, which is what the read is charged on, so the two
        // halves of the same page disagreed about it. `actions::scan` reports the
        // same fault on the same JSON.
        let item: Item = serde_json::from_str(&json).map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad item JSON in storage: {e}"))
        })?;
        // A row of the table itself is charged on its billed size, a vector at
        // four bytes per dimension (captured eu-west-2 and us-east-1,
        // 2026-10-23). An index row stays sized by its numbers.
        evaluated_bytes += if index.is_none() {
            crate::types::billed_item_size(&item, vector_defs)
        } else {
            crate::types::item_size(&item)
        };

        // The reach-back runs on every row walked, not on the rows the filter
        // keeps. Charging only the survivors made a filter that matched nothing
        // free, where AWS charged the same 4.5 on the table arm as the
        // unfiltered read of the same three rows. Captured eu-west-2 2026-08-17.
        //
        // Each reach-back is charged on the bytes of the base item it read, at
        // the request's consistency, and the per-row charges are summed rather
        // than the bytes. Three rows reaching back cost 1.5 on the table arm for
        // ~50 byte and ~3KB base items alike, 4.5 for ~9KB ones, and 9 for the
        // same 9KB rows under ConsistentRead. Summing the bytes first would
        // report 3.5 for the 9KB case, which is what the same rows cost read
        // straight from the table; the reach-back rounds per row instead.
        let full = if reach_back {
            fetch_base_item(storage, table_name, table_key_schema, &item).await?
        } else {
            None
        };
        if let Some(ref full) = full {
            base_read_units += crate::types::read_capacity_units_with_consistency(
                crate::types::billed_item_size(full, vector_defs),
                consistent_read,
            );
        }

        // The filter still reads the index row rather than the base item it
        // reached back for. That is what the index can evaluate, which is why a
        // keyed read naming an unprojected filter attribute is rejected outright
        // and an unkeyed one matches nothing. The reach-back serves the
        // projection, not the WHERE clause.
        if matches_where(&item, where_clause, parameters) {
            matched.push(full.unwrap_or(item));
        }
    }

    Ok(Window {
        matched,
        last_evaluated,
        evaluated,
        evaluated_bytes,
        base_read_units,
    })
}

/// Read the base table item an index row points at, for an LSI projection the
/// index cannot serve on its own.
///
/// `None` when the row is genuinely absent, which the caller answers from the
/// index row instead. A row that is present but will not deserialise is a
/// storage fault and is reported, the same way the index row itself is.
async fn fetch_base_item<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    table_key_schema: &crate::actions::helpers::KeySchema,
    index_row: &Item,
) -> Result<Option<Item>> {
    let pk = index_row
        .get(&table_key_schema.partition_key)
        .and_then(|v| v.to_key_string())
        .unwrap_or_default();
    let sk = match table_key_schema.sort_key.as_deref() {
        Some(name) => index_row
            .get(name)
            .and_then(|v| v.to_key_string())
            .unwrap_or_default(),
        None => String::new(),
    };
    match storage.get_item(table_name, &pk, &sk).await? {
        Some(json) => Ok(Some(serde_json::from_str::<Item>(&json).map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad item JSON in storage: {e}"))
        })?)),
        None => Ok(None),
    }
}

/// Where a page stopped: the storage keys of its last row.
///
/// An index read carries the base table key alongside the index key. Without
/// it the backend's cursor collapses to `(index_pk, index_sk)`, which cannot
/// advance past rows sharing an index key, and those rows are dropped with no
/// error. `src/actions/scan.rs` documents the same trap on its own cursor.
struct Cursor {
    pk: String,
    sk: String,
    base_pk: Option<String>,
    base_sk: Option<String>,
}

/// DynamoDB's refusal of a PartiQL read of a global secondary index whose key
/// has more than one partition key or sort key attribute. It names the table,
/// but only a read qualified with such an index is refused: the table itself
/// and its other indexes stay readable and writable (captured eu-west-2 and
/// us-east-1, 2026-10-09).
pub(crate) const MULTI_ATTRIBUTE_INDEX_REFUSAL: &str = "PartiQL statements cannot be executed \
on tables with multi-attribute HASH keys or multi-attribute RANGE keys.";

/// Refuse a read of `index_name` when that index's key has more than one
/// attribute on either side.
pub(crate) fn reject_multi_attribute_index(
    resolved: &ResolvedTable,
    index_name: &str,
) -> Result<()> {
    let multi_attribute = crate::actions::gsi::parse_gsi_defs(&resolved.meta)?
        .iter()
        .any(|g| g.index_name == index_name && (g.hash_attrs.len() > 1 || g.range_attrs.len() > 1));
    if multi_attribute {
        return Err(DynoxideError::ValidationException(
            MULTI_ATTRIBUTE_INDEX_REFUSAL.to_string(),
        ));
    }
    Ok(())
}

/// [`reject_multi_attribute_index`] for a statement refused before it is
/// parsed. A table that cannot be loaded is left to the statement's own
/// rejection.
pub(crate) async fn reject_multi_attribute_index_by_name<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    index_name: &str,
) -> Result<()> {
    match ResolvedTable::load(storage, table_name).await {
        Ok(resolved) => reject_multi_attribute_index(&resolved, index_name),
        Err(_) => Ok(()),
    }
}

/// Which index a qualified `SELECT` resolved to, and the keys to read it by.
struct ResolvedIndex {
    /// The index as `actions::gsi` parsed it. Held whole rather than copied
    /// field by field, so the projection rule stays in one place.
    def: crate::actions::gsi::IndexDef,
    is_lsi: bool,
}

impl ResolvedIndex {
    fn name(&self) -> &str {
        &self.def.index_name
    }

    fn pk_attr(&self) -> &str {
        &self.def.pk_attr
    }

    fn sk_attr(&self) -> Option<&str> {
        self.def.sk_attr.as_deref()
    }
}

impl ResolvedIndex {
    /// Whether an entry in this index carries `attr`, by the same rule
    /// `build_index_item` projects with.
    fn projects(&self, attr: &str, table_keys: &crate::actions::helpers::KeySchema) -> bool {
        self.def.projects(
            attr,
            &table_keys.partition_key,
            table_keys.sort_key.as_deref(),
        )
    }
}

/// The attribute a path names, ignoring any document navigation after it.
/// `address.city` and `tags[0]` are both carried by `address` and `tags`, and
/// a quoted `"a.b"` by `a.b`.
fn root_attribute(path: &str) -> std::borrow::Cow<'_, str> {
    if path.starts_with('"')
        && let Some(segments) = split_path_segments(path)
        && let Some(PathSegment::Key(name)) = segments.into_iter().next()
    {
        return name;
    }
    let end = path.find(['.', '[']).unwrap_or(path.len());
    std::borrow::Cow::Borrowed(&path[..end])
}

/// The path a WHERE condition on one path reads; `None` for a condition on
/// other operands, which narrows no key range.
fn condition_path(condition: &WhereCondition) -> Option<&str> {
    match condition {
        WhereCondition::Comparison(c) | WhereCondition::NotComparison(c) => Some(c.path.as_str()),
        WhereCondition::Exists(p)
        | WhereCondition::NotExists(p)
        | WhereCondition::BeginsWith(p, _)
        | WhereCondition::NotBeginsWith(p, _)
        | WhereCondition::Between(p, _, _)
        | WhereCondition::NotBetween(p, _, _)
        | WhereCondition::In(p, _)
        | WhereCondition::Contains(p, _)
        | WhereCondition::NotContains(p, _)
        | WhereCondition::IsMissing(p)
        | WhereCondition::IsNotMissing(p)
        | WhereCondition::IsType(p, _)
        | WhereCondition::IsNotType(p, _) => Some(p.as_str()),
        _ => None,
    }
}

/// Every path a WHERE condition reads.
fn condition_paths(condition: &WhereCondition) -> Vec<&str> {
    fn operand_paths(operand: &Operand) -> Vec<&str> {
        match operand {
            Operand::Path(path) => vec![path.as_str()],
            Operand::Size(inner) => operand_paths(inner),
            Operand::Condition(inner) => condition_paths(inner),
            _ => Vec::new(),
        }
    }
    match condition {
        WhereCondition::Compare(left, _, right) => {
            [left, right].into_iter().flat_map(operand_paths).collect()
        }
        WhereCondition::CompareBetween(operand, low, high) => [operand, low, high]
            .into_iter()
            .flat_map(operand_paths)
            .collect(),
        WhereCondition::CompareIn(operand, list) => std::iter::once(operand)
            .chain(list)
            .flat_map(operand_paths)
            .collect(),
        WhereCondition::AttributeType(operand, _) => operand_paths(operand),
        WhereCondition::Not(inner) => condition_paths(inner),
        other => condition_path(other).into_iter().collect(),
    }
}

/// Every attribute a WHERE clause reads.
fn where_attributes(where_clause: Option<&WhereClause>) -> Vec<String> {
    let Some(wc) = where_clause else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for group in &wc.groups {
        for condition in group {
            for path in condition_paths(condition) {
                let root = root_attribute(path).to_string();
                if !out.contains(&root) {
                    out.push(root);
                }
            }
        }
    }
    out
}

/// Resolve a `"table"."index"` qualifier against the table's metadata.
///
/// The rejection deliberately does not name the index. `Query` and `Scan` build
/// `"... specified index: {name}"` through the helpers in `actions::gsi` and
/// `actions::lsi`, and AWS appends the name there but not on the PartiQL
/// surface, so reusing those helpers here would be wrong by exactly the suffix.
/// Captured eu-west-2 2026-08-15.
fn resolve_index(meta: &crate::storage::TableMetadata, index_name: &str) -> Result<ResolvedIndex> {
    if let Some(lsi) = crate::actions::lsi::parse_lsi_defs(meta)?
        .into_iter()
        .find(|l| l.index_name == index_name)
    {
        return Ok(ResolvedIndex {
            def: lsi,
            is_lsi: true,
        });
    }
    if let Some(gsi) = crate::actions::gsi::parse_gsi_defs(meta)?
        .into_iter()
        .find(|g| g.index_name == index_name)
    {
        return Ok(ResolvedIndex {
            def: gsi,
            is_lsi: false,
        });
    }
    // A vector index exists but no PartiQL read can address it, and DynamoDB
    // says so by type rather than reporting the index missing (captured
    // eu-west-2, 2026-08-11). Checked after the two reachable kinds so a
    // GSI or LSI qualifier never pays for parsing the vector definitions.
    if crate::actions::vector_index::parse_vector_defs(meta)?
        .iter()
        .any(|v| v.index_name == index_name)
    {
        return Err(DynoxideError::ValidationException(
            "Scan operation not supported on this index type".to_string(),
        ));
    }
    Err(DynoxideError::ValidationException(
        "The table does not have the specified index".to_string(),
    ))
}

/// A digest binding a continuation token to its statement and parameters.
/// DynamoDB binds it to the statement exactly as written, so that is what is
/// hashed, beside the parameters by value (see `hash_parameters`): a space, a
/// keyword's case, the projection or a map literal's key order changed between
/// pages is a different statement, and `0` and `0.0` are different parameters
/// (captured eu-west-2 and us-east-1, 2026-10-10). Tokens are ephemeral and
/// in-process, so the hash does not need to be stable across builds.
fn statement_fingerprint(written: &str, parameters: &[AttributeValue]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    written.hash(&mut hasher);
    hash_parameters(parameters, &mut hasher);
    hasher.finish()
}

/// Feed a statement's parameters into a hasher as DynamoDB matches them when
/// a NextToken comes back (captured eu-west-2 and us-east-1, 2026-10-10):
///
/// - a number as the same decimal, value and scale both, as Java's
///   `BigDecimal.equals` compares them. The exponent's case, a `+` in the
///   exponent or in front, leading zeros and the sign of zero don't matter;
///   `1.0` is not `1`, `100` is not `1E2`, and `1.5E1` is `15`;
/// - a set as a set, in any order: a number set's members by value whatever
///   their scale, a string or binary set's exactly;
/// - anything else as sent, a map with its keys sorted so its digest doesn't
///   hang on the order its `HashMap` iterates in. A list or map parameter
///   never resumes a read anyway (see `execute_select`).
fn hash_parameters<H: std::hash::Hasher>(parameters: &[AttributeValue], hasher: &mut H) {
    use std::hash::Hash;
    parameters.len().hash(hasher);
    for parameter in parameters {
        match parameter {
            AttributeValue::N(n) => {
                "N".hash(hasher);
                decimal_identity(n).hash(hasher);
            }
            AttributeValue::NS(members) => {
                let mut values: Vec<String> = members
                    .iter()
                    .map(|n| crate::types::normalize_dynamo_number(n))
                    .collect();
                values.sort_unstable();
                "NS".hash(hasher);
                values.hash(hasher);
            }
            AttributeValue::SS(members) => {
                let mut members: Vec<&String> = members.iter().collect();
                members.sort_unstable();
                "SS".hash(hasher);
                members.hash(hasher);
            }
            AttributeValue::BS(members) => {
                let mut members: Vec<&Vec<u8>> = members.iter().collect();
                members.sort_unstable();
                "BS".hash(hasher);
                members.hash(hasher);
            }
            other => crate::canonical_json(other).hash(hasher),
        }
    }
}

/// A number as Java's `BigDecimal` holds it: its sign, its unscaled digits
/// without leading zeros, and its scale (the digits after the point less the
/// exponent). Zero has no sign. Text that isn't a number is kept whole, so it
/// matches only itself.
fn decimal_identity(n: &str) -> (bool, String, i64) {
    let negative = n.starts_with('-');
    let unsigned = n.trim_start_matches(['+', '-']);
    let (coefficient, exponent) = match unsigned.find(['e', 'E']) {
        Some(at) => match unsigned[at + 1..].parse::<i64>() {
            Ok(exponent) => (&unsigned[..at], exponent),
            Err(_) => return (false, n.to_string(), i64::MIN),
        },
        None => (unsigned, 0),
    };
    let (whole, fraction) = coefficient.split_once('.').unwrap_or((coefficient, ""));
    if !whole
        .chars()
        .chain(fraction.chars())
        .all(|c| c.is_ascii_digit())
    {
        return (false, n.to_string(), i64::MIN);
    }
    let digits: String = whole
        .chars()
        .chain(fraction.chars())
        .skip_while(|&d| d == '0')
        .collect();
    let scale = i64::try_from(fraction.len())
        .unwrap_or(i64::MAX)
        .saturating_sub(exponent);
    (negative && !digits.is_empty(), digits, scale)
}

/// Callers should treat this as opaque, but it is only base64 and anyone can
/// read it. It carries the last row's keys, which a projection may have been
/// asked to strip from the items - the same trade `LastEvaluatedKey` makes -
/// plus a fingerprint binding it to the statement that minted it. Nothing may
/// be inferred from its shape; it is free to change.
fn encode_next_token(table_name: &str, fingerprint: u64, stop: &Cursor) -> String {
    use base64::Engine;
    let payload = serde_json::json!({
        "t": table_name,
        "f": fingerprint,
        "pk": stop.pk,
        "sk": stop.sk,
        "bpk": stop.base_pk,
        "bsk": stop.base_sk,
    })
    .to_string();
    base64::engine::general_purpose::STANDARD.encode(payload)
}

/// The rejection for a well-formed token that belongs to a different request.
/// DynamoDB separates the two failure shapes: a token that cannot be read at
/// all is "Invalid NextToken", while one minted by a different table,
/// statement or parameters is "NextToken does not match request" (both
/// captured eu-west-2, 2026-07-29).
fn token_mismatch() -> DynoxideError {
    DynoxideError::ValidationException("NextToken does not match request".to_string())
}

/// Decode a token, rejecting one minted against a different table or a
/// different statement. A token is a position in one row walk; replayed into
/// another table, another WHERE clause or other parameters it would silently
/// skip rows, so a fingerprint mismatch is rejected the same way as a
/// cross-table replay, with DynamoDB's mismatch message rather than the
/// malformed-token one.
fn decode_next_token(token: &str, table_name: &str, fingerprint: u64) -> Result<Cursor> {
    use base64::Engine;
    let invalid = || DynoxideError::ValidationException("Invalid NextToken".to_string());
    let raw = base64::engine::general_purpose::STANDARD
        .decode(token)
        .map_err(|_| invalid())?;
    let value: serde_json::Value = serde_json::from_slice(&raw).map_err(|_| invalid())?;
    if value["t"].as_str() != Some(table_name) || value["f"].as_u64() != Some(fingerprint) {
        return Err(token_mismatch());
    }
    Ok(Cursor {
        pk: value["pk"].as_str().ok_or_else(invalid)?.to_string(),
        sk: value["sk"].as_str().ok_or_else(invalid)?.to_string(),
        base_pk: value["bpk"].as_str().map(str::to_string),
        base_sk: value["bsk"].as_str().map(str::to_string),
    })
}

/// Translate a single AND-group's sort-key conditions into the resolved form
/// the Query action feeds its SQL builder, so a pk-bound SELECT reads only the
/// key-condition range and `limit` paces the way DynamoDB's does.
///
/// All or nothing: `None` unless every condition on the sort-key attribute is
/// one a key condition can express (`=`, `<`, `<=`, `>`, `>=`, BETWEEN,
/// begins_with) with operands that resolve to key-typed scalars. A partial
/// translation would narrow the read incorrectly, whereas falling back to the
/// unfiltered partition read stays correct because `matches_where` still runs
/// on every row. Conditions on other attributes are ignored here and keep
/// filtering post-read.
fn translate_sk_conditions(
    group: &[Arc<WhereCondition>],
    sk_name: &str,
    parameters: &[AttributeValue],
) -> Option<Vec<ResolvedSortKeyCondition>> {
    let mut resolved = Vec::new();
    for cond in group {
        match &**cond {
            WhereCondition::Comparison(c) if names_attribute(&c.path, sk_name) => {
                let value = resolve_value(&c.value, parameters).ok()?;
                value.to_key_string()?;
                let sk = sk_name.to_string();
                resolved.push(match c.op {
                    CompOp::Eq => ResolvedSortKeyCondition::Eq(sk, value),
                    CompOp::Lt => ResolvedSortKeyCondition::Lt(sk, value),
                    CompOp::Le => ResolvedSortKeyCondition::Le(sk, value),
                    CompOp::Gt => ResolvedSortKeyCondition::Gt(sk, value),
                    CompOp::Ge => ResolvedSortKeyCondition::Ge(sk, value),
                    // Not-equal is not a key condition; a range read cannot
                    // express it.
                    CompOp::Ne => return None,
                });
            }
            WhereCondition::Between(path, lo, hi) if names_attribute(path, sk_name) => {
                let lo = resolve_value(lo, parameters).ok()?;
                let hi = resolve_value(hi, parameters).ok()?;
                lo.to_key_string()?;
                hi.to_key_string()?;
                resolved.push(ResolvedSortKeyCondition::Between(
                    sk_name.to_string(),
                    lo,
                    hi,
                ));
            }
            WhereCondition::BeginsWith(path, prefix) if names_attribute(path, sk_name) => {
                let prefix = resolve_value(prefix, parameters).ok()?;
                prefix.to_key_string()?;
                resolved.push(ResolvedSortKeyCondition::BeginsWith(
                    sk_name.to_string(),
                    prefix,
                ));
            }
            // An IN with one value is read as `=`, as DynamoDB reads it.
            // Captured eu-west-2 and us-east-1, 2026-10-10, for the sort key
            // through BatchExecuteStatement and ExecuteTransaction; the
            // ExecuteStatement charge is inferred from those.
            WhereCondition::In(path, values)
                if names_attribute(path, sk_name) && values.len() == 1 =>
            {
                let value = resolve_value(&values[0], parameters).ok()?;
                value.to_key_string()?;
                resolved.push(ResolvedSortKeyCondition::Eq(sk_name.to_string(), value));
            }
            // Any other condition shape on the sort key (IN, contains, a
            // negated comparison, negated begins_with, the existence checks) has
            // no key-condition equivalent, so nothing at all is pushed down for
            // the sort key. A negated comparison in particular must never drive
            // the read: it keeps rows that have no sort key value to compare,
            // and a range read cannot express that.
            WhereCondition::NotComparison(crate::partiql::parser::Condition { path, .. })
            | WhereCondition::NotBeginsWith(path, _)
            | WhereCondition::In(path, _)
            | WhereCondition::Contains(path, _)
            | WhereCondition::Exists(path)
            | WhereCondition::NotExists(path)
            | WhereCondition::IsMissing(path)
            | WhereCondition::IsNotMissing(path)
            | WhereCondition::IsType(path, _)
            | WhereCondition::IsNotType(path, _)
                if names_attribute(path, sk_name) =>
            {
                return None;
            }
            _ => {}
        }
    }
    Some(resolved)
}

/// Find a partition key equality condition, searching across all OR groups.
/// Whether the WHERE clause keys this index, which is what decides between a
/// query and a scan and so whether an unprojected filter attribute is rejected.
///
/// Broader than `find_pk_condition` on one axis and identical on the other. An
/// `IN` on the index partition key counts: one value reads as `=` and queries
/// the index, while several can't be pushed down as a single key and still
/// scan; AWS rejects an unprojected filter alongside either. An index key reached through OR does not count, and AWS
/// accepts an unprojected filter there. Captured eu-west-2 2026-08-15.
///
/// "Reached through OR" is the presence of an alternation, not the absence of
/// the key from some branch of it. Case R11 in that capture ran
/// `(gsiPk='x' AND nonproj='N1') OR (gsiPk='y')`, where every branch does name
/// the key, and AWS still accepted the unprojected filter. So the group count is
/// the test, and a rule reading the key off every group would reject what AWS
/// allows.
fn keys_index(where_clause: Option<&WhereClause>, pk_name: &str) -> bool {
    let Some(wc) = where_clause else {
        return false;
    };
    if wc.groups.len() != 1 {
        return false;
    }
    wc.groups[0].iter().any(|c| match &**c {
        WhereCondition::Comparison(cond) => {
            names_attribute(&cond.path, pk_name) && cond.op == CompOp::Eq
        }
        WhereCondition::In(path, _) => names_attribute(path, pk_name),
        _ => false,
    })
}

/// The one value a single-group WHERE clause pins the partition key to, when
/// it pins one: an `=`, or an `IN` with one value, which DynamoDB reads exactly
/// as `=` (captured eu-west-2 and us-east-1, 2026-10-10: `pk IN ['a']` costs
/// what `pk = 'a'` costs, where a scan costs more).
fn find_pk_condition<'a>(wc: &'a WhereClause, pk_name: &str) -> Option<&'a PartiqlValue> {
    // Only optimise to a Query when there is a single OR group
    // (multi-group OR with pk in only one group would need a union approach).
    // A partition key named by more than one condition isn't a key read either
    // (captured eu-west-2 and us-east-1, 2026-10-09: `pk = true AND pk = 'x'`
    // is served as a scan and returns nothing).
    let [group] = wc.groups.as_slice() else {
        return None;
    };
    group_pk_value(group, pk_name)
}

/// The one value every group of a clause pins the partition key to, when
/// they all pin the same one: `pk = 'a' AND (x = 1 OR x = 2)` flattens to two
/// groups that both name partition `a`.
fn pk_in_every_group<'a>(wc: &'a WhereClause, pk_name: &str) -> Option<&'a PartiqlValue> {
    let (first, rest) = wc.groups.split_first()?;
    let value = group_pk_value(first, pk_name)?;
    rest.iter()
        .all(|group| group_pk_value(group, pk_name) == Some(value))
        .then_some(value)
}

/// The one value a group pins the partition key to, as `find_pk_condition`
/// reads it.
fn group_pk_value<'a>(group: &'a [Arc<WhereCondition>], pk_name: &str) -> Option<&'a PartiqlValue> {
    let mut pinning = group.iter().filter_map(|c| match &**c {
        WhereCondition::Comparison(cond)
            if names_attribute(&cond.path, pk_name) && cond.op == CompOp::Eq =>
        {
            Some(std::slice::from_ref(&cond.value))
        }
        WhereCondition::In(path, values) if names_attribute(path, pk_name) => {
            Some(values.as_slice())
        }
        _ => None,
    });
    let values = pinning.next()?;
    if pinning.any(|other| other != values) {
        return None;
    }
    // An IN with several values names several partitions; it stays a scan.
    match values {
        [value] => Some(value),
        _ => None,
    }
}

/// A table's metadata and its parsed key schema, worked out once.
///
/// The key schema is the half worth carrying. Metadata is answered from a cache
/// on the native backend and crosses the bridge on wasm, but the key schema is
/// parsed out of JSON on every call on both, and nothing keeps the parsed form.
///
/// A batch resolves this before any of its statements run and every statement
/// then works from it, so `require_table` no longer runs per statement and a
/// table altered or dropped part way through a batch is served from the
/// snapshot taken at the start of it. The window is one request wide and
/// DynamoDB promises nothing across the members of a batch, so this is recorded
/// rather than closed.
pub struct ResolvedTable {
    pub meta: crate::storage::TableMetadata,
    pub key_schema: crate::actions::helpers::KeySchema,
    /// The table this was resolved from, so a caller handing it to
    /// [`execute_written_page`] alongside a statement against another table
    /// can be spotted rather than obeyed.
    pub table_name: String,
}

impl ResolvedTable {
    pub async fn load<S: StorageBackend>(storage: &S, table_name: &str) -> Result<Self> {
        let meta = require_table(storage, table_name).await?;
        let key_schema = crate::actions::helpers::parse_key_schema(&meta)?;
        Ok(Self {
            meta,
            key_schema,
            table_name: table_name.to_string(),
        })
    }
}

/// The `(table, pk, sk)` a statement targets, given its table already resolved.
///
/// For duplicate detection across a batch. `None` when the statement does not
/// resolve to a single item, which covers a `SELECT` spanning a partition and
/// anything whose key cannot be read off the statement. A `SELECT` naming every
/// key attribute does resolve, and AWS rejects a batch carrying two of them
/// against one item just as it does for writes.
///
/// The result is a tuple rather than a joined string. Joining on a delimiter is
/// not injective once a key value contains it: `pk='a#b', sk='c'` and
/// `pk='a', sk='b#c'` render alike, and AWS accepts that pair as two distinct
/// items.
///
/// Both batch surfaces resolve a table once and then ask this of every statement
/// against it, because the metadata and the key schema are per table while the
/// key is per statement. A statement whose key cannot be read yields `None` and
/// is left to fail on its own terms during execution rather than surfacing here
/// as a duplicate-detection failure.
///
/// The table name comes off the resolution rather than being passed alongside
/// it, so the two cannot disagree about which table the key belongs to.
pub fn statement_target_in(
    stmt: &Statement,
    parameters: &[AttributeValue],
    resolved: &ResolvedTable,
) -> Option<(String, String, String)> {
    statement_key(stmt, parameters, &resolved.key_schema)
        .map(|(pk, sk)| (resolved.table_name.clone(), pk, sk))
}

/// The single item a statement names, read off an already-parsed key schema.
fn statement_key(
    stmt: &Statement,
    parameters: &[AttributeValue],
    key_schema: &crate::actions::helpers::KeySchema,
) -> Option<(String, String)> {
    let key_of = |source: &dyn Fn(&str) -> Option<AttributeValue>| -> Option<(String, String)> {
        let pk = source(&key_schema.partition_key)?.to_key_string()?;
        let sk = match key_schema.sort_key {
            Some(ref name) => source(name)?.to_key_string()?,
            None => String::new(),
        };
        Some((pk, sk))
    };

    // A SELECT's key may be pinned by `=` or by an IN with one value, which
    // DynamoDB treats as the same item (captured eu-west-2 and us-east-1,
    // 2026-10-10: `pk IN ['a']` twice, or beside `pk = 'a'`, is a duplicate).
    let from_where = |where_clause: &Option<WhereClause>| -> Option<(String, String)> {
        let wc = where_clause.as_ref()?;
        key_of(&|name: &str| {
            find_key_value_in_groups(&wc.groups, name)
                .and_then(|value| resolve_value(value, parameters).ok())
        })
    };

    // An UPDATE or DELETE writes the item its top-level key equalities name,
    // so that is the item it holds in the transaction: in
    // `(pk = 'b' OR x = 1) AND pk = 'a'` the OR group is a condition on item
    // a, not a second key (captured eu-west-2 and us-east-1, 2026-10-10).
    // Equalities that disagree name no item here; the statement refuses
    // itself for them.
    let from_top_level = |where_clause: &Option<WhereClause>| -> Option<(String, String)> {
        let wc = where_clause.as_ref()?;
        key_of(&|name: &str| {
            let mut values = wc
                .top_level_equalities
                .iter()
                .filter(|cond| names_attribute(&cond.path, name))
                .map(|cond| resolve_value(&cond.value, parameters).ok());
            let first = values.next()??;
            let first_key = first.to_key_string();
            if values.any(|v| v.and_then(|v| v.to_key_string()) != first_key) {
                return None;
            }
            Some(first)
        })
    };

    match stmt {
        Statement::Insert { item, .. } => key_of(&|name: &str| {
            item.get(name)
                .and_then(|v| resolve_value(v, parameters).ok())
        }),
        Statement::Update { where_clause, .. } | Statement::Delete { where_clause, .. } => {
            from_top_level(where_clause)
        }
        // A SELECT resolves only when its WHERE pins every key attribute; one
        // spanning a partition yields nothing and takes no part in the check.
        Statement::Select { where_clause, .. } => from_where(where_clause),
    }
}

/// An INSERT's item with its parameters filled in.
fn resolve_insert_item(
    item_template: &HashMap<String, PartiqlValue>,
    parameters: &[AttributeValue],
) -> Result<Item> {
    let mut item = HashMap::new();
    for (k, v) in item_template {
        let resolved = match v {
            PartiqlValue::Literal(av) => av.clone(),
            PartiqlValue::Parameter(idx) => parameters.get(*idx).cloned().ok_or_else(|| {
                DynoxideError::ValidationException(format!(
                    "Parameter index {idx} out of range (have {} parameters)",
                    parameters.len()
                ))
            })?,
        };
        item.insert(k.clone(), resolved);
    }
    Ok(item)
}

/// Returns what the insert consumed. An `if_not_exists` duplicate makes it a
/// no-op, which carries no images and no index units.
async fn execute_insert<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    item_template: &HashMap<String, PartiqlValue>,
    parameters: &[AttributeValue],
    if_not_exists: bool,
    capacity_mode: Option<&str>,
    resolved: Option<&ResolvedTable>,
) -> Result<WriteCapacity> {
    let mut item = resolve_insert_item(item_template, parameters)?;

    // Resolved by the caller when a preparation pass already did it, which is
    // what stops a batch reading the same table's metadata and parsing the same
    // key schema once per statement on top of once per batch member.
    let owned;
    let resolved = match resolved {
        Some(r) => r,
        None => {
            owned = ResolvedTable::load(storage, table_name).await?;
            &owned
        }
    };
    let meta = &resolved.meta;
    let key_schema = &resolved.key_schema;

    // A key of the wrong type, or of a type no key can have, is refused in
    // PartiQL's words ahead of PutItem's key checks (captured eu-west-2 and
    // us-east-1, 2026-10-09).
    let keys = std::iter::once((
        &key_schema.partition_key,
        Some(&key_schema.partition_key_type),
    ))
    .chain(
        key_schema
            .sort_key
            .as_ref()
            .map(|sk| (sk, key_schema.sort_key_type.as_ref())),
    );
    for (name, declared) in keys {
        if let Some(value) = item.get(name)
            && typed_key(value, name, declared).is_err()
        {
            return Err(DynoxideError::ValidationException(format!(
                "Key attribute's data type should match its data type in table's schema: Key {name}"
            )));
        }
    }

    // Validate keys present
    crate::actions::helpers::validate_item_keys(&item, key_schema, meta)?;
    // An item exactly at the nesting limit is a duplicate first when its key
    // is taken, and otherwise gets PartiQL's short message, so the item checks,
    // which would give the long one, are held back for it (captured eu-west-2,
    // 2026-10-09).
    let at_the_limit = item_nested_to_the_limit(&item);
    if !at_the_limit {
        crate::validation::validate_item_attribute_values(&item)?;
    }

    // Deduplicate sets
    crate::validation::normalize_item_sets(&mut item);

    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = crate::actions::helpers::extract_key_strings(&item, key_schema)?;

    // PartiQL INSERT must reject duplicates (unlike PutItem which overwrites)
    let existing = storage.get_item(table_name, &pk, &sk).await?;
    if existing.is_some() {
        if if_not_exists {
            // Silently succeed, writing nothing and touching no index. The
            // table still has to be named, or the no-op lands in a bucket
            // keyed on the empty string and surfaces as a nameless entry.
            return Ok(WriteCapacity::new(
                table_name,
                None,
                None,
                HashMap::new(),
                HashMap::new(),
            ));
        }
        return Err(DynoxideError::DuplicateItemException(
            "Duplicate primary key exists in table".to_string(),
        ));
    }
    if at_the_limit {
        return Err(nested_to_the_limit());
    }

    let item_json = serde_json::to_string(&item)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

    // An INSERT is held to the same flat limit as PutItem, and reports it in the
    // same words. Captured against eu-west-2, where the ceiling is 409,600 on
    // the item itself.
    if crate::types::item_size(&item) > crate::types::MAX_ITEM_SIZE {
        return Err(DynoxideError::ValidationException(
            "Item size has exceeded the maximum allowed size".to_string(),
        ));
    }
    // What the row records and the write is charged: the billed size, with a
    // vector attribute at 4 bytes per dimension.
    let vector_defs = crate::actions::vector_index::parse_vector_defs(meta)?;
    let item_size = crate::types::billed_item_size(&item, &vector_defs);

    let hash_prefix = item
        .get(&key_schema.partition_key)
        .map(crate::storage::compute_hash_prefix)
        .unwrap_or_default();
    let old_json = storage
        .put_item_with_hash(table_name, &pk, &sk, &item_json, item_size, &hash_prefix)
        .await?;

    let old_item: Option<Item> = old_json.as_ref().and_then(|j| serde_json::from_str(j).ok());

    let target = crate::actions::gsi::IndexWrite {
        table_name,
        pk: &pk,
        sk: &sk,
        pk_attr: &key_schema.partition_key,
        sk_attr: key_schema.sort_key.as_deref(),
        old_item: old_item.as_ref(),
        capacity_mode,
    };

    let gsi_units =
        crate::actions::gsi::maintain_gsis_after_write(storage, meta, &target, &item).await?;

    let lsi_units =
        crate::actions::lsi::maintain_lsis_after_write(storage, meta, &target, &item).await?;

    // Vector index maintenance
    let vector_bytes = crate::actions::vector_index::maintain_vector_indexes_after_write(
        storage, meta, &target, &item,
    )
    .await?;

    // Stream record
    crate::streams::record_stream_event(storage, meta, old_item.as_ref(), Some(&item)).await?;

    // An INSERT rejects an existing key above, so there is never an old image
    // here; `old_item` only ever comes back empty.
    Ok(WriteCapacity::new(
        table_name,
        old_item
            .as_ref()
            .map(|old| crate::types::billed_item_size(old, &vector_defs)),
        Some(item_size),
        gsi_units,
        lsi_units,
    )
    .with_vector_bytes(vector_bytes))
}

/// Applies an UPDATE and returns the `RETURNING` projection (or `None` when the
/// statement carried no `RETURNING` clause) and what the write consumed. An
/// update that resolves to an empty item is skipped and carries no images.
#[allow(clippy::too_many_arguments)]
async fn execute_update<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    set_clauses: &[crate::partiql::parser::SetClause],
    remove_paths: &[String],
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    returning: Option<ReturningVariant>,
    capacity_mode: Option<&str>,
    resolved: Option<&ResolvedTable>,
    old_on_condition_failure: bool,
) -> Result<(Option<Item>, WriteCapacity)> {
    // Resolved by the caller when a preparation pass already did it, which is
    // what stops a batch reading the same table's metadata and parsing the same
    // key schema once per statement on top of once per batch member.
    let owned;
    let resolved = match resolved {
        Some(r) => r,
        None => {
            owned = ResolvedTable::load(storage, table_name).await?;
            &owned
        }
    };
    let meta = &resolved.meta;
    let key_schema = &resolved.key_schema;

    // WHERE clause is required for UPDATE to identify the item
    let wc = where_clause.ok_or_else(mandatory_key_equality)?;

    // An UPDATE addresses one item, named by the clause's top-level key
    // equalities: see `single_item_key`.
    let (pk_str, sk_str) = single_item_key(wc, key_schema, parameters)?;

    // A key attribute can't be set or removed: the item would be stored under
    // its old key with another key value in it. Refused in UpdateItem's words
    // before the item is read, so a missing item or a false condition makes no
    // difference (captured eu-west-2 and us-east-1, 10 October 2026).
    let paths = set_clauses
        .iter()
        .map(|clause| clause.path.as_str())
        .chain(remove_paths.iter().map(String::as_str));
    for path in paths {
        let key = std::iter::once(key_schema.partition_key.as_str())
            .chain(key_schema.sort_key.as_deref())
            .find(|key| names_attribute(path, key));
        if let Some(key) = key {
            return Err(crate::actions::helpers::key_attribute_update_refusal(key));
        }
    }

    // No two paths of one statement may overlap, the later clause being `path
    // one`. Refused before the item is read, as UpdateItem refuses it.
    reject_overlapping_paths(set_clauses, remove_paths)?;

    // Get existing item
    let existing_json = storage.get_item(table_name, &pk_str, &sk_str).await?;
    let mut item: Item = existing_json
        .as_ref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    let old_item = item.clone();

    // PartiQL UPDATE is not an upsert: the target item must already exist, so a
    // missing item fails ConditionalCheckFailedException and creates nothing.
    // Non-key WHERE predicates act as a further condition on the existing item;
    // if that predicate is false the update fails the same way, carrying the
    // item when it was asked for. Neither writes.
    if existing_json.is_none() {
        return Err(DynoxideError::ConditionalCheckFailedException(
            "The conditional request failed".to_string(),
            None,
        ));
    }
    if !matches_where(&old_item, where_clause, parameters) {
        return Err(DynoxideError::ConditionalCheckFailedException(
            "The conditional request failed".to_string(),
            old_on_condition_failure.then_some(old_item),
        ));
    }

    let before_item = item.clone();

    // Every SET value is worked out from the item as it was, so `SET a = 1, b = a
    // + 1` reads the old `a`, or finds none (eu-west-2, 18 October 2026).
    // They are written in path order, as UpdateItem writes them, so two SETs
    // past the end of a list append in the order of their indexes (eu-west-2
    // and us-east-1, 20 October 2026). A SET that removes its path is taken out
    // with the REMOVEs.
    let mut writes = Vec::with_capacity(set_clauses.len());
    for clause in set_clauses {
        writes.push((
            element_path(&clause.path),
            &clause.path,
            resolve_set_value(&clause.value, &before_item, parameters)?,
        ));
    }
    writes.sort_by(|(a, _, _), (b, _, _)| crate::expressions::update::compare_paths(a, b));
    let mut removals = Vec::with_capacity(remove_paths.len());
    for (elements, path, value) in writes {
        match value {
            Some(val) => set_nested_value(&mut item, path, val)?,
            None => removals.push((elements, path)),
        }
    }

    // REMOVE goes last, highest path first, so that taking out `l[0]` does not
    // move the element that `l[1]` names. A path the item did not have is left
    // alone, even where a SET past the end has since put an element there; its
    // parent must still be one the item had.
    for path in remove_paths {
        let elements = element_path(path);
        if elements.is_empty()
            || crate::expressions::resolve_path(&before_item, &elements).is_some()
        {
            removals.push((elements, path));
        } else {
            crate::expressions::check_update_parent(&before_item, &elements)
                .map_err(DynoxideError::ValidationException)?;
        }
    }
    removals.sort_by(|(a, _), (b, _)| crate::expressions::update::compare_paths(b, a));
    for (_, path) in removals {
        remove_nested_value(&mut item, path)?;
    }

    // Ensure keys are present
    if item.is_empty() {
        return Ok((
            None,
            WriteCapacity::new(table_name, None, None, HashMap::new(), HashMap::new()),
        ));
    }

    // Validate attribute values after SET clauses applied
    reject_item_nested_to_the_limit(&item)?;
    crate::validation::validate_item_attribute_values(&item)?;
    crate::validation::normalize_item_sets(&mut item);

    // Reject an index key this update set to an invalid value (see helpers).
    crate::actions::helpers::validate_updated_index_keys(&before_item, &item, meta)?;

    let item_json = serde_json::to_string(&item)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

    // A PartiQL UPDATE is measured flat against the resulting item, the same as
    // an INSERT, and unlike the standalone UpdateItem, which is also charged for
    // what its request carries. Captured against eu-west-2: an update this
    // surface accepts at 409,600 is refused by UpdateItem at the same key. Only
    // the wording is shared with UpdateItem.
    if crate::types::item_size(&item) > crate::types::MAX_ITEM_SIZE {
        return Err(DynoxideError::ValidationException(
            "Item size to update has exceeded the maximum allowed size".to_string(),
        ));
    }
    // What the row records and the write is charged: the billed size.
    let vector_defs = crate::actions::vector_index::parse_vector_defs(meta)?;
    let item_size = crate::types::billed_item_size(&item, &vector_defs);

    let hash_prefix = item
        .get(&key_schema.partition_key)
        .map(crate::storage::compute_hash_prefix)
        .unwrap_or_default();
    storage
        .put_item_with_hash(
            table_name,
            &pk_str,
            &sk_str,
            &item_json,
            item_size,
            &hash_prefix,
        )
        .await?;

    // `old_item` carries injected key attributes on a create-through-update, so
    // genuine absence is decided by whether a row was read back.
    let old_ref = existing_json.is_some().then_some(&old_item);

    let target = crate::actions::gsi::IndexWrite {
        table_name,
        pk: &pk_str,
        sk: &sk_str,
        pk_attr: &key_schema.partition_key,
        sk_attr: key_schema.sort_key.as_deref(),
        old_item: old_ref,
        capacity_mode,
    };

    let gsi_units =
        crate::actions::gsi::maintain_gsis_after_write(storage, meta, &target, &item).await?;

    let lsi_units =
        crate::actions::lsi::maintain_lsis_after_write(storage, meta, &target, &item).await?;

    // Vector index maintenance
    let vector_bytes = crate::actions::vector_index::maintain_vector_indexes_after_write(
        storage, meta, &target, &item,
    )
    .await?;

    // Stream record
    crate::streams::record_stream_event(storage, meta, old_ref, Some(&item)).await?;

    // Build the RETURNING projection from the item's before/after states. The
    // MODIFIED variants project each touched path (a nested `a.b` yields just
    // the changed leaf, never the whole `a` attribute and never the key),
    // resolved against the relevant item.
    let projection = returning.map(|variant| {
        let modified: std::collections::BTreeSet<String> = set_clauses
            .iter()
            .map(|c| c.path.clone())
            .chain(remove_paths.iter().cloned())
            .collect();
        project_returning(variant, &old_item, &item, &modified)
    });

    Ok((
        projection,
        WriteCapacity::new(
            table_name,
            old_ref.map(|old| crate::types::billed_item_size(old, &vector_defs)),
            Some(item_size),
            gsi_units,
            lsi_units,
        )
        .with_vector_bytes(vector_bytes),
    ))
}

/// Build the `RETURNING` projection for an UPDATE from the item's before/after
/// states. `ALL` variants return the whole item (key included); `MODIFIED`
/// variants project only the touched paths, resolved against the old item
/// (`MODIFIED OLD *`) or the new item (`MODIFIED NEW *`), which never includes
/// the key. A `MODIFIED` projection can be empty (a path that no longer
/// resolves contributes nothing), which the caller surfaces as an empty `Items`
/// array.
fn project_returning(
    variant: ReturningVariant,
    old_item: &Item,
    new_item: &Item,
    modified: &std::collections::BTreeSet<String>,
) -> Item {
    match variant {
        ReturningVariant::AllOld => old_item.clone(),
        ReturningVariant::AllNew => new_item.clone(),
        ReturningVariant::ModifiedOld => project_modified(modified, old_item),
        ReturningVariant::ModifiedNew => project_modified(modified, new_item),
    }
}

/// An intermediate `MODIFIED` projection node. List positions are collected by
/// their real index in a `BTreeMap` so that, on conversion, the contributed
/// elements emerge as a dense list in ascending index order: DynamoDB does not
/// keep gaps, so `SET a[0], a[2]` projects `{a: [v0, v2]}`, not a sparse list.
enum ProjNode {
    Leaf(AttributeValue),
    Map(HashMap<String, ProjNode>),
    List(std::collections::BTreeMap<usize, ProjNode>),
}

/// Project the touched paths from `source` into a `MODIFIED` projection. Each
/// path is resolved against `source` (navigating map keys and real list
/// indices); a path that resolves contributes its value, one that does not
/// contributes nothing. This is why a map REMOVE yields nothing under
/// `MODIFIED NEW` (the key is gone) while a list REMOVE contributes the
/// shifted-in element (`tags[1]` still resolves after `REMOVE tags[1]`).
/// Contributed list elements pack densely in ascending index order. This is by
/// index, not statement order: for `SET a[2]=.., a[0]=..` real DynamoDB returns
/// `{a: [v0, v2]}`, which the `BTreeMap`-keyed pack matches.
fn project_modified(paths: &std::collections::BTreeSet<String>, source: &Item) -> Item {
    let mut root: HashMap<String, ProjNode> = HashMap::new();
    for path in paths {
        if let (Some(val), Some(segments)) =
            (resolve_nested_path(source, path), split_path_segments(path))
        {
            // The top-level of an item is always addressed by a map key.
            if let Some((PathSegment::Key(key), rest)) = segments.split_first() {
                let node = root
                    .entry((*key).to_string())
                    .or_insert_with(|| fresh_proj_node(rest));
                insert_proj_node(node, rest, val.clone());
            }
        }
    }
    root.into_iter()
        .map(|(k, node)| (k, proj_node_to_value(node)))
        .collect()
}

/// The container a projection node needs for its next segment: a list for an
/// index, a map otherwise. An empty `segments` is a leaf position, overwritten
/// immediately by the value, so the placeholder type there is immaterial.
fn fresh_proj_node(segments: &[PathSegment]) -> ProjNode {
    match segments.first() {
        Some(PathSegment::Index(_)) => ProjNode::List(std::collections::BTreeMap::new()),
        _ => ProjNode::Map(HashMap::new()),
    }
}

/// Insert `val` at `segments` within `node`, creating intermediate map/list
/// nodes as needed.
fn insert_proj_node(node: &mut ProjNode, segments: &[PathSegment], val: AttributeValue) {
    let Some((seg, rest)) = segments.split_first() else {
        *node = ProjNode::Leaf(val);
        return;
    };
    match seg {
        PathSegment::Key(k) => {
            if let ProjNode::Map(map) = node {
                let child = map
                    .entry((*k).to_string())
                    .or_insert_with(|| fresh_proj_node(rest));
                insert_proj_node(child, rest, val);
            }
        }
        PathSegment::Index(i) => {
            if let ProjNode::List(list) = node {
                let child = list.entry(*i).or_insert_with(|| fresh_proj_node(rest));
                insert_proj_node(child, rest, val);
            }
        }
    }
}

/// Convert a projection node into an `AttributeValue`. A `List` node's
/// `BTreeMap` yields its elements in ascending index order, densely.
fn proj_node_to_value(node: ProjNode) -> AttributeValue {
    match node {
        ProjNode::Leaf(v) => v,
        ProjNode::Map(map) => AttributeValue::M(
            map.into_iter()
                .map(|(k, n)| (k, proj_node_to_value(n)))
                .collect(),
        ),
        ProjNode::List(list) => {
            AttributeValue::L(list.into_values().map(proj_node_to_value).collect())
        }
    }
}

/// Returns the deleted item (None when the target was missing and the delete
/// was a no-op) and its size in bytes for `ConsumedCapacity` accounting.
async fn execute_delete<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    capacity_mode: Option<&str>,
    resolved: Option<&ResolvedTable>,
    old_on_condition_failure: bool,
) -> Result<(Option<Item>, WriteCapacity)> {
    // Resolved by the caller when a preparation pass already did it, which is
    // what stops a batch reading the same table's metadata and parsing the same
    // key schema once per statement on top of once per batch member.
    let owned;
    let resolved = match resolved {
        Some(r) => r,
        None => {
            owned = ResolvedTable::load(storage, table_name).await?;
            &owned
        }
    };
    let meta = &resolved.meta;
    let key_schema = &resolved.key_schema;

    let wc = where_clause.ok_or_else(mandatory_key_equality)?;

    // A DELETE addresses one item, named by the clause's top-level key
    // equalities, the sort key included where the table has one: see
    // `single_item_key`.
    let (pk_str, sk_str) = single_item_key(wc, key_schema, parameters)?;

    // Non-key WHERE predicates act as a condition on the existing item, like a
    // conditional write. AWS raises ConditionalCheckFailedException when the item
    // is present but the condition is false, and a missing item is a silent
    // no-op (the condition is never evaluated). Re-running the full WHERE via
    // matches_where covers both the key equality (always true for the fetched
    // item) and any extra predicates.
    if let Some(json) = storage.get_item(table_name, &pk_str, &sk_str).await? {
        let existing: Item = serde_json::from_str(&json)
            .map_err(|e| DynoxideError::InternalServerError(format!("Bad item JSON: {e}")))?;
        if !matches_where(&existing, where_clause, parameters) {
            return Err(DynoxideError::ConditionalCheckFailedException(
                "The conditional request failed".to_string(),
                old_on_condition_failure.then_some(existing),
            ));
        }
    }

    let old_json = storage.delete_item(table_name, &pk_str, &sk_str).await?;
    let old_item: Option<Item> = old_json.as_ref().and_then(|j| serde_json::from_str(j).ok());

    let target = crate::actions::gsi::IndexWrite {
        table_name,
        pk: &pk_str,
        sk: &sk_str,
        pk_attr: &key_schema.partition_key,
        sk_attr: key_schema.sort_key.as_deref(),
        old_item: old_item.as_ref(),
        capacity_mode,
    };

    let gsi_units = crate::actions::gsi::maintain_gsis_after_delete(storage, meta, &target).await?;

    let lsi_units = crate::actions::lsi::maintain_lsis_after_delete(storage, meta, &target).await?;

    // Vector index maintenance
    let vector_bytes =
        crate::actions::vector_index::maintain_vector_indexes_after_delete(storage, meta, &target)
            .await?;

    // Stream record
    if old_item.is_some() {
        crate::streams::record_stream_event(storage, meta, old_item.as_ref(), None).await?;
    }

    // A delete is charged on the item it removed, so a no-op delete against a
    // missing target carries no image and falls back to the one-unit minimum.
    let vector_defs = crate::actions::vector_index::parse_vector_defs(meta)?;
    let capacity = WriteCapacity::new(
        table_name,
        old_item
            .as_ref()
            .map(|old| crate::types::billed_item_size(old, &vector_defs)),
        None,
        gsi_units,
        lsi_units,
    )
    .with_vector_bytes(vector_bytes);
    Ok((old_item, capacity))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The table a statement names, or DynamoDB's PartiQL answer when there is
/// none: `Requested resource not found`, with nothing after it, on every
/// statement kind (captured eu-west-2 and us-east-1, 2026-10-09).
async fn require_table<S: StorageBackend>(
    storage: &S,
    table_name: &str,
) -> Result<crate::storage::TableMetadata> {
    crate::actions::helpers::require_table_for_item_op(storage, table_name).await
}

/// The single item a write's WHERE clause addresses, as `(pk, sk)` key strings,
/// or the rejection to give the author.
///
/// UPDATE and DELETE address exactly one item, and DynamoDB takes it from the
/// equalities among the clause's top-level conditions, the ones joined to the
/// rest by `AND` alone. A key named only under an `OR`, a `NOT` or an `IN` is
/// not pinned, even when every branch names the same item: `pk = 'a' OR pk =
/// 'a'` and `NOT (NOT pk = 'a')` are both refused, while `pk = 'a' AND (b = 1
/// OR c = 2)` runs with the `OR` as a condition. A key given two different
/// values is refused on its own terms (captured eu-west-2 and us-east-1,
/// 2026-10-09).
fn single_item_key(
    wc: &WhereClause,
    key_schema: &crate::actions::helpers::KeySchema,
    parameters: &[AttributeValue],
) -> Result<(String, String)> {
    let pinned =
        |name: &str, declared: Option<&ScalarAttributeType>| -> Result<(String, AttributeValue)> {
            let mut key: Option<(String, AttributeValue)> = None;
            for cond in wc
                .top_level_equalities
                .iter()
                .filter(|c| names_attribute(&c.path, name))
            {
                let resolved = resolve_value(&cond.value, parameters)?;
                let value = typed_key(&resolved, name, declared)?;
                match key {
                    Some((ref already, _)) if *already != value => {
                        return Err(DynoxideError::ValidationException(format!(
                            "Multiple conditions on same key {name}. Only single item \
                         Update/Insert/Delete are supported"
                        )));
                    }
                    _ => key = Some((value, resolved)),
                }
            }
            key.ok_or_else(mandatory_key_equality)
        };
    let (pk, pk_value) = pinned(
        &key_schema.partition_key,
        Some(&key_schema.partition_key_type),
    )?;
    let sk = match key_schema.sort_key {
        Some(ref sk_name) => Some(pinned(sk_name, key_schema.sort_key_type.as_ref())?),
        None => None,
    };
    // A key too large to be one is refused once the whole key is pinned, the
    // partition key first (captured eu-west-2 and us-east-1, 2026-10-10: a
    // missing sort key equality is reported ahead of either).
    if let Some(refusal) = crate::actions::helpers::key_size_refusal(&pk_value, false) {
        return Err(refusal);
    }
    let sk = match sk {
        Some((sk, sk_value)) => {
            if let Some(refusal) = crate::actions::helpers::key_size_refusal(&sk_value, true) {
                return Err(refusal);
            }
            sk
        }
        None => String::new(),
    };
    Ok((pk, sk))
}

/// DynamoDB's refusal of an UPDATE or DELETE that doesn't pin its key with
/// top-level equalities, a missing WHERE clause included.
fn mandatory_key_equality() -> DynoxideError {
    DynoxideError::ValidationException(
        "Where clause does not contain a mandatory equality on all key attributes".to_string(),
    )
}

/// Find the value a key attribute is pinned to by an `=` or an IN with one
/// value, searching across all OR groups.
fn find_key_value_in_groups<'a>(
    groups: &'a [Vec<Arc<WhereCondition>>],
    path: &str,
) -> Option<&'a PartiqlValue> {
    groups.iter().find_map(|group| {
        group.iter().find_map(|c| match &**c {
            WhereCondition::Comparison(cond)
                if names_attribute(&cond.path, path) && cond.op == CompOp::Eq =>
            {
                Some(&cond.value)
            }
            WhereCondition::In(p, values) if names_attribute(p, path) && values.len() == 1 => {
                Some(&values[0])
            }
            _ => None,
        })
    })
}

/// Find a comparison condition matching a given path with Eq operator.
fn find_comparison<'a>(
    conditions: &'a [Arc<WhereCondition>],
    path: &str,
) -> Option<&'a crate::partiql::parser::Condition> {
    conditions.iter().find_map(|c| match &**c {
        WhereCondition::Comparison(cond)
            if names_attribute(&cond.path, path) && cond.op == CompOp::Eq =>
        {
            Some(cond)
        }
        _ => None,
    })
}

/// Whether `value` starts with `prefix`: a string by its characters, a binary
/// value by its bytes. Values of different types never match, so a binary
/// prefix doesn't match a string that spells the same bytes (captured
/// eu-west-2 and us-east-1, 2026-10-09).
fn begins_with(value: &AttributeValue, prefix: &AttributeValue) -> bool {
    match (value, prefix) {
        (AttributeValue::S(s), AttributeValue::S(p)) => s.starts_with(p.as_str()),
        (AttributeValue::B(b), AttributeValue::B(p)) => b.starts_with(p),
        _ => false,
    }
}

/// Resolve a PartiqlValue to a concrete AttributeValue.
fn resolve_value(val: &PartiqlValue, parameters: &[AttributeValue]) -> Result<AttributeValue> {
    match val {
        PartiqlValue::Literal(av) => Ok(av.clone()),
        PartiqlValue::Parameter(idx) => parameters.get(*idx).cloned().ok_or_else(|| {
            DynoxideError::ValidationException(format!(
                "Parameter index {idx} out of range (have {} parameters)",
                parameters.len()
            ))
        }),
    }
}

/// What a SET assignment leaves at its path: the value to write, or `None`
/// for a `set_delete` that empties the set or never found one, which leaves
/// the path absent.
fn resolve_set_value(
    val: &SetValue,
    item: &Item,
    parameters: &[AttributeValue],
) -> Result<Option<AttributeValue>> {
    if let SetValue::SetCall {
        delete,
        path,
        members,
        refusal_at,
    } = val
    {
        return resolve_set_call(*delete, path, members, refusal_at, item, parameters);
    }
    resolve_set_operand(val, item, parameters).map(Some)
}

/// `set_add` or `set_delete` against the set stored at `path`.
///
/// A missing attribute takes the members on `set_add` and stays missing on
/// `set_delete`, and removing every member removes the attribute. Members of
/// another type than the stored set, or a stored value that isn't a set, are
/// an operand of the wrong type; numbers are compared by value. The members
/// themselves are checked as a parameter is, duplicates included (captured
/// eu-west-2, us-east-1, eu-west-1 and us-west-2, 2026-10-09).
fn resolve_set_call(
    delete: bool,
    path: &str,
    members: &PartiqlValue,
    refusal_at: &str,
    item: &Item,
    parameters: &[AttributeValue],
) -> Result<Option<AttributeValue>> {
    let name = if delete { "SET_DELETE" } else { "SET_ADD" };
    let members = resolve_value(members, parameters)?;
    if !matches!(
        members,
        AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_)
    ) {
        return Err(DynoxideError::ValidationException(format!(
            "The second argument to {name} must be a value with type SET at {refusal_at}"
        )));
    }
    crate::validation::validate_parameter_value(&members)?;
    let wrong_type = || {
        DynoxideError::ValidationException(
            "An operand in the update expression has an incorrect data type".to_string(),
        )
    };
    let result = match (resolve_nested_path(item, path), &members) {
        (None, _) => return Ok((!delete).then_some(members)),
        (Some(AttributeValue::SS(held)), AttributeValue::SS(change)) => {
            AttributeValue::SS(combine_set_members(held, change, delete, String::clone))
        }
        (Some(AttributeValue::NS(held)), AttributeValue::NS(change)) => {
            AttributeValue::NS(combine_set_members(held, change, delete, |n: &String| {
                crate::types::normalize_dynamo_number(n)
            }))
        }
        (Some(AttributeValue::BS(held)), AttributeValue::BS(change)) => {
            AttributeValue::BS(combine_set_members(held, change, delete, Vec::clone))
        }
        _ => return Err(wrong_type()),
    };
    let empty = match &result {
        AttributeValue::SS(m) | AttributeValue::NS(m) => m.is_empty(),
        AttributeValue::BS(m) => m.is_empty(),
        _ => false,
    };
    Ok((!empty).then_some(result))
}

/// `set_delete` takes the held members `change` names; `set_add` appends the
/// members of `change` the set doesn't hold, in the order written. Members are
/// matched by `key`, which for a number set is its normalised value. Each
/// member's key is worked out once and looked up in a hash set, so the work
/// grows with the sets' sizes rather than their product.
fn combine_set_members<T: Clone, K: Eq + std::hash::Hash>(
    held: &[T],
    change: &[T],
    delete: bool,
    key: impl Fn(&T) -> K,
) -> Vec<T> {
    use std::collections::HashSet;
    if delete {
        let named: HashSet<K> = change.iter().map(&key).collect();
        held.iter()
            .filter(|m| !named.contains(&key(m)))
            .cloned()
            .collect()
    } else {
        let mut seen: HashSet<K> = held.iter().map(&key).collect();
        let mut out = held.to_vec();
        for m in change {
            if seen.insert(key(m)) {
                out.push(m.clone());
            }
        }
        out
    }
}

/// Resolve a SetValue other than a set call to a concrete AttributeValue,
/// potentially using the current item.
fn resolve_set_operand(
    val: &SetValue,
    item: &Item,
    parameters: &[AttributeValue],
) -> Result<AttributeValue> {
    match val {
        SetValue::SetCall { .. } => unreachable!("resolve_set_value handles a set call"),
        SetValue::Simple(pv) => resolve_value(pv, parameters),
        SetValue::Add(attr, term) => arithmetic(attr, term, item, parameters, "+"),
        SetValue::Sub(attr, term) => arithmetic(attr, term, item, parameters, "-"),
        SetValue::ListAppend(first, second) => {
            // A literal or parameter of the wrong type is refused before the
            // item is read, ahead of an attribute that is missing or mistyped.
            let static_first = literal_of(first, parameters, "list_append", is_list)?;
            let static_second = literal_of(second, parameters, "list_append", is_list)?;
            let list_a = static_first.map_or_else(|| attribute_of(first, item, is_list), Ok)?;
            let list_b = static_second.map_or_else(|| attribute_of(second, item, is_list), Ok)?;
            match (list_a, list_b) {
                (AttributeValue::L(mut la), AttributeValue::L(lb)) => {
                    la.extend(lb);
                    Ok(AttributeValue::L(la))
                }
                _ => unreachable!("both operands were checked to be lists"),
            }
        }
    }
}

fn is_list(value: &AttributeValue) -> bool {
    matches!(value, AttributeValue::L(_))
}

fn is_number(value: &AttributeValue) -> bool {
    matches!(value, AttributeValue::N(_))
}

/// The value of a literal or parameter operand, `None` for an attribute. A
/// value that `fits` is returned; one that does not is refused as DynamoDB
/// refuses it, naming the operator and the operand's type.
fn literal_of(
    term: &SetTerm,
    parameters: &[AttributeValue],
    operator: &str,
    fits: fn(&AttributeValue) -> bool,
) -> Result<Option<AttributeValue>> {
    let SetTerm::Value(value) = term else {
        return Ok(None);
    };
    let value = resolve_value(value, parameters)?;
    if fits(&value) {
        return Ok(Some(value));
    }
    Err(DynoxideError::ValidationException(format!(
        "Incorrect operand type for operator or function; operator or function: {operator}, \
         operand type: {}",
        value.type_name()
    )))
}

/// The value of an attribute operand: refused when the item lacks it, and when
/// it is not of the type the operator takes.
fn attribute_of(
    term: &SetTerm,
    item: &Item,
    fits: fn(&AttributeValue) -> bool,
) -> Result<AttributeValue> {
    let SetTerm::Path(path) = term else {
        unreachable!("a value is read by literal_of");
    };
    match resolve_nested_path(item, path) {
        None => Err(DynoxideError::ValidationException(
            crate::expressions::MISSING_ATTRIBUTE.to_string(),
        )),
        Some(value) if fits(value) => Ok(value.clone()),
        Some(_) => Err(DynoxideError::ValidationException(
            crate::expressions::OPERAND_TYPE.to_string(),
        )),
    }
}

/// `attr + term` or `attr - term`, on numbers only.
fn arithmetic(
    attr: &str,
    term: &SetTerm,
    item: &Item,
    parameters: &[AttributeValue],
    operator: &str,
) -> Result<AttributeValue> {
    use bigdecimal::BigDecimal;
    use std::str::FromStr;
    let literal = literal_of(term, parameters, operator, is_number)?;
    let left = attribute_of(&SetTerm::Path(attr.to_string()), item, is_number)?;
    let right = literal.map_or_else(|| attribute_of(term, item, is_number), Ok)?;
    let (AttributeValue::N(left), AttributeValue::N(right)) = (left, right) else {
        unreachable!("both operands were checked to be numbers");
    };
    let number = |n: &str| {
        BigDecimal::from_str(n)
            .map_err(|e| DynoxideError::ValidationException(format!("Invalid number: {e}")))
    };
    let (left, right) = (number(&left)?, number(&right)?);
    let result = if operator == "+" {
        left + right
    } else {
        left - right
    };
    Ok(AttributeValue::N(format_bigdecimal(&result)))
}

/// A statement's path as the elements an UpdateItem path is made of, for the
/// checks the two share. A path that does not split names nothing and yields
/// no elements.
fn element_path(path: &str) -> Vec<crate::expressions::PathElement> {
    use crate::expressions::PathElement;
    split_path_segments(path)
        .unwrap_or_default()
        .into_iter()
        .map(|segment| match segment {
            PathSegment::Key(key) => PathElement::Attribute(key.into_owned()),
            PathSegment::Index(i) => PathElement::Index(i),
        })
        .collect()
}

/// Refuse a statement with two paths that overlap, DynamoDB's check on the
/// paths a statement writes and removes. The later clause is `path one`;
/// the SET clauses are taken as written before the REMOVE paths.
fn reject_overlapping_paths(
    set_clauses: &[crate::partiql::parser::SetClause],
    remove_paths: &[String],
) -> Result<()> {
    let mut paths: Vec<_> = set_clauses
        .iter()
        .map(|clause| element_path(&clause.path))
        .chain(remove_paths.iter().map(|path| element_path(path)))
        .collect();
    paths.reverse();
    crate::expressions::update::check_path_overlaps(&paths)
        .map_err(DynoxideError::ValidationException)
}

/// The `ValidationException` DynamoDB raises for a document path that cannot be
/// applied by an update (e.g. indexing a scalar, or a missing intermediate).
fn invalid_update_path() -> DynoxideError {
    DynoxideError::ValidationException(crate::expressions::INVALID_UPDATE_PATH.to_string())
}

/// Set a value at a document path, navigating both map keys and list indices,
/// so `SET tags[0] = :v` writes the real list element rather than a literal
/// `tags[0]` key. A list index at or beyond the end appends, matching DynamoDB.
/// Every parent on the way must already exist: a missing map is refused, not
/// created, the same as in an UpdateItem (eu-west-2, 18 October 2026).
fn set_nested_value(item: &mut Item, path: &str, val: AttributeValue) -> Result<()> {
    let segments = split_path_segments(path).ok_or_else(invalid_update_path)?;
    let (first, rest) = segments.split_first().ok_or_else(invalid_update_path)?;
    let key = match first {
        PathSegment::Key(k) => (*k).to_string(),
        // The top-level item is a map; it cannot be indexed.
        PathSegment::Index(_) => return Err(invalid_update_path()),
    };
    if rest.is_empty() {
        item.insert(key, val);
        return Ok(());
    }
    let entry = item.get_mut(&key).ok_or_else(invalid_update_path)?;
    set_into_value(entry, rest, val)
}

/// Recursive helper for [`set_nested_value`]: apply the remaining path segments
/// to `current`.
fn set_into_value(
    current: &mut AttributeValue,
    segments: &[PathSegment],
    val: AttributeValue,
) -> Result<()> {
    let (seg, rest) = segments.split_first().expect("segments is non-empty");
    if rest.is_empty() {
        return match seg {
            PathSegment::Key(k) => match current {
                AttributeValue::M(map) => {
                    map.insert((*k).to_string(), val);
                    Ok(())
                }
                _ => Err(invalid_update_path()),
            },
            PathSegment::Index(i) => match current {
                AttributeValue::L(list) => {
                    if *i < list.len() {
                        list[*i] = val;
                    } else {
                        list.push(val);
                    }
                    Ok(())
                }
                _ => Err(invalid_update_path()),
            },
        };
    }
    match seg {
        PathSegment::Key(k) => match current {
            AttributeValue::M(map) => match map.get_mut(k.as_ref()) {
                Some(next) => set_into_value(next, rest, val),
                None => Err(invalid_update_path()),
            },
            _ => Err(invalid_update_path()),
        },
        PathSegment::Index(i) => match current {
            AttributeValue::L(list) => match list.get_mut(*i) {
                Some(next) => set_into_value(next, rest, val),
                None => Err(invalid_update_path()),
            },
            _ => Err(invalid_update_path()),
        },
    }
}

/// Remove the value at a document path, navigating both map keys and list
/// indices, so `REMOVE tags[0]` deletes the list element (shifting the rest)
/// rather than a literal `tags[0]` key. A last step that is not there is left
/// alone; a parent that is missing, or of the wrong type for the step taken
/// through it, is refused as it is for SET (eu-west-2, 18 October 2026).
fn remove_nested_value(item: &mut Item, path: &str) -> Result<()> {
    let segments = split_path_segments(path).ok_or_else(invalid_update_path)?;
    let (first, rest) = segments.split_first().ok_or_else(invalid_update_path)?;
    // The top-level item is a map; it cannot be indexed.
    let PathSegment::Key(key) = first else {
        return Err(invalid_update_path());
    };
    if rest.is_empty() {
        item.remove(key.as_ref());
        return Ok(());
    }
    let current = item.get_mut(key.as_ref()).ok_or_else(invalid_update_path)?;
    remove_from_value(current, rest)
}

/// Recursive helper for [`remove_nested_value`].
fn remove_from_value(current: &mut AttributeValue, segments: &[PathSegment]) -> Result<()> {
    let (seg, rest) = segments.split_first().expect("segments is non-empty");
    if rest.is_empty() {
        return match (seg, current) {
            (PathSegment::Key(k), AttributeValue::M(map)) => {
                map.remove(k.as_ref());
                Ok(())
            }
            (PathSegment::Index(i), AttributeValue::L(list)) => {
                if *i < list.len() {
                    list.remove(*i);
                }
                Ok(())
            }
            _ => Err(invalid_update_path()),
        };
    }
    let next = match (seg, current) {
        (PathSegment::Key(k), AttributeValue::M(map)) => map.get_mut(k.as_ref()),
        (PathSegment::Index(i), AttributeValue::L(list)) => list.get_mut(*i),
        _ => None,
    };
    remove_from_value(next.ok_or_else(invalid_update_path)?, rest)
}

/// Check if an item matches a WHERE clause (with OR-group support).
fn matches_where(
    item: &Item,
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
) -> bool {
    let wc = match where_clause {
        Some(wc) => wc,
        None => return true,
    };

    // A placeholder with no parameter to fill it decides nothing: several
    // conditions hold of an item that lacks the attribute (`<>` and the
    // negated forms), and a statement short of its parameters mustn't read or
    // delete an item on that account.
    if wc
        .groups
        .iter()
        .flatten()
        .any(|condition| has_unbound_parameter(condition, parameters.len()))
    {
        return false;
    }

    // OR semantics: any group matching is sufficient
    wc.groups
        .iter()
        .any(|group| matches_conditions(item, group, parameters))
}

/// Whether `condition` names a parameter at or past `bound`, the number
/// supplied. Every variant is listed, so a new one has to say how it reads
/// parameters.
fn has_unbound_parameter(condition: &WhereCondition, bound: usize) -> bool {
    fn value(v: &PartiqlValue, bound: usize) -> bool {
        matches!(v, PartiqlValue::Parameter(i) if *i >= bound)
    }
    fn operand(o: &Operand, bound: usize) -> bool {
        match o {
            Operand::Path(_) => false,
            Operand::Value(v) => value(v, bound),
            Operand::Size(inner) => operand(inner, bound),
            Operand::Condition(inner) => has_unbound_parameter(inner, bound),
        }
    }
    match condition {
        WhereCondition::Comparison(c) | WhereCondition::NotComparison(c) => value(&c.value, bound),
        WhereCondition::Exists(_)
        | WhereCondition::NotExists(_)
        | WhereCondition::IsMissing(_)
        | WhereCondition::IsNotMissing(_)
        | WhereCondition::IsType(..)
        | WhereCondition::IsNotType(..) => false,
        WhereCondition::BeginsWith(_, v)
        | WhereCondition::NotBeginsWith(_, v)
        | WhereCondition::Contains(_, v)
        | WhereCondition::NotContains(_, v) => value(v, bound),
        WhereCondition::Between(_, low, high) | WhereCondition::NotBetween(_, low, high) => {
            value(low, bound) || value(high, bound)
        }
        WhereCondition::In(_, values) => values.iter().any(|v| value(v, bound)),
        WhereCondition::Compare(left, _, right) => operand(left, bound) || operand(right, bound),
        WhereCondition::CompareBetween(o, low, high) => {
            operand(o, bound) || operand(low, bound) || operand(high, bound)
        }
        WhereCondition::CompareIn(o, list) => {
            operand(o, bound) || list.iter().any(|m| operand(m, bound))
        }
        WhereCondition::AttributeType(o, kind) => operand(o, bound) || value(kind, bound),
        WhereCondition::Not(inner) => has_unbound_parameter(inner, bound),
    }
}

/// Check if an item matches all conditions in a group (AND semantics).
fn matches_conditions<C: std::borrow::Borrow<WhereCondition>>(
    item: &Item,
    conditions: &[C],
    parameters: &[AttributeValue],
) -> bool {
    for cond in conditions {
        match cond.borrow() {
            WhereCondition::Comparison(c) => {
                // A missing attribute is unequal to anything, and neither less
                // nor greater (captured eu-west-2 and us-east-1, 2026-10-09).
                let item_val = match resolve_nested_path(item, &c.path) {
                    Some(v) => v,
                    None if c.op == CompOp::Ne => continue,
                    None => return false,
                };
                let target = match resolve_value(&c.value, parameters) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                if !compare_values(item_val, &c.op, &target) {
                    return false;
                }
            }
            WhereCondition::NotComparison(c) => {
                // Logical negation of the comparison, which is why the missing
                // path is kept rather than dropped: the comparison is false on a
                // row that has no such attribute, so its negation is true. The
                // same shape `NOT CONTAINS` and `NOT BEGINS_WITH` already use.
                if let Some(item_val) = resolve_nested_path(item, &c.path) {
                    let target = match resolve_value(&c.value, parameters) {
                        Ok(v) => v,
                        Err(_) => return false,
                    };
                    if compare_values(item_val, &c.op, &target) {
                        return false;
                    }
                } else if c.op == CompOp::Ne {
                    return false;
                }
            }
            WhereCondition::Exists(path) | WhereCondition::IsNotMissing(path) => {
                if resolve_nested_path(item, path).is_none() {
                    return false;
                }
            }
            WhereCondition::NotExists(path) | WhereCondition::IsMissing(path) => {
                if resolve_nested_path(item, path).is_some() {
                    return false;
                }
            }
            WhereCondition::IsType(path, test) => {
                if !resolve_nested_path(item, path).is_some_and(|v| test.matches(v)) {
                    return false;
                }
            }
            WhereCondition::IsNotType(path, test) => {
                if resolve_nested_path(item, path).is_some_and(|v| test.matches(v)) {
                    return false;
                }
            }
            WhereCondition::BeginsWith(path, prefix_val) => {
                let item_val = match resolve_nested_path(item, path) {
                    Some(v) => v,
                    None => return false,
                };
                let prefix = match resolve_value(prefix_val, parameters) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                if !begins_with(item_val, &prefix) {
                    return false;
                }
            }
            WhereCondition::NotBeginsWith(path, prefix_val) => {
                // Logical negation of begins_with: the row matches unless the
                // value starts with the prefix. A missing attribute, or one of
                // another type than the prefix, does not begin with it, so it
                // is kept.
                if let Some(item_val) = resolve_nested_path(item, path) {
                    let prefix = match resolve_value(prefix_val, parameters) {
                        Ok(v) => v,
                        Err(_) => return false,
                    };
                    if begins_with(item_val, &prefix) {
                        return false;
                    }
                }
            }
            WhereCondition::Between(path, low, high) => {
                let item_val = match resolve_nested_path(item, path) {
                    Some(v) => v,
                    None => return false,
                };
                let low_val = match resolve_value(low, parameters) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                let high_val = match resolve_value(high, parameters) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                if !compare_values(item_val, &CompOp::Ge, &low_val)
                    || !compare_values(item_val, &CompOp::Le, &high_val)
                {
                    return false;
                }
            }
            WhereCondition::NotBetween(path, low, high) => {
                // A row without the attribute matches, as for `NOT` of a
                // comparison.
                if let Some(item_val) = resolve_nested_path(item, path) {
                    let (Ok(low_val), Ok(high_val)) = (
                        resolve_value(low, parameters),
                        resolve_value(high, parameters),
                    ) else {
                        return false;
                    };
                    if compare_values(item_val, &CompOp::Ge, &low_val)
                        && compare_values(item_val, &CompOp::Le, &high_val)
                    {
                        return false;
                    }
                }
            }
            WhereCondition::In(path, values) => {
                let item_val = match resolve_nested_path(item, path) {
                    Some(v) => v,
                    None => return false,
                };
                let matched = values.iter().any(|v| {
                    resolve_value(v, parameters)
                        .map(|target| compare_values(item_val, &CompOp::Eq, &target))
                        .unwrap_or(false)
                });
                if !matched {
                    return false;
                }
            }
            WhereCondition::Contains(path, substr_val) => {
                if !contains_value(item, path, substr_val, parameters) {
                    return false;
                }
            }
            WhereCondition::NotContains(path, substr_val) => {
                if contains_value(item, path, substr_val, parameters) {
                    return false;
                }
            }
            WhereCondition::Not(inner) => {
                if matches_conditions(item, std::slice::from_ref(inner), parameters) {
                    return false;
                }
            }
            other => {
                if !operand_condition_holds(item, other, parameters) {
                    return false;
                }
            }
        }
    }

    true
}

/// Whether a condition on operands other than a path and a value holds. A
/// missing value, or the size of one that has none, makes `=`, the ordering
/// operators, BETWEEN and IN false and `<>` true (captured eu-west-2 and
/// us-east-1, 2026-10-09).
fn operand_condition_holds(
    item: &Item,
    condition: &WhereCondition,
    parameters: &[AttributeValue],
) -> bool {
    let value = |operand: &Operand| operand_value(item, operand, parameters);
    match condition {
        WhereCondition::Compare(left, op, right) => match (value(left), value(right)) {
            (Some(left), Some(right)) => compare_values(&left, op, &right),
            _ => *op == CompOp::Ne,
        },
        WhereCondition::CompareBetween(operand, low, high) => {
            match (value(operand), value(low), value(high)) {
                (Some(v), Some(low), Some(high)) => {
                    compare_values(&v, &CompOp::Ge, &low) && compare_values(&v, &CompOp::Le, &high)
                }
                _ => false,
            }
        }
        WhereCondition::CompareIn(operand, list) => value(operand).is_some_and(|v| {
            list.iter()
                .any(|member| value(member).is_some_and(|m| compare_values(&v, &CompOp::Eq, &m)))
        }),
        WhereCondition::AttributeType(operand, kind) => {
            match (value(operand), resolve_value(kind, parameters)) {
                (Some(v), Ok(AttributeValue::S(kind))) => v.type_name() == kind,
                _ => false,
            }
        }
        _ => false,
    }
}

/// An operand's value for an item, or `None` when it has none: a missing path,
/// or the size of a value that has no size. A string's size is its length in
/// UTF-16 code units, as Java counts it (captured eu-west-2 and us-east-1,
/// 2026-10-09: `héllo` is 5 and an emoji 2).
fn operand_value(
    item: &Item,
    operand: &Operand,
    parameters: &[AttributeValue],
) -> Option<AttributeValue> {
    match operand {
        Operand::Path(path) => resolve_nested_path(item, path).cloned(),
        Operand::Value(value) => resolve_value(value, parameters).ok(),
        Operand::Condition(condition) => Some(AttributeValue::BOOL(matches_conditions(
            item,
            std::slice::from_ref(condition.as_ref()),
            parameters,
        ))),
        Operand::Size(inner) => {
            let size = match operand_value(item, inner, parameters)? {
                AttributeValue::S(s) => s.encode_utf16().count(),
                AttributeValue::B(b) => b.len(),
                AttributeValue::SS(set) => set.len(),
                AttributeValue::NS(set) => set.len(),
                AttributeValue::BS(set) => set.len(),
                AttributeValue::L(list) => list.len(),
                AttributeValue::M(map) => map.len(),
                _ => return None,
            };
            Some(AttributeValue::N(size.to_string()))
        }
    }
}

/// Whether `path` holds a value containing `substr_val`: a substring of a
/// string, a member of a set, or an element of a list.
///
/// Shared by `CONTAINS` and `NOT CONTAINS` so the two cannot answer different
/// questions. A path that does not resolve, or a value that cannot be compared
/// to the operand, contains nothing.
fn contains_value(
    item: &Item,
    path: &str,
    substr_val: &PartiqlValue,
    parameters: &[AttributeValue],
) -> bool {
    let Some(item_val) = resolve_nested_path(item, path) else {
        return false;
    };
    let Ok(substr) = resolve_value(substr_val, parameters) else {
        return false;
    };
    match (item_val, &substr) {
        (AttributeValue::S(s), AttributeValue::S(sub)) => s.contains(sub.as_str()),
        (AttributeValue::SS(set), AttributeValue::S(val)) => set.contains(val),
        (AttributeValue::NS(set), AttributeValue::N(val)) => set.contains(val),
        (AttributeValue::L(list), target) => list.contains(target),
        _ => false,
    }
}

/// Resolve a dotted/indexed path to a nested attribute value.
///
/// Supports paths like `"a"`, `"a.b.c"`, and `"a[0].b"`.
fn resolve_nested_path<'a>(item: &'a Item, path: &str) -> Option<&'a AttributeValue> {
    // Fast path: no dots, brackets or quoted name means a simple top-level
    // lookup.
    if !path.contains(['.', '[']) && !path.starts_with('"') {
        return item.get(path);
    }

    let segments = split_path_segments(path)?;
    if segments.is_empty() {
        return None;
    }

    // First segment must be a map key on the top-level item
    let mut current = match &segments[0] {
        PathSegment::Key(k) => item.get(k.as_ref())?,
        PathSegment::Index(_) => return None,
    };

    for seg in &segments[1..] {
        current = match seg {
            PathSegment::Key(k) => match current {
                AttributeValue::M(map) => map.get(k.as_ref())?,
                _ => return None,
            },
            PathSegment::Index(idx) => match current {
                AttributeValue::L(list) => list.get(*idx)?,
                _ => return None,
            },
        };
    }

    Some(current)
}

/// Format a BigDecimal number, stripping unnecessary trailing zeros.
fn format_bigdecimal(n: &bigdecimal::BigDecimal) -> String {
    let normalized = n.normalized();
    if normalized.as_bigint_and_exponent().1 < 0 {
        normalized.with_scale(0).to_string()
    } else {
        normalized.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partiql::parser::parse;

    /// `set_add` appends the members it doesn't hold, in the order written,
    /// once each; `set_delete` keeps the held members it doesn't name. A
    /// number set matches by value. Each member's key is worked out once, so a
    /// large set costs in proportion to its size rather than its square.
    #[test]
    fn set_members_combine_once_per_member() {
        let s = |v: &[&str]| v.iter().map(|m| m.to_string()).collect::<Vec<_>>();
        let held = s(&["a", "b", "c"]);
        assert_eq!(
            combine_set_members(&held, &s(&["c", "d", "d", "e"]), false, String::clone),
            s(&["a", "b", "c", "d", "e"])
        );
        assert_eq!(
            combine_set_members(&held, &s(&["c", "a", "z"]), true, String::clone),
            s(&["b"])
        );
        let numbers = s(&["1", "2.0", "30"]);
        let by_value = |n: &String| crate::types::normalize_dynamo_number(n);
        assert_eq!(
            combine_set_members(&numbers, &s(&["2", "3e1", "4"]), false, by_value),
            s(&["1", "2.0", "30", "4"])
        );
        assert_eq!(
            combine_set_members(&numbers, &s(&["1.00", "30"]), true, by_value),
            s(&["2.0"])
        );

        let held: Vec<String> = (0..2_000).map(|i| format!("h{i}")).collect();
        let change: Vec<String> = (0..2_000).map(|i| format!("c{i}")).collect();
        for delete in [false, true] {
            let calls = std::cell::Cell::new(0usize);
            let counted = |m: &String| {
                calls.set(calls.get() + 1);
                m.clone()
            };
            combine_set_members(&held, &change, delete, counted);
            assert_eq!(
                calls.get(),
                held.len() + change.len(),
                "set_{} worked out {} keys for {} members",
                if delete { "delete" } else { "add" },
                calls.get(),
                held.len() + change.len()
            );
        }
    }

    fn where_of(statement: &str) -> WhereClause {
        match parse(statement).unwrap() {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => wc,
            other => panic!("expected a SELECT with WHERE: {other:?}"),
        }
    }

    /// The fingerprint gives the same answer for the same statement and
    /// parameters, and tells apart any change to the text as written or to a
    /// parameter, so a NextToken resumes only the read it was minted for.
    #[test]
    fn a_fingerprint_follows_the_text_as_written() {
        let fingerprint = |statement: &str| statement_fingerprint(statement, &[]);
        let a = r#"SELECT * FROM "t" WHERE pk = 'k' AND (a = 1 OR b = 2)"#;
        let b = r#"SELECT * FROM "t" WHERE pk = 'k' AND (a = 1 OR b = 3)"#;
        let c = r#"SELECT * FROM "t" WHERE pk = 'k' AND (a = 1 AND b = 2)"#;
        let spaced = r#"SELECT * FROM "t" WHERE pk = 'k' AND (a = 1 OR  b = 2)"#;
        let lower = r#"select * FROM "t" WHERE pk = 'k' AND (a = 1 OR b = 2)"#;
        assert_eq!(fingerprint(a), fingerprint(a));
        for other in [b, c, spaced, lower] {
            assert_ne!(fingerprint(a), fingerprint(other), "{other}");
        }

        // Parameters count by value.
        let with =
            |p: &[AttributeValue]| statement_fingerprint(r#"SELECT * FROM "t" WHERE pk = ?"#, p);
        let one = [AttributeValue::S("1".to_string())];
        let two = [AttributeValue::S("2".to_string())];
        assert_eq!(with(&one), with(&one));
        assert_ne!(with(&one), with(&two));
        assert_ne!(with(&one), with(&[]));
    }

    /// A placeholder with no parameter to fill it decides nothing, whatever
    /// the condition: each of these would hold of the item if the missing
    /// value were simply skipped (a `<>`, a negation, or an IN list whose
    /// other member matches).
    #[test]
    fn a_condition_short_of_its_parameters_never_matches() {
        let item: Item = [
            ("pk".to_string(), AttributeValue::S("a".to_string())),
            ("tag".to_string(), AttributeValue::S("x".to_string())),
        ]
        .into_iter()
        .collect();
        let matched: Vec<&str> = [
            ("Comparison", "flag <> ?"),
            ("NotComparison", "NOT flag = ?"),
            ("In", "tag IN [?, 'x']"),
            ("Compare", "size(tag) <> ?"),
            ("Compare, parameter first", "? <> size(tag)"),
            // A condition function is an operand too: the call itself is
            // fine, and it is the placeholder beside it that is unfilled.
            ("Condition operand", "attribute_exists(flag) <> ?"),
            (
                "Condition operand holding the parameter",
                "attribute_type(flag, ?) <> tag",
            ),
            (
                "Condition operand holding the parameter, on a present path",
                "attribute_type(tag, ?) <> flag",
            ),
            (
                "Condition operand, parameter first",
                "? <> attribute_exists(flag)",
            ),
            (
                "Condition operand under NOT",
                "NOT attribute_exists(flag) = ?",
            ),
            (
                "Condition operand in an IN list",
                "attribute_exists(flag) IN [?, false]",
            ),
            (
                "Condition operand in an IN list under NOT",
                "NOT attribute_exists(flag) IN [?, true]",
            ),
        ]
        .into_iter()
        .filter(|(_, clause)| {
            let wc = where_of(&format!(r#"SELECT * FROM "t" WHERE {clause}"#));
            matches_where(&item, Some(&wc), &[])
        })
        .map(|(kind, _)| kind)
        .collect();
        assert!(
            matched.is_empty(),
            "matched with no parameters: {matched:?}"
        );
        // The same conditions with their parameter supplied are evaluated.
        let wc = where_of(r#"SELECT * FROM "t" WHERE tag IN [?, 'x']"#);
        assert!(matches_where(
            &item,
            Some(&wc),
            &[AttributeValue::S("y".to_string())]
        ));
        // And so are the condition function forms: with the parameter
        // supplied, each of them holds of this item, so it is the missing
        // parameter and nothing else that stops them above.
        let supplied = AttributeValue::BOOL(true);
        for clause in [
            "attribute_exists(flag) <> ?",
            "? <> attribute_exists(flag)",
            "attribute_type(flag, ?) <> tag",
            "attribute_type(tag, ?) <> flag",
            "NOT attribute_exists(flag) = ?",
            "attribute_exists(flag) IN [?, false]",
            "NOT attribute_exists(flag) IN [?, true]",
        ] {
            let wc = where_of(&format!(r#"SELECT * FROM "t" WHERE {clause}"#));
            assert!(
                matches_where(&item, Some(&wc), std::slice::from_ref(&supplied)),
                "{clause}"
            );
        }
    }
}
