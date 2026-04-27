//! PartiQL statement executor.
//!
//! Maps parsed PartiQL statements to internal DynamoDB operations.

use crate::errors::{DynoxideError, Result};
use crate::partiql::parser::{
    CompOp, PartiqlValue, SetValue, Statement, WhereClause, WhereCondition,
};
use crate::storage::Storage;
use crate::types::{AttributeValue, Item};
use std::collections::HashMap;

/// Execute a parsed PartiQL statement.
///
/// Returns `Some(items)` for SELECT (may be empty), `None` for write operations.
/// An optional `limit` restricts how many items a SELECT returns.
pub fn execute(
    storage: &Storage,
    stmt: &Statement,
    parameters: &[AttributeValue],
    limit: Option<usize>,
) -> Result<Option<Vec<Item>>> {
    match stmt {
        Statement::Select {
            table_name,
            projections,
            where_clause,
        } => execute_select(
            storage,
            table_name,
            projections,
            where_clause.as_ref(),
            parameters,
            limit,
        ),
        Statement::Insert {
            table_name,
            item,
            if_not_exists,
        } => {
            execute_insert(storage, table_name, item, parameters, *if_not_exists)?;
            Ok(None)
        }
        Statement::Update {
            table_name,
            set_clauses,
            remove_paths,
            where_clause,
        } => {
            execute_update(
                storage,
                table_name,
                set_clauses,
                remove_paths,
                where_clause.as_ref(),
                parameters,
            )?;
            Ok(None)
        }
        Statement::Delete {
            table_name,
            where_clause,
        } => {
            execute_delete(storage, table_name, where_clause.as_ref(), parameters)?;
            Ok(None)
        }
    }
}

/// Insert a projected value into a result item.
///
/// For dotted paths (e.g. `a.b.c`), DynamoDB PartiQL returns the resolved value
/// keyed by the leaf segment name (`c`), not the full path or reconstructed
/// nested structure. For simple paths and array index paths, the key is used as-is.
fn insert_nested_projection(result: &mut Item, path: &str, val: AttributeValue) {
    let parts: Vec<&str> = path.split('.').collect();
    // Use the leaf segment as the key
    let key = parts.last().unwrap();
    result.insert(key.to_string(), val);
}

fn execute_select(
    storage: &Storage,
    table_name: &str,
    projections: &[String],
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    limit: Option<usize>,
) -> Result<Option<Vec<Item>>> {
    let meta = require_table(storage, table_name)?;
    let key_schema = crate::actions::helpers::parse_key_schema(&meta)?;

    // Check for COUNT(*) projection
    if projections.len() == 1 && projections[0] == "COUNT(*)" {
        let items = collect_matching_items(
            storage,
            table_name,
            where_clause,
            parameters,
            &key_schema,
            None,
        )?;
        let count = items.len();
        let mut result = HashMap::new();
        result.insert("Count".to_string(), AttributeValue::N(count.to_string()));
        return Ok(Some(vec![result]));
    }

    let items = collect_matching_items(
        storage,
        table_name,
        where_clause,
        parameters,
        &key_schema,
        limit,
    )?;

    // Apply projections
    let items = if projections.is_empty() {
        items
    } else {
        items
            .into_iter()
            .map(|item| {
                let mut projected = HashMap::new();
                for proj in projections {
                    if let Some(val) = resolve_nested_path(&item, proj) {
                        insert_nested_projection(&mut projected, proj, val.clone());
                    }
                }
                projected
            })
            .collect()
    };

    Ok(Some(items))
}

/// Collect items that match the WHERE clause, optionally limited.
fn collect_matching_items(
    storage: &Storage,
    table_name: &str,
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    key_schema: &crate::actions::helpers::KeySchema,
    limit: Option<usize>,
) -> Result<Vec<Item>> {
    // Try to use Query if the WHERE clause constrains the partition key
    let pk_condition = where_clause.and_then(|wc| find_pk_condition(wc, &key_schema.partition_key));

    let items: Vec<Item> = if let Some(pk_cond) = pk_condition {
        let pk_val = resolve_value(&pk_cond.value, parameters)?;
        let pk_str = pk_val
            .to_key_string()
            .ok_or_else(|| DynoxideError::ValidationException("Invalid key value".to_string()))?;

        let rows = storage.query_items(table_name, &pk_str, &Default::default())?;

        let iter = rows
            .into_iter()
            .filter_map(|(_, _, json)| serde_json::from_str::<Item>(&json).ok())
            .filter(|item| matches_where(item, where_clause, parameters));

        if let Some(lim) = limit {
            iter.take(lim).collect()
        } else {
            iter.collect()
        }
    } else {
        let rows = storage.scan_items(table_name, &Default::default())?;

        let iter = rows
            .into_iter()
            .filter_map(|(_, _, json)| serde_json::from_str::<Item>(&json).ok())
            .filter(|item| matches_where(item, where_clause, parameters));

        if let Some(lim) = limit {
            iter.take(lim).collect()
        } else {
            iter.collect()
        }
    };

    Ok(items)
}

/// Find a partition key equality condition, searching across all OR groups.
fn find_pk_condition<'a>(
    wc: &'a WhereClause,
    pk_name: &str,
) -> Option<&'a crate::partiql::parser::Condition> {
    // Only optimise to a Query when there is a single OR group
    // (multi-group OR with pk in only one group would need a union approach).
    if wc.groups.len() == 1 {
        wc.groups[0].iter().find_map(|c| match c {
            WhereCondition::Comparison(cond) if cond.path == pk_name && cond.op == CompOp::Eq => {
                Some(cond)
            }
            _ => None,
        })
    } else {
        None
    }
}

fn execute_insert(
    storage: &Storage,
    table_name: &str,
    item_template: &HashMap<String, PartiqlValue>,
    parameters: &[AttributeValue],
    if_not_exists: bool,
) -> Result<()> {
    // Resolve any parameter placeholders in the item
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

    let meta = require_table(storage, table_name)?;
    let key_schema = crate::actions::helpers::parse_key_schema(&meta)?;

    // Validate keys present
    crate::actions::helpers::validate_item_keys(&item, &key_schema, &meta)?;
    crate::validation::validate_item_attribute_values(&item)?;

    // Deduplicate sets
    crate::validation::normalize_item_sets(&mut item);

    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = crate::actions::helpers::extract_key_strings(&item, &key_schema)?;

    // PartiQL INSERT must reject duplicates (unlike PutItem which overwrites)
    let existing = storage.get_item(table_name, &pk, &sk)?;
    if existing.is_some() {
        if if_not_exists {
            // Silently succeed — no-op
            return Ok(());
        }
        return Err(DynoxideError::DuplicateItemException(
            "Duplicate primary key exists in table".to_string(),
        ));
    }

    let item_json = serde_json::to_string(&item)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let item_size = crate::types::item_size(&item);

    let hash_prefix = item
        .get(&key_schema.partition_key)
        .map(crate::storage::compute_hash_prefix)
        .unwrap_or_default();
    let old_json =
        storage.put_item_with_hash(table_name, &pk, &sk, &item_json, item_size, &hash_prefix)?;

    // GSI maintenance
    let table_sk_attr = key_schema.sort_key.as_deref();
    let _ = crate::actions::gsi::maintain_gsis_after_write(
        storage,
        table_name,
        &meta,
        &pk,
        &sk,
        &item,
        &key_schema.partition_key,
        table_sk_attr,
    )?;

    // LSI maintenance
    crate::actions::lsi::maintain_lsis_after_write(
        storage,
        table_name,
        &meta,
        &pk,
        &sk,
        &item,
        &key_schema.partition_key,
        table_sk_attr,
    )?;

    // Stream record
    let old_item: Option<Item> = old_json.as_ref().and_then(|j| serde_json::from_str(j).ok());
    crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), Some(&item))?;

    Ok(())
}

fn execute_update(
    storage: &Storage,
    table_name: &str,
    set_clauses: &[crate::partiql::parser::SetClause],
    remove_paths: &[String],
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
) -> Result<()> {
    let meta = require_table(storage, table_name)?;
    let key_schema = crate::actions::helpers::parse_key_schema(&meta)?;

    // WHERE clause is required for UPDATE to identify the item
    let wc = where_clause.ok_or_else(|| {
        DynoxideError::ValidationException("UPDATE requires a WHERE clause".to_string())
    })?;

    // DynamoDB does not support OR in UPDATE WHERE clauses
    if wc.groups.len() > 1 {
        return Err(DynoxideError::ValidationException(
            "UPDATE does not support OR conditions in WHERE clause".to_string(),
        ));
    }

    // Extract partition key from WHERE (must be in first/only group for key lookup)
    let pk_cond =
        find_comparison_in_groups(&wc.groups, &key_schema.partition_key).ok_or_else(|| {
            DynoxideError::ValidationException(
                "UPDATE WHERE must include partition key equality".to_string(),
            )
        })?;

    let pk_val = resolve_value(&pk_cond.value, parameters)?;
    let pk_str = pk_val
        .to_key_string()
        .ok_or_else(|| DynoxideError::ValidationException("Invalid key value".to_string()))?;

    let sk_str = if let Some(ref sk_name) = key_schema.sort_key {
        let sk_cond = find_comparison_in_groups(&wc.groups, sk_name);
        if sk_cond.is_none() {
            return Err(DynoxideError::ValidationException(
                "Where clause does not contain a mandatory equality on all key attributes"
                    .to_string(),
            ));
        }
        sk_cond
            .map(|c| resolve_value(&c.value, parameters))
            .transpose()?
            .and_then(|v| v.to_key_string())
            .unwrap_or_default()
    } else {
        String::new()
    };

    // Get existing item
    let existing_json = storage.get_item(table_name, &pk_str, &sk_str)?;
    let mut item: Item = existing_json
        .as_ref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    let old_item = item.clone();

    // Apply SET clauses with nested path support
    for clause in set_clauses {
        let val = resolve_set_value(&clause.value, &item, parameters)?;
        set_nested_value(&mut item, &clause.path, val)?;
    }

    // Apply REMOVE clauses
    for path in remove_paths {
        remove_nested_value(&mut item, path);
    }

    // Ensure keys are present
    if item.is_empty() {
        return Ok(());
    }

    // Validate attribute values after SET clauses applied
    crate::validation::validate_item_attribute_values(&item)?;
    crate::validation::normalize_item_sets(&mut item);

    let item_json = serde_json::to_string(&item)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let item_size = crate::types::item_size(&item);

    let hash_prefix = item
        .get(&key_schema.partition_key)
        .map(crate::storage::compute_hash_prefix)
        .unwrap_or_default();
    storage.put_item_with_hash(
        table_name,
        &pk_str,
        &sk_str,
        &item_json,
        item_size,
        &hash_prefix,
    )?;

    // GSI maintenance
    let table_sk_attr = key_schema.sort_key.as_deref();
    let _ = crate::actions::gsi::maintain_gsis_after_write(
        storage,
        table_name,
        &meta,
        &pk_str,
        &sk_str,
        &item,
        &key_schema.partition_key,
        table_sk_attr,
    )?;

    // LSI maintenance
    crate::actions::lsi::maintain_lsis_after_write(
        storage,
        table_name,
        &meta,
        &pk_str,
        &sk_str,
        &item,
        &key_schema.partition_key,
        table_sk_attr,
    )?;

    // Stream record
    let old_ref = if existing_json.is_some() {
        Some(&old_item)
    } else {
        None
    };
    crate::streams::record_stream_event(storage, &meta, old_ref, Some(&item))?;

    Ok(())
}

fn execute_delete(
    storage: &Storage,
    table_name: &str,
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
) -> Result<()> {
    let meta = require_table(storage, table_name)?;
    let key_schema = crate::actions::helpers::parse_key_schema(&meta)?;

    let wc = where_clause.ok_or_else(|| {
        DynoxideError::ValidationException("DELETE requires a WHERE clause".to_string())
    })?;

    // DynamoDB does not support OR in DELETE WHERE clauses
    if wc.groups.len() > 1 {
        return Err(DynoxideError::ValidationException(
            "DELETE does not support OR conditions in WHERE clause".to_string(),
        ));
    }

    let pk_cond =
        find_comparison_in_groups(&wc.groups, &key_schema.partition_key).ok_or_else(|| {
            DynoxideError::ValidationException(
                "DELETE WHERE must include partition key equality".to_string(),
            )
        })?;

    let pk_val = resolve_value(&pk_cond.value, parameters)?;
    let pk_str = pk_val
        .to_key_string()
        .ok_or_else(|| DynoxideError::ValidationException("Invalid key value".to_string()))?;

    // I15: Validate that the sort key is present in the WHERE clause if the table has one
    if let Some(ref sk_name) = key_schema.sort_key {
        let has_sk_condition = wc.groups.iter().any(|group| {
            group.iter().any(|c| match c {
                WhereCondition::Comparison(comp) => comp.path == *sk_name && comp.op == CompOp::Eq,
                _ => false,
            })
        });
        if !has_sk_condition {
            return Err(DynoxideError::ValidationException(
                "Where clause does not contain a mandatory equality on all key attributes"
                    .to_string(),
            ));
        }
    }

    let sk_str = if let Some(ref sk_name) = key_schema.sort_key {
        find_comparison_in_groups(&wc.groups, sk_name)
            .map(|c| resolve_value(&c.value, parameters))
            .transpose()?
            .and_then(|v| v.to_key_string())
            .unwrap_or_default()
    } else {
        String::new()
    };

    let old_json = storage.delete_item(table_name, &pk_str, &sk_str)?;

    // GSI maintenance
    let _ = crate::actions::gsi::maintain_gsis_after_delete(
        storage, table_name, &meta, &pk_str, &sk_str,
    )?;

    // LSI maintenance
    crate::actions::lsi::maintain_lsis_after_delete(storage, table_name, &meta, &pk_str, &sk_str)?;

    // Stream record
    let old_item: Option<Item> = old_json.as_ref().and_then(|j| serde_json::from_str(j).ok());
    if old_item.is_some() {
        crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), None)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn require_table(storage: &Storage, table_name: &str) -> Result<crate::storage::TableMetadata> {
    crate::actions::helpers::require_table(storage, table_name)
}

/// Find a comparison condition matching a given path with Eq operator,
/// searching across all OR groups.
fn find_comparison_in_groups<'a>(
    groups: &'a [Vec<WhereCondition>],
    path: &str,
) -> Option<&'a crate::partiql::parser::Condition> {
    for group in groups {
        if let Some(cond) = find_comparison(group, path) {
            return Some(cond);
        }
    }
    None
}

/// Find a comparison condition matching a given path with Eq operator.
fn find_comparison<'a>(
    conditions: &'a [WhereCondition],
    path: &str,
) -> Option<&'a crate::partiql::parser::Condition> {
    conditions.iter().find_map(|c| match c {
        WhereCondition::Comparison(cond) if cond.path == path && cond.op == CompOp::Eq => {
            Some(cond)
        }
        _ => None,
    })
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

/// Resolve a SetValue to a concrete AttributeValue, potentially using the current item.
fn resolve_set_value(
    val: &SetValue,
    item: &Item,
    parameters: &[AttributeValue],
) -> Result<AttributeValue> {
    match val {
        SetValue::Simple(pv) => resolve_value(pv, parameters),
        SetValue::Add(attr, pv) => {
            let current = resolve_nested_path(item, attr);
            let operand = resolve_value(pv, parameters)?;
            match (current, &operand) {
                (Some(AttributeValue::N(cur)), AttributeValue::N(add)) => {
                    use bigdecimal::BigDecimal;
                    use std::str::FromStr;
                    let a = BigDecimal::from_str(cur).map_err(|e| {
                        DynoxideError::ValidationException(format!("Invalid number: {e}"))
                    })?;
                    let b = BigDecimal::from_str(add).map_err(|e| {
                        DynoxideError::ValidationException(format!("Invalid number: {e}"))
                    })?;
                    let result = a + b;
                    Ok(AttributeValue::N(format_bigdecimal(&result)))
                }
                (None, AttributeValue::N(_)) => {
                    // Attribute doesn't exist yet — use the operand value
                    Ok(operand)
                }
                _ => Err(DynoxideError::ValidationException(
                    "SET expression add requires numeric attribute and operand".to_string(),
                )),
            }
        }
        SetValue::Sub(attr, pv) => {
            let current = resolve_nested_path(item, attr);
            let operand = resolve_value(pv, parameters)?;
            match (current, &operand) {
                (Some(AttributeValue::N(cur)), AttributeValue::N(sub)) => {
                    use bigdecimal::BigDecimal;
                    use std::str::FromStr;
                    let a = BigDecimal::from_str(cur).map_err(|e| {
                        DynoxideError::ValidationException(format!("Invalid number: {e}"))
                    })?;
                    let b = BigDecimal::from_str(sub).map_err(|e| {
                        DynoxideError::ValidationException(format!("Invalid number: {e}"))
                    })?;
                    let result = a - b;
                    Ok(AttributeValue::N(format_bigdecimal(&result)))
                }
                (None, AttributeValue::N(sub)) => {
                    // Attribute doesn't exist yet — treat as 0 - operand
                    use bigdecimal::BigDecimal;
                    use std::str::FromStr;
                    let b = BigDecimal::from_str(sub).map_err(|e| {
                        DynoxideError::ValidationException(format!("Invalid number: {e}"))
                    })?;
                    let result = -b;
                    Ok(AttributeValue::N(format_bigdecimal(&result)))
                }
                _ => Err(DynoxideError::ValidationException(
                    "SET expression subtract requires numeric attribute and operand".to_string(),
                )),
            }
        }
        SetValue::ListAppend(first, second) => {
            let a = resolve_value(first, parameters)?;
            let b = resolve_value(second, parameters)?;
            // At least one should be a list. If an attribute name was given,
            // resolve it from the item.
            let list_a = match &a {
                AttributeValue::S(name) => resolve_nested_path(item, name)
                    .cloned()
                    .unwrap_or(AttributeValue::L(Vec::new())),
                other => other.clone(),
            };
            let list_b = match &b {
                AttributeValue::S(name) => resolve_nested_path(item, name)
                    .cloned()
                    .unwrap_or(AttributeValue::L(Vec::new())),
                other => other.clone(),
            };
            match (list_a, list_b) {
                (AttributeValue::L(mut la), AttributeValue::L(lb)) => {
                    la.extend(lb);
                    Ok(AttributeValue::L(la))
                }
                _ => Err(DynoxideError::ValidationException(
                    "list_append requires list operands".to_string(),
                )),
            }
        }
    }
}

/// Set a value at a potentially nested path (e.g. `address.city`).
fn set_nested_value(item: &mut Item, path: &str, val: AttributeValue) -> Result<()> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 1 {
        item.insert(path.to_string(), val);
        return Ok(());
    }
    // Navigate into nested maps, creating them if needed
    let mut current = item;
    for part in &parts[..parts.len() - 1] {
        let entry = current
            .entry(part.to_string())
            .or_insert_with(|| AttributeValue::M(HashMap::new()));
        match entry {
            AttributeValue::M(map) => {
                current = map;
            }
            _ => {
                return Err(DynoxideError::ValidationException(
                    "The document path provided in the update expression is invalid for update"
                        .to_string(),
                ));
            }
        }
    }
    current.insert(parts.last().unwrap().to_string(), val);
    Ok(())
}

/// Remove a value at a potentially nested path (e.g. `address.city`).
fn remove_nested_value(item: &mut Item, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 1 {
        item.remove(path);
        return;
    }
    // Navigate into nested maps
    let mut current = item;
    for part in &parts[..parts.len() - 1] {
        match current.get_mut(*part) {
            Some(AttributeValue::M(map)) => {
                current = map;
            }
            _ => return, // Path doesn't exist or isn't a map — nothing to remove
        }
    }
    current.remove(*parts.last().unwrap());
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

    // OR semantics: any group matching is sufficient
    wc.groups
        .iter()
        .any(|group| matches_conditions(item, group, parameters))
}

/// Check if an item matches all conditions in a group (AND semantics).
fn matches_conditions(
    item: &Item,
    conditions: &[WhereCondition],
    parameters: &[AttributeValue],
) -> bool {
    for cond in conditions {
        match cond {
            WhereCondition::Comparison(c) => {
                let item_val = match resolve_nested_path(item, &c.path) {
                    Some(v) => v,
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
            WhereCondition::BeginsWith(path, prefix_val) => {
                let item_val = match resolve_nested_path(item, path) {
                    Some(v) => v,
                    None => return false,
                };
                let prefix = match resolve_value(prefix_val, parameters) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                match (item_val, &prefix) {
                    (AttributeValue::S(s), AttributeValue::S(p)) => {
                        if !s.starts_with(p.as_str()) {
                            return false;
                        }
                    }
                    _ => return false,
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
                let item_val = match resolve_nested_path(item, path) {
                    Some(v) => v,
                    None => return false,
                };
                let substr = match resolve_value(substr_val, parameters) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                match (item_val, &substr) {
                    (AttributeValue::S(s), AttributeValue::S(sub)) => {
                        if !s.contains(sub.as_str()) {
                            return false;
                        }
                    }
                    (AttributeValue::SS(set), AttributeValue::S(val)) => {
                        if !set.contains(val) {
                            return false;
                        }
                    }
                    (AttributeValue::NS(set), AttributeValue::N(val)) => {
                        if !set.contains(val) {
                            return false;
                        }
                    }
                    (AttributeValue::L(list), target) => {
                        if !list.contains(target) {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
        }
    }

    true
}

/// Resolve a dotted/indexed path to a nested attribute value.
///
/// Supports paths like `"a"`, `"a.b.c"`, and `"a[0].b"`.
fn resolve_nested_path<'a>(item: &'a Item, path: &str) -> Option<&'a AttributeValue> {
    // Fast path: no dots or brackets means a simple top-level lookup
    if !path.contains('.') && !path.contains('[') {
        return item.get(path);
    }

    let segments = split_path_segments(path)?;
    if segments.is_empty() {
        return None;
    }

    // First segment must be a map key on the top-level item
    let mut current = match &segments[0] {
        PathSegment::Key(k) => item.get(*k)?,
        PathSegment::Index(_) => return None,
    };

    for seg in &segments[1..] {
        current = match seg {
            PathSegment::Key(k) => match current {
                AttributeValue::M(map) => map.get(*k)?,
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

enum PathSegment<'a> {
    Key(&'a str),
    Index(usize),
}

/// Split a path like `"a.b[0].c"` into segments.
/// Returns None if the path contains malformed bracket expressions (e.g. `a[xyz]`).
fn split_path_segments(path: &str) -> Option<Vec<PathSegment<'_>>> {
    let mut segments = Vec::new();
    let bytes = path.as_bytes();
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                if start < i {
                    segments.push(PathSegment::Key(&path[start..i]));
                }
                i += 1;
                start = i;
            }
            b'[' => {
                if start < i {
                    segments.push(PathSegment::Key(&path[start..i]));
                }
                i += 1;
                let idx_start = i;
                while i < bytes.len() && bytes[i] != b']' {
                    i += 1;
                }
                let idx = path[idx_start..i].parse::<usize>().ok()?;
                segments.push(PathSegment::Index(idx));
                if i < bytes.len() {
                    i += 1; // skip ']'
                }
                start = i;
                // Skip a trailing dot after ']' (e.g. `a[0].b`)
                if i < bytes.len() && bytes[i] == b'.' {
                    i += 1;
                    start = i;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    if start < bytes.len() {
        segments.push(PathSegment::Key(&path[start..]));
    }

    Some(segments)
}

/// Compare two AttributeValues using a comparison operator.
fn compare_values(left: &AttributeValue, op: &CompOp, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::S(a), AttributeValue::S(b)) => compare_ord(a, op, b),
        (AttributeValue::N(a), AttributeValue::N(b)) => {
            use bigdecimal::BigDecimal;
            use std::str::FromStr;
            match (BigDecimal::from_str(a), BigDecimal::from_str(b)) {
                (Ok(da), Ok(db)) => compare_ord(&da, op, &db),
                _ => false,
            }
        }
        (AttributeValue::BOOL(a), AttributeValue::BOOL(b)) => match op {
            CompOp::Eq => a == b,
            CompOp::Ne => a != b,
            _ => false,
        },
        _ => match op {
            CompOp::Eq => false,
            CompOp::Ne => true,
            _ => false,
        },
    }
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

fn compare_ord<T: PartialOrd>(a: &T, op: &CompOp, b: &T) -> bool {
    match op {
        CompOp::Eq => a == b,
        CompOp::Ne => a != b,
        CompOp::Lt => a < b,
        CompOp::Le => a <= b,
        CompOp::Gt => a > b,
        CompOp::Ge => a >= b,
    }
}
