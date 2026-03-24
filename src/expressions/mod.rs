//! DynamoDB expression parsing and evaluation.
//!
//! Supports all five expression types:
//! - `ConditionExpression` / `FilterExpression` — conditional checks
//! - `KeyConditionExpression` — Query partition + sort key conditions
//! - `ProjectionExpression` — attribute subset selection
//! - `UpdateExpression` — item mutation (SET, REMOVE, ADD, DELETE)

pub mod condition;
pub mod key_condition;
pub mod projection;
pub mod reserved;
pub mod tokenizer;
pub mod update;

use crate::errors::DynoxideError;
use crate::types::AttributeValue;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

/// Resolve an attribute name, handling `#name` substitution.
pub fn resolve_name(
    name: &str,
    attr_names: &Option<HashMap<String, String>>,
) -> Result<String, String> {
    if name.starts_with('#') {
        match attr_names {
            Some(map) => map.get(name).cloned().ok_or_else(|| {
                format!(
                    "Value provided in ExpressionAttributeNames unused in expressions: keys: {{{name}}}"
                )
            }),
            None => Err(format!(
                "An expression attribute name used in the document path is not defined; attribute name: {name}"
            )),
        }
    } else {
        Ok(name.to_string())
    }
}

/// Resolve an attribute value reference `:name`.
pub fn resolve_value<'a>(
    name: &str,
    attr_values: &'a Option<HashMap<String, AttributeValue>>,
) -> Result<&'a AttributeValue, String> {
    match attr_values {
        Some(map) => map.get(name).ok_or_else(|| {
            format!(
                "Value provided in ExpressionAttributeValues unused in expressions: keys: {{{name}}}"
            )
        }),
        None => Err(format!(
            "An expression attribute value used in expression is not defined; attribute value: {name}"
        )),
    }
}

/// Wrapper around expression attribute names/values that tracks which entries are used.
///
/// Use `RefCell` for interior mutability so it can be passed as `&TrackedExpressionAttributes`
/// (not `&mut`) through all expression evaluation functions.
pub struct TrackedExpressionAttributes<'a> {
    pub names: &'a Option<HashMap<String, String>>,
    pub values: &'a Option<HashMap<String, AttributeValue>>,
    used_names: RefCell<HashSet<String>>,
    used_values: RefCell<HashSet<String>>,
    /// When false, resolve_name/resolve_value skip HashSet insertions.
    /// Used in the per-item hot loop where tracking has already been done pre-loop.
    tracking_enabled: bool,
}

impl<'a> TrackedExpressionAttributes<'a> {
    pub fn new(
        names: &'a Option<HashMap<String, String>>,
        values: &'a Option<HashMap<String, AttributeValue>>,
    ) -> Self {
        Self {
            names,
            values,
            used_names: RefCell::new(HashSet::new()),
            used_values: RefCell::new(HashSet::new()),
            tracking_enabled: true,
        }
    }

    /// Create a variant that skips tracking. Name/value resolution still works
    /// but HashSet insertions are skipped. Use in hot loops where tracking has
    /// already been done by `track_condition_expr` pre-loop.
    pub fn without_tracking(
        names: &'a Option<HashMap<String, String>>,
        values: &'a Option<HashMap<String, AttributeValue>>,
    ) -> Self {
        Self {
            names,
            values,
            used_names: RefCell::new(HashSet::new()),
            used_values: RefCell::new(HashSet::new()),
            tracking_enabled: false,
        }
    }

    /// Resolve an attribute name, handling `#name` substitution, and track usage.
    pub fn resolve_name(&self, name: &str) -> Result<String, String> {
        if name.starts_with('#') {
            if self.tracking_enabled {
                self.used_names.borrow_mut().insert(name.to_string());
            }
            match self.names {
                Some(map) => map.get(name).cloned().ok_or_else(|| {
                    format!(
                        "An expression attribute name used in the document path is not defined; attribute name: {name}"
                    )
                }),
                None => Err(format!(
                    "An expression attribute name used in the document path is not defined; attribute name: {name}"
                )),
            }
        } else {
            Ok(name.to_string())
        }
    }

    /// Resolve an attribute value reference `:name` and track usage.
    pub fn resolve_value<'b>(&'b self, name: &str) -> Result<&'a AttributeValue, String> {
        if self.tracking_enabled {
            self.used_values.borrow_mut().insert(name.to_string());
        }
        match self.values {
            Some(map) => map.get(name).ok_or_else(|| {
                format!(
                    "An expression attribute value used in expression is not defined; attribute value: {name}"
                )
            }),
            None => Err(format!(
                "An expression attribute value used in expression is not defined; attribute value: {name}"
            )),
        }
    }

    /// Pre-register all `#name` and `:value` references found in a parsed condition expression.
    /// This ensures they are tracked even if the expression is never evaluated (e.g., no items).
    pub fn track_condition_expr(&self, expr: &condition::ConditionExpr) {
        self.walk_condition(expr);
    }

    fn walk_condition(&self, expr: &condition::ConditionExpr) {
        match expr {
            condition::ConditionExpr::Comparison { left, op: _, right } => {
                self.walk_operand(left);
                self.walk_operand(right);
            }
            condition::ConditionExpr::Between { operand, lo, hi } => {
                self.walk_operand(operand);
                self.walk_operand(lo);
                self.walk_operand(hi);
            }
            condition::ConditionExpr::In { operand, values } => {
                self.walk_operand(operand);
                for v in values {
                    self.walk_operand(v);
                }
            }
            condition::ConditionExpr::AttributeExists(path)
            | condition::ConditionExpr::AttributeNotExists(path) => {
                self.walk_path_elements(path);
            }
            condition::ConditionExpr::AttributeType(path, op) => {
                self.walk_path_elements(path);
                self.walk_operand(op);
            }
            condition::ConditionExpr::BeginsWith(a, b)
            | condition::ConditionExpr::Contains(a, b) => {
                self.walk_operand(a);
                self.walk_operand(b);
            }
            condition::ConditionExpr::And(l, r) | condition::ConditionExpr::Or(l, r) => {
                self.walk_condition(l);
                self.walk_condition(r);
            }
            condition::ConditionExpr::Not(inner) => {
                self.walk_condition(inner);
            }
        }
    }

    fn walk_operand(&self, operand: &condition::Operand) {
        match operand {
            condition::Operand::Path(path) | condition::Operand::Size(path) => {
                self.walk_path_elements(path);
            }
            condition::Operand::ValueRef(name) => {
                self.used_values.borrow_mut().insert(name.clone());
            }
        }
    }

    fn walk_path_elements(&self, path: &[PathElement]) {
        for elem in path {
            if let PathElement::Attribute(name) = elem {
                if name.starts_with('#') {
                    self.used_names.borrow_mut().insert(name.clone());
                }
            }
        }
    }

    /// Pre-register all `#name` references found in a parsed projection expression.
    pub fn track_projection_expr(&self, proj: &projection::ProjectionExpr) {
        for path in &proj.paths {
            self.walk_path_elements(path);
        }
    }

    /// Pre-register all `#name` and `:value` references found in a parsed update expression.
    pub fn track_update_expr(&self, expr: &update::UpdateExpr) {
        for action in &expr.set_actions {
            self.walk_path_elements(&action.path);
            self.walk_set_value(&action.value);
        }
        for path in &expr.remove_actions {
            self.walk_path_elements(path);
        }
        for action in &expr.add_actions {
            self.walk_path_elements(&action.path);
            self.used_values
                .borrow_mut()
                .insert(action.value_ref.clone());
        }
        for action in &expr.delete_actions {
            self.walk_path_elements(&action.path);
            self.used_values
                .borrow_mut()
                .insert(action.value_ref.clone());
        }
    }

    fn walk_set_value(&self, value: &update::SetValue) {
        match value {
            update::SetValue::Operand(op) => self.walk_set_operand(op),
            update::SetValue::Plus(l, r) | update::SetValue::Minus(l, r) => {
                self.walk_set_operand(l);
                self.walk_set_operand(r);
            }
        }
    }

    fn walk_set_operand(&self, operand: &update::SetOperand) {
        match operand {
            update::SetOperand::Path(path) => self.walk_path_elements(path),
            update::SetOperand::ValueRef(name) => {
                self.used_values.borrow_mut().insert(name.clone());
            }
            update::SetOperand::IfNotExists(path, default) => {
                self.walk_path_elements(path);
                self.walk_set_operand(default);
            }
            update::SetOperand::ListAppend(a, b) => {
                self.walk_set_operand(a);
                self.walk_set_operand(b);
            }
        }
    }

    /// Pre-register all `#name` and `:value` references in a parsed key condition.
    /// Note: key_condition::parse already resolves names, so we track the original
    /// value refs that will be resolved later via resolve_values.
    pub fn track_key_condition(&self, cond: &key_condition::KeyCondition) {
        self.used_values
            .borrow_mut()
            .insert(cond.pk_value_ref.clone());
        if let Some(ref sk) = cond.sk_condition {
            match sk {
                key_condition::SortKeyCondition::Eq(_, vr)
                | key_condition::SortKeyCondition::Lt(_, vr)
                | key_condition::SortKeyCondition::Le(_, vr)
                | key_condition::SortKeyCondition::Gt(_, vr)
                | key_condition::SortKeyCondition::Ge(_, vr)
                | key_condition::SortKeyCondition::BeginsWith(_, vr) => {
                    self.used_values.borrow_mut().insert(vr.clone());
                }
                key_condition::SortKeyCondition::Between(_, lo, hi) => {
                    self.used_values.borrow_mut().insert(lo.clone());
                    self.used_values.borrow_mut().insert(hi.clone());
                }
            }
        }
    }

    /// Check for unused names/values. Returns an error listing all unused keys.
    pub fn check_unused(&self) -> Result<(), DynoxideError> {
        let used_names = self.used_names.borrow();
        let used_values = self.used_values.borrow();

        if let Some(names_map) = self.names {
            let unused: Vec<&String> = names_map
                .keys()
                .filter(|k| !used_names.contains(*k))
                .collect();
            if !unused.is_empty() {
                let mut keys: Vec<&str> = unused.iter().map(|s| s.as_str()).collect();
                keys.sort();
                return Err(DynoxideError::ValidationException(format!(
                    "Value provided in ExpressionAttributeNames unused in expressions: keys: {{{}}}",
                    keys.join(", ")
                )));
            }
        }

        if let Some(values_map) = self.values {
            let unused: Vec<&String> = values_map
                .keys()
                .filter(|k| !used_values.contains(*k))
                .collect();
            if !unused.is_empty() {
                let mut keys: Vec<&str> = unused.iter().map(|s| s.as_str()).collect();
                keys.sort();
                return Err(DynoxideError::ValidationException(format!(
                    "Value provided in ExpressionAttributeValues unused in expressions: keys: {{{}}}",
                    keys.join(", ")
                )));
            }
        }

        Ok(())
    }
}

/// Resolve `#name` references in path elements, tracking usage via a `TrackedExpressionAttributes`.
///
/// This is the single implementation used by condition, projection, and update modules.
pub fn resolve_path_elements(
    path: &[PathElement],
    tracker: &TrackedExpressionAttributes,
) -> Result<Vec<PathElement>, String> {
    path.iter()
        .map(|elem| match elem {
            PathElement::Attribute(name) if name.starts_with('#') => {
                let resolved = tracker.resolve_name(name)?;
                Ok(PathElement::Attribute(resolved))
            }
            other => Ok(other.clone()),
        })
        .collect()
}

/// Evaluate a condition expression without tracking attribute usage.
///
/// This is a convenience wrapper for callers (e.g., import filters) that don't need
/// unused-attribute validation. Uses the no-tracking variant to avoid RefCell/HashSet overhead.
pub fn evaluate_without_tracking(
    expr: &condition::ConditionExpr,
    item: &HashMap<String, AttributeValue>,
    attr_names: &Option<HashMap<String, String>>,
    attr_values: &Option<HashMap<String, AttributeValue>>,
) -> Result<bool, String> {
    let tracker = TrackedExpressionAttributes::without_tracking(attr_names, attr_values);
    condition::evaluate(expr, item, &tracker)
}

/// Navigate a document path into an item, returning the attribute value at that path.
pub fn resolve_path(
    item: &HashMap<String, AttributeValue>,
    path: &[PathElement],
) -> Option<AttributeValue> {
    if path.is_empty() {
        return None;
    }

    let first = match &path[0] {
        PathElement::Attribute(name) => item.get(name)?,
        PathElement::Index(_) => return None,
    };

    let mut current = first.clone();
    for element in &path[1..] {
        match element {
            PathElement::Attribute(name) => {
                if let AttributeValue::M(map) = &current {
                    current = map.get(name)?.clone();
                } else {
                    return None;
                }
            }
            PathElement::Index(i) => {
                if let AttributeValue::L(list) = &current {
                    current = list.get(*i)?.clone();
                } else {
                    return None;
                }
            }
        }
    }

    Some(current)
}

/// Set a value at a document path, creating intermediate maps/lists as needed.
/// Returns the modified item.
pub fn set_path(
    item: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
    value: AttributeValue,
) -> Result<(), String> {
    if path.is_empty() {
        return Err("Empty path".to_string());
    }

    if path.len() == 1 {
        match &path[0] {
            PathElement::Attribute(name) => {
                item.insert(name.clone(), value);
                Ok(())
            }
            PathElement::Index(_) => Err("Cannot index into top-level item".to_string()),
        }
    } else {
        let first_name = match &path[0] {
            PathElement::Attribute(name) => name.clone(),
            PathElement::Index(_) => return Err("Cannot index into top-level item".to_string()),
        };

        // DynamoDB does NOT auto-create top-level attributes for nested paths.
        // SET missing.nested = :v fails if "missing" doesn't exist on the item.
        // Only SET topLevel = :v (path.len() == 1, handled above) creates new attributes.
        let entry = match item.get_mut(&first_name) {
            Some(e) => e,
            None => {
                return Err(
                    "The document path provided in the update expression is invalid for update"
                        .to_string(),
                );
            }
        };

        set_nested(entry, &path[1..], value)
    }
}

/// Extend a list with NULL padding so that `list[target_len - 1]` is valid.
fn pad_list_to(list: &mut Vec<AttributeValue>, target_len: usize) {
    while list.len() < target_len {
        list.push(AttributeValue::NULL(true));
    }
}

fn set_nested(
    current: &mut AttributeValue,
    path: &[PathElement],
    value: AttributeValue,
) -> Result<(), String> {
    if path.is_empty() {
        return Err("Empty remaining path".to_string());
    }

    // Auto-promote NULL to the structure type needed by the next path element.
    // This handles the case where list padding created NULL placeholders that
    // need to become Maps or Lists for deeper path navigation.
    if matches!(current, AttributeValue::NULL(_)) {
        match &path[0] {
            PathElement::Attribute(_) => {
                *current = AttributeValue::M(HashMap::new());
            }
            PathElement::Index(_) => {
                *current = AttributeValue::L(Vec::new());
            }
        }
    }

    if path.len() == 1 {
        match &path[0] {
            PathElement::Attribute(name) => {
                if let AttributeValue::M(map) = current {
                    map.insert(name.clone(), value);
                    Ok(())
                } else {
                    Err(
                        "The document path provided in the update expression is invalid for update"
                            .to_string(),
                    )
                }
            }
            PathElement::Index(i) => {
                if let AttributeValue::L(list) = current {
                    pad_list_to(list, *i + 1);
                    list[*i] = value;
                    Ok(())
                } else {
                    Err(
                        "The document path provided in the update expression is invalid for update"
                            .to_string(),
                    )
                }
            }
        }
    } else {
        match &path[0] {
            PathElement::Attribute(name) => {
                if let AttributeValue::M(map) = current {
                    // DynamoDB does NOT auto-create intermediate map entries.
                    // The key must already exist to navigate deeper.
                    match map.get_mut(name) {
                        Some(entry) => set_nested(entry, &path[1..], value),
                        None => Err(
                            "The document path provided in the update expression is invalid for update"
                                .to_string(),
                        ),
                    }
                } else {
                    Err(
                        "The document path provided in the update expression is invalid for update"
                            .to_string(),
                    )
                }
            }
            PathElement::Index(i) => {
                if let AttributeValue::L(list) = current {
                    pad_list_to(list, *i + 1);
                    set_nested(&mut list[*i], &path[1..], value)
                } else {
                    Err(
                        "The document path provided in the update expression is invalid for update"
                            .to_string(),
                    )
                }
            }
        }
    }
}

/// Remove a value at a document path.
pub fn remove_path(
    item: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
) -> Result<(), String> {
    if path.is_empty() {
        return Err("Empty path".to_string());
    }

    if path.len() == 1 {
        match &path[0] {
            PathElement::Attribute(name) => {
                item.remove(name);
                Ok(())
            }
            PathElement::Index(_) => Err("Cannot index into top-level item".to_string()),
        }
    } else {
        let first_name = match &path[0] {
            PathElement::Attribute(name) => name.clone(),
            PathElement::Index(_) => return Err("Cannot index into top-level item".to_string()),
        };

        if let Some(entry) = item.get_mut(&first_name) {
            remove_nested(entry, &path[1..])
        } else {
            Ok(()) // Path doesn't exist, nothing to remove
        }
    }
}

fn remove_nested(current: &mut AttributeValue, path: &[PathElement]) -> Result<(), String> {
    if path.is_empty() {
        return Err("Empty remaining path".to_string());
    }

    if path.len() == 1 {
        match &path[0] {
            PathElement::Attribute(name) => {
                if let AttributeValue::M(map) = current {
                    map.remove(name);
                    Ok(())
                } else {
                    Ok(()) // Not a map, nothing to remove
                }
            }
            PathElement::Index(i) => {
                if let AttributeValue::L(list) = current {
                    if *i < list.len() {
                        list.remove(*i);
                    }
                    Ok(())
                } else {
                    Ok(()) // Not a list, nothing to remove
                }
            }
        }
    } else {
        match &path[0] {
            PathElement::Attribute(name) => {
                if let AttributeValue::M(map) = current {
                    if let Some(entry) = map.get_mut(name) {
                        remove_nested(entry, &path[1..])
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            }
            PathElement::Index(i) => {
                if let AttributeValue::L(list) = current {
                    if let Some(entry) = list.get_mut(*i) {
                        remove_nested(entry, &path[1..])
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Element in a document path (e.g., `a.b[0].c`).
#[derive(Debug, Clone, PartialEq)]
pub enum PathElement {
    Attribute(String),
    Index(usize),
}
