//! DynamoDB expression parsing and evaluation.
//!
//! Supports all five expression types:
//! - `ConditionExpression` / `FilterExpression`: conditional checks
//! - `KeyConditionExpression`: Query partition + sort key conditions
//! - `ProjectionExpression`: attribute subset selection
//! - `UpdateExpression`: item mutation (SET, REMOVE, ADD, DELETE)

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

/// The maximum byte length DynamoDB allows for a single expression parameter.
pub(crate) const MAX_EXPRESSION_BYTES: usize = 4096;

/// Reject an expression parameter longer than DynamoDB's 4096-byte limit,
/// measured on the raw string as sent: aliases are not substituted and
/// whitespace is not normalised before counting. Called at each expression
/// parser's entry so every surface (Update/Condition/Filter/KeyCondition/
/// Projection) is guarded.
///
/// The message ends at `size;`, with no figure: so it read on every surface
/// captured in us-east-1 and eu-west-2 on 2026-10-09 except the two that
/// [`check_expression_size_with_figure`] covers.
pub(crate) fn check_expression_size(expr: &str) -> Result<(), String> {
    if expr.len() > MAX_EXPRESSION_BYTES {
        return Err("Expression size has exceeded the maximum allowed size;".to_string());
    }
    Ok(())
}

/// The size check for the two surfaces where the message still carries the
/// figure in us-east-1, a Scan's FilterExpression and a transaction's
/// ConditionExpression: `...size; expression size: 4097`. eu-west-2 has
/// dropped the figure on both (captured 2026-10-09); Dynoxide follows
/// us-east-1 until the regions agree.
pub(crate) fn check_expression_size_with_figure(expr: &str) -> Result<(), String> {
    if expr.len() > MAX_EXPRESSION_BYTES {
        return Err(format!(
            "Expression size has exceeded the maximum allowed size; expression size: {}",
            expr.len()
        ));
    }
    Ok(())
}

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
            if let PathElement::Attribute(name) = elem
                && name.starts_with('#')
            {
                self.used_names.borrow_mut().insert(name.clone());
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
            update::SetOperand::Group(inner) => self.walk_set_value(inner),
        }
    }

    /// The first value reference, in name order, that the tracked expressions
    /// use but `ExpressionAttributeValues` doesn't define.
    pub(crate) fn first_undefined_value(&self) -> Option<String> {
        let used_values = self.used_values.borrow();
        let mut undefined: Vec<&String> = used_values
            .iter()
            .filter(|v| !self.values.as_ref().is_some_and(|map| map.contains_key(*v)))
            .collect();
        undefined.sort();
        undefined.first().map(|v| (*v).clone())
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
#[cfg(any(feature = "import", test))]
pub fn evaluate_without_tracking(
    expr: &condition::ConditionExpr,
    item: &HashMap<String, AttributeValue>,
    attr_names: &Option<HashMap<String, String>>,
    attr_values: &Option<HashMap<String, AttributeValue>>,
) -> Result<bool, String> {
    let tracker = TrackedExpressionAttributes::without_tracking(attr_names, attr_values);
    condition::evaluate(expr, item, &tracker)
}

/// The `ValidationException` text DynamoDB gives for a document path an update
/// cannot apply: a parent that is missing, or of the wrong type for the step
/// taken through it.
pub(crate) const INVALID_UPDATE_PATH: &str =
    "The document path provided in the update expression is invalid for update";

/// What an arithmetic SET or `list_append` says of an operand that names an
/// attribute the item does not have.
pub(crate) const MISSING_ATTRIBUTE: &str =
    "The provided expression refers to an attribute that does not exist in the item";

/// What an update says of an operand whose type does not fit the operation: a
/// stored value of the wrong type for `+`, `-` or `list_append`, or an ADD or
/// DELETE operand that does not fit the stored value.
pub(crate) const OPERAND_TYPE: &str =
    "An operand in the update expression has an incorrect data type";

/// Navigate a document path into an item, returning the attribute value at that path.
pub fn resolve_path(
    item: &HashMap<String, AttributeValue>,
    path: &[PathElement],
) -> Option<AttributeValue> {
    resolve_path_ref(item, path).cloned()
}

/// [`resolve_path`] without the copy.
fn resolve_path_ref<'a>(
    item: &'a HashMap<String, AttributeValue>,
    path: &[PathElement],
) -> Option<&'a AttributeValue> {
    let (first, rest) = path.split_first()?;
    let mut current = match first {
        PathElement::Attribute(name) => item.get(name)?,
        PathElement::Index(_) => return None,
    };
    for element in rest {
        current = match (element, current) {
            (PathElement::Attribute(name), AttributeValue::M(map)) => map.get(name)?,
            (PathElement::Index(i), AttributeValue::L(list)) => list.get(*i)?,
            _ => return None,
        };
    }
    Some(current)
}

/// The value at `path` for writing, or `None` where [`resolve_path`] would.
fn resolve_path_mut<'a>(
    item: &'a mut HashMap<String, AttributeValue>,
    path: &[PathElement],
) -> Option<&'a mut AttributeValue> {
    let (first, rest) = path.split_first()?;
    let mut current = match first {
        PathElement::Attribute(name) => item.get_mut(name)?,
        PathElement::Index(_) => return None,
    };
    for element in rest {
        current = match (element, current) {
            (PathElement::Attribute(name), AttributeValue::M(map)) => map.get_mut(name)?,
            (PathElement::Index(i), AttributeValue::L(list)) => list.get_mut(*i)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Whether the step into `path`'s last element can be taken: every parent
/// exists, and the one directly above the last element is a map where the last
/// element names a key, a list where it is an index. The last element itself
/// need not exist. An update refuses the others with [`INVALID_UPDATE_PATH`],
/// whether it sets, removes, adds to or deletes from the path (eu-west-2,
/// 18 October 2026).
pub(crate) fn check_update_parent(
    item: &HashMap<String, AttributeValue>,
    path: &[PathElement],
) -> Result<(), String> {
    let Some((last, parents)) = path.split_last() else {
        return Err("Empty path".to_string());
    };
    if parents.is_empty() {
        return match last {
            PathElement::Attribute(_) => Ok(()),
            PathElement::Index(_) => Err("Cannot index into top-level item".to_string()),
        };
    }
    match (resolve_path_ref(item, parents), last) {
        (Some(AttributeValue::M(_)), PathElement::Attribute(_))
        | (Some(AttributeValue::L(_)), PathElement::Index(_)) => Ok(()),
        _ => Err(INVALID_UPDATE_PATH.to_string()),
    }
}

/// Set a value at a document path. Intermediate maps and lists must already
/// exist; an index past the end of a list appends.
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
                return Err(INVALID_UPDATE_PATH.to_string());
            }
        };

        set_nested(entry, &path[1..], value)
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

    if path.len() == 1 {
        match &path[0] {
            PathElement::Attribute(name) => {
                if let AttributeValue::M(map) = current {
                    map.insert(name.clone(), value);
                    Ok(())
                } else {
                    Err(INVALID_UPDATE_PATH.to_string())
                }
            }
            PathElement::Index(i) => {
                // An index past the end of the list appends the value rather
                // than padding the gap (eu-west-2, 9 October 2026).
                if let AttributeValue::L(list) = current {
                    match list.get_mut(*i) {
                        Some(slot) => *slot = value,
                        None => list.push(value),
                    }
                    Ok(())
                } else {
                    Err(INVALID_UPDATE_PATH.to_string())
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
                        None => Err(INVALID_UPDATE_PATH.to_string()),
                    }
                } else {
                    Err(INVALID_UPDATE_PATH.to_string())
                }
            }
            PathElement::Index(i) => {
                // Stepping through an element past the end is refused, as is
                // stepping through anything that is not a map or a list.
                if let AttributeValue::L(list) = current
                    && let Some(next) = list.get_mut(*i)
                {
                    set_nested(next, &path[1..], value)
                } else {
                    Err(INVALID_UPDATE_PATH.to_string())
                }
            }
        }
    }
}

/// Remove a value at a document path. A last element that is not there is
/// left alone; a parent that is missing or the wrong type is refused, the same
/// as for [`set_path`].
pub fn remove_path(
    item: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
) -> Result<(), String> {
    check_update_parent(item, path)?;
    let (last, parents) = path.split_last().expect("checked non-empty");
    if parents.is_empty() {
        if let PathElement::Attribute(name) = last {
            item.remove(name);
        }
        return Ok(());
    }
    match (resolve_path_mut(item, parents), last) {
        (Some(AttributeValue::M(map)), PathElement::Attribute(name)) => {
            map.remove(name);
        }
        (Some(AttributeValue::L(list)), PathElement::Index(i)) if *i < list.len() => {
            list.remove(*i);
        }
        _ => {}
    }
    Ok(())
}

/// Element in a document path (e.g., `a.b[0].c`).
#[derive(Debug, Clone, PartialEq)]
pub enum PathElement {
    Attribute(String),
    Index(usize),
}

/// Format a resolved path for overlap error messages: `[a]`, `[a, b]`, `[a, [0]]`.
/// Shared by the ProjectionExpression and UpdateExpression overlap checks.
pub(crate) fn format_path_for_error(path: &[PathElement]) -> String {
    let parts: Vec<String> = path
        .iter()
        .map(|elem| match elem {
            PathElement::Attribute(name) => name.clone(),
            PathElement::Index(i) => format!("[{i}]"),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}
