//! KeyConditionExpression parsing.
//!
//! KeyConditionExpression supports: `pk = :val [AND sk_condition]`
//! Sort key conditions: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN ... AND ...`, `begins_with(sk, :prefix)`

use crate::expressions::condition::parse_raw_path;
use crate::expressions::tokenizer::{
    Token, TokenSpan, TokenStream, check_redundant_parens, tokenize, tokenizer_syntax_error,
};
use crate::expressions::{PathElement, TrackedExpressionAttributes};
use crate::types::AttributeValue;

/// Parsed key condition: every clause, in the order written. Which clause
/// binds to which key attribute is decided against the key schema by [`bind`],
/// since a global secondary index key can have up to four partition key and
/// four sort key attributes.
#[derive(Debug)]
pub struct KeyCondition {
    pub clauses: Vec<SortKeyCondition>,
}

/// One clause of a key condition. Each variant names the attribute it
/// constrains first; on a single-attribute key that is the partition or sort
/// key.
#[derive(Debug)]
pub enum SortKeyCondition {
    Eq(String, String), // (attr_name, value_ref)
    Lt(String, String),
    Le(String, String),
    Gt(String, String),
    Ge(String, String),
    Between(String, String, String), // (attr_name, lo_ref, hi_ref)
    BeginsWith(String, String),      // (attr_name, prefix_ref)
}

impl SortKeyCondition {
    /// The attribute this clause constrains.
    pub fn attr_name(&self) -> &str {
        match self {
            Self::Eq(n, _)
            | Self::Lt(n, _)
            | Self::Le(n, _)
            | Self::Gt(n, _)
            | Self::Ge(n, _)
            | Self::Between(n, _, _)
            | Self::BeginsWith(n, _) => n,
        }
    }
}

/// Parse a KeyConditionExpression string, tracking attribute name usage.
///
/// Supports optional parentheses around individual conditions and around
/// the entire expression, matching DynamoDB behavior. Everything checked here
/// needs no key schema, so it runs before the table is looked up; the clause
/// count and which attributes the clauses name are checked by [`bind`].
pub fn parse(expr: &str, tracker: &TrackedExpressionAttributes) -> Result<KeyCondition, String> {
    // Prefix the size error like every other KeyConditionExpression error; real
    // DynamoDB checks size before parsing and returns "Invalid
    // KeyConditionExpression: Expression size has exceeded the maximum allowed
    // size", even for an otherwise malformed key condition. Confirmed against
    // real DynamoDB (eu-west-2).
    super::check_expression_size(expr)
        .map_err(|e| format!("Invalid KeyConditionExpression: {e}"))?;
    let tokens = tokenize(expr).map_err(|e| {
        format!(
            "Invalid KeyConditionExpression: {}",
            tokenizer_syntax_error(expr, &e)
        )
    })?;
    // Reject redundant parens before stripping outer ones (strip_outer_parens
    // would otherwise silently accept `((pk = :pk))`).
    check_redundant_parens(&tokens).map_err(|e| format!("Invalid KeyConditionExpression: {e}"))?;
    let tokens = strip_outer_parens(tokens);
    let mut stream = TokenStream::new(tokens);

    // Every AND clause is parsed, however many there are: a multi-attribute
    // index key takes up to eight.
    let mut conds = vec![parse_single_condition(&mut stream, tracker)?];
    while matches!(stream.peek(), Some(Token::And)) {
        stream.next();
        conds.push(parse_single_condition(&mut stream, tracker)?);
    }

    if !stream.at_end() {
        return Err(format!(
            "Unexpected token in KeyConditionExpression: {}",
            stream.peek().unwrap()
        ));
    }

    // Operand types come after the whole expression has parsed and before
    // anything about which key each condition names.
    let written: Vec<(&ParsedCond, &str)> = conds.iter().map(|(c, op)| (c, *op)).collect();
    check_operand_types(&written, tracker)?;

    // A second condition on the same attribute is refused before the clauses
    // are counted against the key: `pk = :p AND sk > :a AND sk < :b` reports
    // this rather than three conditions for a two-attribute key (captured
    // eu-west-2, October 2026).
    let mut named: Vec<&str> = Vec::new();
    for (cond, _) in &conds {
        let name = cond.attr_name();
        if named.contains(&name) {
            return Err(
                "Invalid KeyConditionExpression: KeyConditionExpressions must only contain one condition per key"
                    .to_string(),
            );
        }
        named.push(name);
    }

    Ok(KeyCondition {
        clauses: conds.into_iter().map(|(c, _)| c.into_clause()).collect(),
    })
}

/// The key a condition is checked against: its partition key attributes and
/// its sort key attributes, each in key-schema order. A table or an LSI has one
/// of each at most; a GSI can have up to four of each.
#[derive(Debug, Clone, Copy)]
pub struct KeyAttributes<'a> {
    pub hash: &'a [String],
    pub range: &'a [String],
}

/// A key condition bound to a key schema, with its values resolved.
#[derive(Debug)]
pub struct BoundKeyCondition {
    /// One value per partition key attribute, in key-schema order.
    pub hash: Vec<(String, AttributeValue)>,
    /// Equalities on the leading sort key attributes, in key-schema order.
    pub range_eq: Vec<(String, AttributeValue)>,
    /// A non-equality condition on the sort key attribute after `range_eq`.
    pub range_last: Option<ResolvedSortKeyCondition>,
}

impl BoundKeyCondition {
    /// Every attribute the condition compares, with each value it is compared
    /// against.
    pub fn compared_values(&self) -> Vec<(&str, &AttributeValue)> {
        let mut out: Vec<(&str, &AttributeValue)> = self
            .hash
            .iter()
            .chain(&self.range_eq)
            .map(|(n, v)| (n.as_str(), v))
            .collect();
        if let Some(last) = &self.range_last {
            out.extend(last.values().into_iter().map(|v| (last.sk_name(), v)));
        }
        out
    }

    /// The partition key string the rows are stored under, or `None` when a
    /// value has no key form.
    pub fn partition_key_string(&self) -> Option<String> {
        let values: Vec<&AttributeValue> = self.hash.iter().map(|(_, v)| v).collect();
        crate::actions::gsi::composite_key_string(&values)
    }

    /// The sort key conditions as SQL over the `sk` column with their
    /// parameters, for a key whose sort key has `range_len` attributes.
    pub fn sort_key_sql(&self, range_len: usize) -> (Option<String>, Vec<String>) {
        pairs_to_sql(&self.sort_key_bounds(range_len))
    }

    fn sort_key_bounds(&self, range_len: usize) -> Vec<(String, String)> {
        if range_len <= 1 {
            if let Some((_, v)) = self.range_eq.first() {
                return vec![("=".into(), val_to_key_string(v))];
            }
            return self
                .range_last
                .as_ref()
                .map(|c| c.to_sql_conditions())
                .unwrap_or_default();
        }
        multi_attribute_sort_key_bounds(&self.range_eq, self.range_last.as_ref(), range_len)
    }
}

/// Bound a multi-attribute sort key.
///
/// Rows are stored under [`crate::actions::gsi::composite_key_string`] of their
/// sort key attributes, so the equality prefix is a run of closed parts, and a
/// condition on the next attribute becomes a range over the parts that follow
/// it. Every bound is on whole parts, so a value is never confused with a
/// longer one it is a prefix of.
fn multi_attribute_sort_key_bounds(
    range_eq: &[(String, AttributeValue)],
    range_last: Option<&ResolvedSortKeyCondition>,
    range_len: usize,
) -> Vec<(String, String)> {
    use crate::actions::gsi::{push_escaped_key, push_key_part};

    let mut prefix = String::new();
    for (_, v) in range_eq {
        push_key_part(&mut prefix, &val_to_key_string(v));
    }
    let closed = |v: &AttributeValue| {
        let mut s = prefix.clone();
        push_key_part(&mut s, &val_to_key_string(v));
        s
    };
    let mut bounds: Vec<(String, String)> = Vec::new();
    let mut push = |op: &str, value: Option<String>| {
        if let Some(value) = value {
            bounds.push((op.to_string(), value));
        }
    };
    let in_group_from = (!range_eq.is_empty()).then(|| prefix.clone());
    let in_group_to = if range_eq.is_empty() {
        None
    } else {
        prefix_upper_bound(&prefix)
    };

    match range_last {
        None if range_eq.len() == range_len => push("=", Some(prefix.clone())),
        None => {
            push(">=", in_group_from);
            push("<", in_group_to);
        }
        Some(ResolvedSortKeyCondition::Eq(_, v)) => {
            let part = closed(v);
            push("<", prefix_upper_bound(&part));
            push(">=", Some(part));
        }
        Some(ResolvedSortKeyCondition::Gt(_, v)) => {
            push(">=", prefix_upper_bound(&closed(v)));
            push("<", in_group_to);
        }
        Some(ResolvedSortKeyCondition::Ge(_, v)) => {
            push(">=", Some(closed(v)));
            push("<", in_group_to);
        }
        Some(ResolvedSortKeyCondition::Lt(_, v)) => {
            push(">=", in_group_from);
            push("<", Some(closed(v)));
        }
        Some(ResolvedSortKeyCondition::Le(_, v)) => {
            push(">=", in_group_from);
            push("<", prefix_upper_bound(&closed(v)));
        }
        Some(ResolvedSortKeyCondition::Between(_, lo, hi)) => {
            push(">=", Some(closed(lo)));
            push("<", prefix_upper_bound(&closed(hi)));
        }
        Some(ResolvedSortKeyCondition::BeginsWith(_, p)) => {
            let mut start = prefix.clone();
            push_escaped_key(&mut start, &val_to_key_string(p));
            push("<", prefix_upper_bound(&start));
            push(">=", Some(start));
        }
    }
    bounds
}

/// The smallest string greater than every string that starts with `prefix`,
/// or `None` when no such string exists (a prefix made only of `char::MAX`).
pub(crate) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let mut next = last as u32 + 1;
        if (0xD800..=0xDFFF).contains(&next) {
            next = 0xE000;
        }
        if let Some(next) = char::from_u32(next) {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// Bind a parsed key condition to a key schema and resolve its values.
///
/// The checks run in this order: the clause count against the key's attribute
/// count; an equality on every partition key attribute; the values, in the
/// order written; a clause on an attribute outside the key; and, on a sort key
/// with more than one attribute, equalities on every attribute ahead of the
/// last one constrained. Every message is DynamoDB's: the count captured
/// eu-west-2, 2026-10-08; the partition key and sort key rules eu-central-1,
/// us-east-1 and eu-west-2, October 2026, on single- and multi-attribute keys.
pub fn bind(
    condition: &KeyCondition,
    keys: KeyAttributes<'_>,
    tracker: &TrackedExpressionAttributes,
) -> Result<BoundKeyCondition, String> {
    let given = condition.clauses.len();
    let defined = keys.hash.len() + keys.range.len();
    if given > defined {
        return Err(format!(
            "The number of query conditions ({given}) exceeds the number of key attributes \
             defined in the schema ({defined})"
        ));
    }

    let clause_for = |attr: &str| condition.clauses.iter().find(|c| c.attr_name() == attr);
    let is_eq = |attr: &str| matches!(clause_for(attr), Some(SortKeyCondition::Eq(..)));
    if keys.hash.len() > 1 || keys.range.len() > 1 {
        // A multi-attribute key names every partition key attribute without an
        // equality, even when its partition key has just one.
        let missing: Vec<&str> = keys
            .hash
            .iter()
            .filter(|a| !is_eq(a))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "HASH key attributes {} must have equality conditions specified in the query",
                missing.join(", ")
            ));
        }
    } else {
        for attr in keys.hash {
            match clause_for(attr) {
                Some(SortKeyCondition::Eq(..)) => {}
                Some(_) => return Err("Query key condition not supported".to_string()),
                None => return Err(format!("Query condition missed key schema element: {attr}")),
            }
        }
    }

    // Values resolve in the order written, so the first undefined one is the
    // one reported.
    let mut resolved = Vec::with_capacity(condition.clauses.len());
    for clause in &condition.clauses {
        resolved.push(resolve_sk_condition(clause, tracker)?);
    }
    let resolved_for = |attr: &str| resolved.iter().find(|c| c.sk_name() == attr);

    let in_key = |attr: &str| keys.hash.iter().chain(keys.range).any(|k| k == attr);
    if condition.clauses.iter().any(|c| !in_key(c.attr_name())) {
        // With every partition key attribute matched and no more clauses than
        // key attributes, a stray clause leaves a sort key attribute without
        // one.
        let unmatched = keys
            .range
            .iter()
            .find(|a| clause_for(a).is_none())
            .map(String::as_str)
            .unwrap_or_default();
        return Err(format!(
            "Query condition missed key schema element: {unmatched}"
        ));
    }

    let mut range_eq = Vec::new();
    let mut range_last = None;
    if let Some(last) = keys.range.iter().rposition(|a| clause_for(a).is_some()) {
        let missing: Vec<&str> = keys.range[..last]
            .iter()
            .filter(|a| !matches!(clause_for(a), Some(SortKeyCondition::Eq(..))))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "RANGE key attributes {} must have equality conditions specified in the query \
                 because a condition is present on key attribute {}",
                missing.join(", "),
                keys.range[last]
            ));
        }
        for attr in &keys.range[..=last] {
            match resolved_for(attr) {
                Some(ResolvedSortKeyCondition::Eq(_, v)) => {
                    range_eq.push((attr.clone(), v.clone()))
                }
                Some(other) => range_last = Some(other.clone()),
                None => {}
            }
        }
    }

    let hash = keys
        .hash
        .iter()
        .filter_map(|attr| match resolved_for(attr) {
            Some(ResolvedSortKeyCondition::Eq(_, v)) => Some((attr.clone(), v.clone())),
            _ => None,
        })
        .collect();

    Ok(BoundKeyCondition {
        hash,
        range_eq,
        range_last,
    })
}

#[derive(Debug, Clone)]
pub enum ResolvedSortKeyCondition {
    Eq(String, AttributeValue),
    Lt(String, AttributeValue),
    Le(String, AttributeValue),
    Gt(String, AttributeValue),
    Ge(String, AttributeValue),
    Between(String, AttributeValue, AttributeValue),
    BeginsWith(String, AttributeValue),
}

impl ResolvedSortKeyCondition {
    pub fn sk_name(&self) -> &str {
        match self {
            Self::Eq(n, _)
            | Self::Lt(n, _)
            | Self::Le(n, _)
            | Self::Gt(n, _)
            | Self::Ge(n, _)
            | Self::Between(n, _, _)
            | Self::BeginsWith(n, _) => n,
        }
    }

    /// The values this condition compares against.
    pub fn values(&self) -> Vec<&AttributeValue> {
        match self {
            Self::Eq(_, v)
            | Self::Lt(_, v)
            | Self::Le(_, v)
            | Self::Gt(_, v)
            | Self::Ge(_, v)
            | Self::BeginsWith(_, v) => vec![v],
            Self::Between(_, lo, hi) => vec![lo, hi],
        }
    }

    /// The bounds this condition puts on the stored sort key, which all hold.
    /// BETWEEN gives two, and so does begins_with unless its prefix has no
    /// upper bound.
    pub fn bounds(&self) -> Vec<SortKeyBound> {
        match self {
            Self::Eq(_, v) => vec![SortKeyBound::Eq(val_to_key_string(v))],
            Self::Lt(_, v) => vec![SortKeyBound::Lt(val_to_key_string(v))],
            Self::Le(_, v) => vec![SortKeyBound::Le(val_to_key_string(v))],
            Self::Gt(_, v) => vec![SortKeyBound::Gt(val_to_key_string(v))],
            Self::Ge(_, v) => vec![SortKeyBound::Ge(val_to_key_string(v))],
            Self::Between(_, lo, hi) => vec![
                SortKeyBound::Ge(val_to_key_string(lo)),
                SortKeyBound::Le(val_to_key_string(hi)),
            ],
            // A range rather than LIKE: SQLite's LIKE folds ASCII case, and
            // DynamoDB compares key bytes exactly, so `begins_with(sk, "abc")`
            // must not match "ABCdef".
            Self::BeginsWith(_, prefix) => {
                let start = val_to_key_string(prefix);
                let mut out = Vec::new();
                if let Some(end) = prefix_upper_bound(&start) {
                    out.push(SortKeyBound::Lt(end));
                }
                out.push(SortKeyBound::Ge(start));
                out
            }
        }
    }

    /// Convert to SQL WHERE clause components for sk column.
    /// Returns (operator, value_string) pairs.
    /// For BETWEEN, returns two conditions.
    pub fn to_sql_conditions(&self) -> Vec<(String, String)> {
        self.bounds()
            .into_iter()
            .map(|bound| {
                let operator = bound.operator().to_string();
                (operator, bound.into_key())
            })
            .collect()
    }
}

/// One bound on a stored sort key, in the stored form that compares the way
/// rows are ordered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKeyBound {
    Eq(String),
    Lt(String),
    Le(String),
    Gt(String),
    Ge(String),
}

impl SortKeyBound {
    /// The SQL comparison operator.
    pub fn operator(&self) -> &'static str {
        match self {
            Self::Eq(_) => "=",
            Self::Lt(_) => "<",
            Self::Le(_) => "<=",
            Self::Gt(_) => ">",
            Self::Ge(_) => ">=",
        }
    }

    fn into_key(self) -> String {
        match self {
            Self::Eq(k) | Self::Lt(k) | Self::Le(k) | Self::Gt(k) | Self::Ge(k) => k,
        }
    }

    /// Whether a stored key satisfies the bound.
    pub fn admits(&self, key: &str) -> bool {
        match self {
            Self::Eq(b) => key == b,
            Self::Lt(b) => key < b.as_str(),
            Self::Le(b) => key <= b.as_str(),
            Self::Gt(b) => key > b.as_str(),
            Self::Ge(b) => key >= b.as_str(),
        }
    }
}

/// Assemble resolved sort key conditions into a SQL fragment over the sk
/// column, together with the parameter values in placeholder order. The
/// partition key occupies ?1, so sk placeholders are numbered from ?2.
/// Returns None for the fragment when there are no conditions.
pub fn sk_conditions_to_sql(
    conditions: &[ResolvedSortKeyCondition],
) -> (Option<String>, Vec<String>) {
    let pairs: Vec<(String, String)> = conditions
        .iter()
        .flat_map(|c| c.to_sql_conditions())
        .collect();
    pairs_to_sql(&pairs)
}

/// Render a union of sort-key ranges. Every group must constrain the sort key;
/// an unconstrained branch requires reading the whole partition instead.
pub fn sk_condition_groups_to_sql(
    groups: &[Vec<ResolvedSortKeyCondition>],
) -> (Option<String>, Vec<String>) {
    if let [group] = groups {
        return sk_conditions_to_sql(group);
    }
    let mut branches = Vec::new();
    let mut values = Vec::new();
    for group in groups {
        let pairs: Vec<_> = group.iter().flat_map(|c| c.to_sql_conditions()).collect();
        if pairs.is_empty() {
            return (None, Vec::new());
        }
        let mut bounds = Vec::new();
        for (op, value) in pairs {
            bounds.push(format!("sk {op} ?{}", values.len() + 2));
            values.push(value);
        }
        branches.push(format!("({})", bounds.join(" AND ")));
    }
    if branches.is_empty() {
        (None, values)
    } else {
        (Some(format!("AND ({})", branches.join(" OR "))), values)
    }
}

/// Render `(operator, value)` bounds on the sk column as SQL, numbering the
/// placeholders from ?2 (the partition key is ?1).
fn pairs_to_sql(pairs: &[(String, String)]) -> (Option<String>, Vec<String>) {
    let mut sql_parts = Vec::new();
    let mut param_values = Vec::new();
    for (op, val) in pairs {
        let param_idx = param_values.len() + 2;
        sql_parts.push(format!("AND sk {op} ?{param_idx}"));
        param_values.push(val.clone());
    }
    let sql = if sql_parts.is_empty() {
        None
    } else {
        Some(sql_parts.join(" "))
    };
    (sql, param_values)
}

fn val_to_key_string(val: &AttributeValue) -> String {
    val.to_key_string().unwrap_or_default()
}

fn resolve_sk_condition(
    cond: &SortKeyCondition,
    tracker: &TrackedExpressionAttributes,
) -> Result<ResolvedSortKeyCondition, String> {
    match cond {
        SortKeyCondition::Eq(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Eq(sk.clone(), v))
        }
        SortKeyCondition::Lt(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Lt(sk.clone(), v))
        }
        SortKeyCondition::Le(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Le(sk.clone(), v))
        }
        SortKeyCondition::Gt(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Gt(sk.clone(), v))
        }
        SortKeyCondition::Ge(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Ge(sk.clone(), v))
        }
        SortKeyCondition::Between(sk, lo_ref, hi_ref) => {
            let lo = tracker.resolve_value(lo_ref)?.clone();
            let hi = tracker.resolve_value(hi_ref)?.clone();
            // Validate same type
            if std::mem::discriminant(&lo) != std::mem::discriminant(&hi) {
                return Err(format!(
                    "Invalid KeyConditionExpression: The BETWEEN operator requires same data type \
                     for lower and upper bounds; lower bound operand: AttributeValue: {{{}}}, \
                     upper bound operand: AttributeValue: {{{}}}",
                    format_attr_value_short(&lo),
                    format_attr_value_short(&hi)
                ));
            }
            // Validate ordering (upper >= lower)
            if !between_order_valid(&lo, &hi) {
                return Err(format!(
                    "Invalid KeyConditionExpression: The BETWEEN operator requires upper bound \
                     to be greater than or equal to lower bound; lower bound operand: \
                     AttributeValue: {{{}}}, upper bound operand: AttributeValue: {{{}}}",
                    format_attr_value_short(&lo),
                    format_attr_value_short(&hi)
                ));
            }
            Ok(ResolvedSortKeyCondition::Between(sk.clone(), lo, hi))
        }
        SortKeyCondition::BeginsWith(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::BeginsWith(sk.clone(), v))
        }
    }
}

// ---------------------------------------------------------------------------
// Internal parsing helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum ParsedCond {
    Eq(String, String), // (attr_name, value_ref)
    Lt(String, String),
    Le(String, String),
    Gt(String, String),
    Ge(String, String),
    Between(String, String, String), // (attr_name, lo_ref, hi_ref)
    BeginsWith(String, String),      // (attr_name, prefix_ref)
}

/// Reject an operand its operator has no use for: begins_with takes a string or
/// binary, the ordering comparisons and BETWEEN take a string, number or binary,
/// and `=` takes anything (a value of the wrong type for the key is a different
/// error). DynamoDB checks this before it resolves the table, condition by
/// condition in the order written, naming the operator as written, so `:v < sk`
/// reports `<` even though it is held as `sk > :v`. The first problem met in
/// that order wins: a bad operand ahead of an undefined value is reported, and
/// an undefined value ahead of a bad operand stops the check, leaving the
/// undefined value to resolution. Captured eu-west-2, 2026-10-08 and 2026-10-09.
fn check_operand_types(
    conds: &[(&ParsedCond, &str)],
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    fn orderable(v: &AttributeValue) -> bool {
        matches!(
            v,
            AttributeValue::S(_) | AttributeValue::N(_) | AttributeValue::B(_)
        )
    }
    fn string_or_binary(v: &AttributeValue) -> bool {
        matches!(v, AttributeValue::S(_) | AttributeValue::B(_))
    }

    let value = |value_ref: &str| tracker.values.as_ref().and_then(|m| m.get(value_ref));
    for (cond, op) in conds {
        let accepts: Option<fn(&AttributeValue) -> bool> = match cond {
            ParsedCond::Eq(..) => None,
            ParsedCond::BeginsWith(..) => Some(string_or_binary),
            _ => Some(orderable),
        };
        for value_ref in cond.value_refs() {
            let Some(v) = value(value_ref) else {
                return Ok(());
            };
            if let Some(accepts) = accepts
                && !accepts(v)
            {
                return Err(format!(
                    "Invalid KeyConditionExpression: Incorrect operand type for operator or function; \
                     operator or function: {op}, operand type: {}",
                    v.type_name()
                ));
            }
        }
    }
    Ok(())
}

impl ParsedCond {
    fn value_refs(&self) -> Vec<&str> {
        match self {
            ParsedCond::Eq(_, v)
            | ParsedCond::Lt(_, v)
            | ParsedCond::Le(_, v)
            | ParsedCond::Gt(_, v)
            | ParsedCond::Ge(_, v)
            | ParsedCond::BeginsWith(_, v) => vec![v],
            ParsedCond::Between(_, lo, hi) => vec![lo, hi],
        }
    }

    fn attr_name(&self) -> &str {
        match self {
            ParsedCond::Eq(n, _)
            | ParsedCond::Lt(n, _)
            | ParsedCond::Le(n, _)
            | ParsedCond::Gt(n, _)
            | ParsedCond::Ge(n, _)
            | ParsedCond::Between(n, _, _)
            | ParsedCond::BeginsWith(n, _) => n,
        }
    }

    fn into_clause(self) -> SortKeyCondition {
        match self {
            ParsedCond::Eq(n, v) => SortKeyCondition::Eq(n, v),
            ParsedCond::Lt(n, v) => SortKeyCondition::Lt(n, v),
            ParsedCond::Le(n, v) => SortKeyCondition::Le(n, v),
            ParsedCond::Gt(n, v) => SortKeyCondition::Gt(n, v),
            ParsedCond::Ge(n, v) => SortKeyCondition::Ge(n, v),
            ParsedCond::Between(n, lo, hi) => SortKeyCondition::Between(n, lo, hi),
            ParsedCond::BeginsWith(n, v) => SortKeyCondition::BeginsWith(n, v),
        }
    }
}

/// Strip balanced outer parentheses from a token list.
/// `(pk = :pk AND sk = :sk)` → `pk = :pk AND sk = :sk`
/// `((pk = :pk))` → `pk = :pk` (applied repeatedly)
/// `(pk = :pk) AND (sk = :sk)` → unchanged (closing paren is not at the end)
fn strip_outer_parens(mut tokens: Vec<(Token, TokenSpan)>) -> Vec<(Token, TokenSpan)> {
    loop {
        if tokens.len() < 2 {
            break;
        }
        if !matches!(tokens.first().map(|(t, _)| t), Some(Token::LParen)) {
            break;
        }
        // Walk forward, tracking paren depth, to see if the opening paren's
        // match is the very last token.
        let mut depth = 0;
        let mut close_pos = None;
        for (i, (tok, _)) in tokens.iter().enumerate() {
            match tok {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        close_pos = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        if close_pos == Some(tokens.len() - 1) {
            // The outermost parens wrap the entire expression: strip them.
            tokens.remove(tokens.len() - 1);
            tokens.remove(0);
        } else {
            break;
        }
    }
    tokens
}

/// Parse one condition, returning it with its operator as written, which the
/// operand type error names.
fn parse_single_condition(
    stream: &mut TokenStream,
    tracker: &TrackedExpressionAttributes,
) -> Result<(ParsedCond, &'static str), String> {
    // Count and skip optional wrapping parentheses: `(pk = :pk)`, `((pk = :pk))`
    let mut parens = 0;
    while matches!(stream.peek(), Some(Token::LParen)) {
        stream.next();
        parens += 1;
    }

    // Check for begins_with function
    if let Some(Token::Identifier(name)) = stream.peek()
        && name.to_lowercase() == "begins_with"
    {
        stream.next();
        stream.expect(&Token::LParen)?;
        let path = parse_raw_path(stream)?;
        let attr_name = resolve_path_to_name(&path, tracker)?;
        stream.expect(&Token::Comma)?;
        let val_ref = expect_value_ref(stream)?;
        stream.expect(&Token::RParen)?;
        consume_close_parens(stream, parens)?;
        return Ok((ParsedCond::BeginsWith(attr_name, val_ref), "begins_with"));
    }

    // Reversed operand order (:val op #attr). AWS accepts the value on the left
    // for the comparators and treats it as the attr-on-left form (`:lo <= #sk`
    // is `#sk >= :lo`). begins_with keeps the key first and is handled above.
    if matches!(stream.peek(), Some(Token::ValueRef(_))) {
        let val_ref = expect_value_ref(stream)?;
        // Match here rather than bind, to release the stream borrow before the path parse.
        type Build = fn(String, String) -> ParsedCond;
        let build: Option<(Build, &'static str)> = match stream.next() {
            Some(Token::Eq) => Some((ParsedCond::Eq, "=")),
            Some(Token::Lt) => Some((ParsedCond::Gt, "<")),
            Some(Token::Le) => Some((ParsedCond::Ge, "<=")),
            Some(Token::Gt) => Some((ParsedCond::Lt, ">")),
            Some(Token::Ge) => Some((ParsedCond::Le, ">=")),
            _ => None,
        };
        let (build, op) = build.ok_or_else(|| {
            "Invalid KeyConditionExpression: unsupported operator with a value on the left"
                .to_string()
        })?;
        let path = parse_raw_path(stream)?;
        let attr_name = resolve_path_to_name(&path, tracker)?;
        consume_close_parens(stream, parens)?;
        return Ok((build(attr_name, val_ref), op));
    }

    // attr op :val
    let path = parse_raw_path(stream)?;
    let attr_name = resolve_path_to_name(&path, tracker)?;

    let result = match stream.next() {
        Some(Token::Eq) => {
            let val_ref = expect_value_ref(stream)?;
            Ok((ParsedCond::Eq(attr_name, val_ref), "="))
        }
        Some(Token::Lt) => {
            let val_ref = expect_value_ref(stream)?;
            Ok((ParsedCond::Lt(attr_name, val_ref), "<"))
        }
        Some(Token::Le) => {
            let val_ref = expect_value_ref(stream)?;
            Ok((ParsedCond::Le(attr_name, val_ref), "<="))
        }
        Some(Token::Gt) => {
            let val_ref = expect_value_ref(stream)?;
            Ok((ParsedCond::Gt(attr_name, val_ref), ">"))
        }
        Some(Token::Ge) => {
            let val_ref = expect_value_ref(stream)?;
            Ok((ParsedCond::Ge(attr_name, val_ref), ">="))
        }
        Some(Token::Between) => {
            let lo_ref = expect_value_ref(stream)?;
            stream.expect(&Token::And)?;
            let hi_ref = expect_value_ref(stream)?;
            Ok((ParsedCond::Between(attr_name, lo_ref, hi_ref), "BETWEEN"))
        }
        Some(t) => Err(format!(
            "Unexpected operator in KeyConditionExpression: {t}"
        )),
        None => Err("Unexpected end of KeyConditionExpression".to_string()),
    };

    consume_close_parens(stream, parens)?;
    result
}

/// Consume exactly `count` closing parentheses from the stream.
fn consume_close_parens(stream: &mut TokenStream, count: usize) -> Result<(), String> {
    for _ in 0..count {
        match stream.next() {
            Some(Token::RParen) => {}
            Some(t) => {
                return Err(format!(
                    "Expected closing parenthesis in KeyConditionExpression, got {t}"
                ));
            }
            None => {
                return Err(
                    "Unexpected end of KeyConditionExpression, expected closing parenthesis"
                        .to_string(),
                );
            }
        }
    }
    Ok(())
}

fn resolve_path_to_name(
    path: &[PathElement],
    tracker: &TrackedExpressionAttributes,
) -> Result<String, String> {
    if path.len() != 1 {
        // A dotted or indexed document path on a key attribute.
        return Err(
            "Invalid KeyConditionExpression: KeyConditionExpressions cannot have conditions on nested attributes"
                .to_string(),
        );
    }
    match &path[0] {
        PathElement::Attribute(name) => {
            if name.starts_with('#') {
                tracker.resolve_name(name)
            } else {
                Ok(name.clone())
            }
        }
        PathElement::Index(_) => Err("KeyConditionExpression cannot use index paths".to_string()),
    }
}

/// Format an attribute value for error messages (DynamoDB short format).
fn format_attr_value_short(val: &AttributeValue) -> String {
    match val {
        AttributeValue::S(s) => format!("S:{s}"),
        AttributeValue::N(n) => format!("N:{n}"),
        AttributeValue::B(b) => {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            format!("B:{encoded}")
        }
        AttributeValue::BOOL(b) => format!("BOOL:{b}"),
        AttributeValue::NULL(_) => "NULL:true".to_string(),
        AttributeValue::SS(set) => format!("SS:{:?}", set),
        AttributeValue::NS(set) => format!("NS:{:?}", set),
        AttributeValue::BS(_) => "BS:[...]".to_string(),
        AttributeValue::L(_) => "L:[...]".to_string(),
        AttributeValue::M(_) => "M:{...}".to_string(),
    }
}

/// Check if BETWEEN bounds are in valid order (lo <= hi).
fn between_order_valid(lo: &AttributeValue, hi: &AttributeValue) -> bool {
    match (lo, hi) {
        (AttributeValue::S(a), AttributeValue::S(b)) => a <= b,
        (AttributeValue::N(a), AttributeValue::N(b)) => {
            let a_f = a.parse::<f64>().unwrap_or(0.0);
            let b_f = b.parse::<f64>().unwrap_or(0.0);
            a_f <= b_f
        }
        (AttributeValue::B(a), AttributeValue::B(b)) => a <= b,
        _ => true,
    }
}

fn expect_value_ref(stream: &mut TokenStream) -> Result<String, String> {
    match stream.next() {
        Some(Token::ValueRef(name)) => Ok(name.clone()),
        Some(t) => Err(format!("Expected value reference (:name), got {t}")),
        None => Err("Expected value reference, got end of expression".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_tracker<'a>(
        names: &'a Option<HashMap<String, String>>,
        values: &'a Option<HashMap<String, AttributeValue>>,
    ) -> TrackedExpressionAttributes<'a> {
        TrackedExpressionAttributes::new(names, values)
    }

    /// The two-clause view most of these tests were written against: the first
    /// equality is the partition key and the other clause the sort key.
    #[derive(Debug)]
    struct TwoClause {
        pk_name: String,
        pk_value_ref: String,
        sk_condition: Option<SortKeyCondition>,
    }

    fn parse(expr: &str, tracker: &TrackedExpressionAttributes) -> Result<TwoClause, String> {
        let mut clauses = super::parse(expr, tracker)?.clauses;
        let pk_at = clauses
            .iter()
            .position(|c| matches!(c, SortKeyCondition::Eq(..)))
            .expect("parse requires an equality");
        let SortKeyCondition::Eq(pk_name, pk_value_ref) = clauses.remove(pk_at) else {
            unreachable!("position found an equality")
        };
        Ok(TwoClause {
            pk_name,
            pk_value_ref,
            sk_condition: clauses.pop(),
        })
    }

    #[test]
    fn accepts_reversed_operand_sort_key() {
        let names = Some(HashMap::from([
            ("#pk".to_string(), "pk".to_string()),
            ("#sk".to_string(), "sk".to_string()),
        ]));
        let values = Some(HashMap::from([
            (":pk".to_string(), AttributeValue::S("x".into())),
            (":lo".to_string(), AttributeValue::S("a".into())),
        ]));
        let tracker = make_tracker(&names, &values);
        // Each reversed comparator maps to its attribute-on-left equivalent.
        for (op, expected) in [("<", "Gt"), ("<=", "Ge"), (">", "Lt"), (">=", "Le")] {
            let expr = format!("#pk = :pk AND :lo {op} #sk");
            let kc = parse(&expr, &tracker).unwrap();
            let tag = match &kc.sk_condition {
                Some(SortKeyCondition::Gt(n, v)) if n == "sk" && v == ":lo" => "Gt",
                Some(SortKeyCondition::Ge(n, v)) if n == "sk" && v == ":lo" => "Ge",
                Some(SortKeyCondition::Lt(n, v)) if n == "sk" && v == ":lo" => "Lt",
                Some(SortKeyCondition::Le(n, v)) if n == "sk" && v == ":lo" => "Le",
                _ => "other",
            };
            assert_eq!(tag, expected, "reversed {op} should flip to {expected}");
        }
    }

    #[test]
    fn accepts_reversed_equality_on_sort_key() {
        let names = Some(HashMap::from([
            ("#pk".to_string(), "pk".to_string()),
            ("#sk".to_string(), "sk".to_string()),
        ]));
        let values = Some(HashMap::from([
            (":pk".to_string(), AttributeValue::S("x".into())),
            (":sk".to_string(), AttributeValue::S("m".into())),
        ]));
        let tracker = make_tracker(&names, &values);
        let kc = parse("#pk = :pk AND :sk = #sk", &tracker).unwrap();
        assert!(
            matches!(&kc.sk_condition, Some(SortKeyCondition::Eq(n, v)) if n == "sk" && v == ":sk")
        );
    }

    #[test]
    fn rejects_reversed_operand_with_nested_key_path() {
        let names = Some(HashMap::from([
            ("#pk".to_string(), "pk".to_string()),
            ("#sk".to_string(), "sk".to_string()),
        ]));
        let values = Some(HashMap::from([
            (":pk".to_string(), AttributeValue::S("x".into())),
            (":lo".to_string(), AttributeValue::S("a".into())),
        ]));
        let tracker = make_tracker(&names, &values);
        let err = parse("#pk = :pk AND :lo <= #sk.foo", &tracker).unwrap_err();
        assert_eq!(
            err,
            "Invalid KeyConditionExpression: KeyConditionExpressions cannot have conditions on nested attributes"
        );
    }

    #[test]
    fn rejects_nested_path_on_key() {
        let names = Some(HashMap::from([
            ("#pk".to_string(), "pk".to_string()),
            ("#sk".to_string(), "sk".to_string()),
        ]));
        let values = Some(HashMap::from([
            (":pk".to_string(), AttributeValue::S("x".into())),
            (":v".to_string(), AttributeValue::S("y".into())),
        ]));
        let tracker = make_tracker(&names, &values);
        let err = parse("#pk = :pk AND #sk.foo = :v", &tracker).unwrap_err();
        assert_eq!(
            err,
            "Invalid KeyConditionExpression: KeyConditionExpressions cannot have conditions on nested attributes"
        );
    }

    #[test]
    fn test_pk_only() {
        let no_names = None;
        let no_values = None;
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("pk = :pk", &tracker).unwrap();
        assert_eq!(kc.pk_name, "pk");
        assert_eq!(kc.pk_value_ref, ":pk");
        assert!(kc.sk_condition.is_none());
    }

    #[test]
    fn test_pk_and_sk_eq() {
        let no_names = None;
        let no_values = None;
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("pk = :pk AND sk = :sk", &tracker).unwrap();
        assert_eq!(kc.pk_name, "pk");
        assert!(matches!(kc.sk_condition, Some(SortKeyCondition::Eq(_, _))));
    }

    #[test]
    fn test_pk_and_sk_between() {
        let no_names = None;
        let no_values = None;
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("pk = :pk AND sk BETWEEN :lo AND :hi", &tracker).unwrap();
        assert!(matches!(
            kc.sk_condition,
            Some(SortKeyCondition::Between(_, _, _))
        ));
    }

    #[test]
    fn test_pk_and_begins_with() {
        let no_names = None;
        let no_values = None;
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("pk = :pk AND begins_with(sk, :prefix)", &tracker).unwrap();
        assert!(matches!(
            kc.sk_condition,
            Some(SortKeyCondition::BeginsWith(_, _))
        ));
    }

    #[test]
    fn test_with_attribute_names() {
        let an = Some(HashMap::from([
            ("#pk".to_string(), "partitionKey".to_string()),
            ("#sk".to_string(), "sortKey".to_string()),
        ]));
        let no_values = None;
        let tracker = make_tracker(&an, &no_values);
        let kc = parse("#pk = :pk AND #sk > :sk", &tracker).unwrap();
        assert_eq!(kc.pk_name, "partitionKey");
        assert!(matches!(kc.sk_condition, Some(SortKeyCondition::Gt(ref n, _)) if n == "sortKey"));
    }

    #[test]
    fn test_resolve_values() {
        let no_names = None;
        let no_values = None;
        let parse_tracker = make_tracker(&no_names, &no_values);
        let kc = super::parse("pk = :pk AND sk >= :sk", &parse_tracker).unwrap();
        let av = Some(HashMap::from([
            (":pk".to_string(), AttributeValue::S("user#1".into())),
            (":sk".to_string(), AttributeValue::S("2024-01-01".into())),
        ]));
        let resolve_tracker = make_tracker(&no_names, &av);
        let (hash, range) = (["pk".to_string()], ["sk".to_string()]);
        let keys = KeyAttributes {
            hash: &hash,
            range: &range,
        };
        let resolved = bind(&kc, keys, &resolve_tracker).unwrap();
        assert_eq!(
            resolved.hash,
            vec![("pk".to_string(), AttributeValue::S("user#1".into()))]
        );
        assert!(matches!(
            resolved.range_last,
            Some(ResolvedSortKeyCondition::Ge(_, _))
        ));
    }

    #[test]
    fn test_parenthesized_conditions() {
        let no_names = None;
        let no_values = None;

        // Parens around each condition
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("(pk = :pk) AND (sk = :sk)", &tracker).unwrap();
        assert_eq!(kc.pk_name, "pk");
        assert!(matches!(kc.sk_condition, Some(SortKeyCondition::Eq(_, _))));

        // Parens around entire expression
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("(pk = :pk AND sk = :sk)", &tracker).unwrap();
        assert_eq!(kc.pk_name, "pk");
        assert!(matches!(kc.sk_condition, Some(SortKeyCondition::Eq(_, _))));

        // Genuinely nested (non-redundant) parens are accepted.
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("(pk = :pk AND (sk > :sk))", &tracker).unwrap();
        assert_eq!(kc.pk_name, "pk");
        assert!(matches!(kc.sk_condition, Some(SortKeyCondition::Gt(_, _))));

        // Redundant parentheses are rejected, matching real DynamoDB. Dynoxide
        // previously stripped them and silently accepted.
        let tracker = make_tracker(&no_names, &no_values);
        let err = parse("((pk = :pk)) AND ((sk > :sk))", &tracker).unwrap_err();
        assert!(
            err.contains("redundant parentheses"),
            "expected redundant-parentheses rejection, got: {err}"
        );

        // Parens around begins_with
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("(pk = :pk) AND (begins_with(sk, :prefix))", &tracker).unwrap();
        assert!(matches!(
            kc.sk_condition,
            Some(SortKeyCondition::BeginsWith(_, _))
        ));

        // Parens with attribute name references
        let an = Some(HashMap::from([
            ("#pk".to_string(), "PK".to_string()),
            ("#sk".to_string(), "SK".to_string()),
        ]));
        let tracker = make_tracker(&an, &no_values);
        let kc = parse("(#pk = :pk) AND (#sk = :sk)", &tracker).unwrap();
        assert_eq!(kc.pk_name, "PK");
    }

    #[test]
    fn test_sk_comparisons() {
        let no_names = None;
        let no_values = None;
        for (op, variant) in [("<", "Lt"), ("<=", "Le"), (">", "Gt"), (">=", "Ge")] {
            let tracker = make_tracker(&no_names, &no_values);
            let kc = parse(&format!("pk = :pk AND sk {op} :sk"), &tracker).unwrap();
            let sk = kc.sk_condition.unwrap();
            let name = match &sk {
                SortKeyCondition::Lt(n, _) => format!("Lt:{n}"),
                SortKeyCondition::Le(n, _) => format!("Le:{n}"),
                SortKeyCondition::Gt(n, _) => format!("Gt:{n}"),
                SortKeyCondition::Ge(n, _) => format!("Ge:{n}"),
                _ => "other".to_string(),
            };
            assert!(name.starts_with(variant), "Expected {variant}, got {name}");
        }
    }
}
