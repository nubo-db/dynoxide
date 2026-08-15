//! PartiQL statement parser.
//!
//! Parses a subset of PartiQL relevant to DynamoDB:
//! - `SELECT [projections] FROM "table" [WHERE conditions]`
//! - INSERT INTO "table" VALUE { ... } [IF NOT EXISTS]
//! - UPDATE "table" SET path = value [REMOVE attr1, attr2] [WHERE conditions]
//! - DELETE FROM "table" [WHERE conditions]

use crate::types::AttributeValue;
use std::collections::HashMap;

/// Why a statement did not parse.
///
/// DynamoDB wraps a syntax error in `Statement wasn't well formed, can't be
/// processed: ` and returns some rejections bare, so which envelope a message
/// takes is part of the observable contract. The parser says which it is rather
/// than leaving each caller to guess from the text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// A malformed statement. The caller wraps this in DynamoDB's envelope.
    #[error("{0}")]
    Syntax(String),
    /// A well-formed statement DynamoDB rejects on its own terms, reported
    /// without the envelope.
    #[error("{0}")]
    Validation(String),
}

impl ParseError {
    /// The message DynamoDB returns, envelope included where it uses one.
    pub fn into_message(self) -> String {
        match self {
            ParseError::Syntax(m) => {
                format!("Statement wasn't well formed, can't be processed: {m}")
            }
            ParseError::Validation(m) => m,
        }
    }
}

// Every helper below still returns a bare `String` for a syntax error, so `?`
// lifts it into the common case and only the handful of sites with their own
// envelope name a variant.
impl From<String> for ParseError {
    fn from(message: String) -> Self {
        ParseError::Syntax(message)
    }
}

impl From<&str> for ParseError {
    fn from(message: &str) -> Self {
        ParseError::Syntax(message.to_string())
    }
}

/// A parsed PartiQL statement.
///
/// Every variant is `#[non_exhaustive]`: they carry a growing set of clauses (a
/// `RETURNING` variant was added in 0.12.0, an index qualifier after that), so
/// downstream code must match them with `..` and cannot build them by struct
/// literal. This keeps later clause additions non-breaking, matching the
/// precedent set by `DynoxideError`.
#[derive(Debug, Clone)]
pub enum Statement {
    #[non_exhaustive]
    Select {
        table_name: String,
        /// The index named by a `"table"."index"` qualifier, if any.
        index_name: Option<String>,
        projections: Vec<String>, // empty = SELECT *
        where_clause: Option<WhereClause>,
    },
    #[non_exhaustive]
    Insert {
        table_name: String,
        item: HashMap<String, PartiqlValue>,
        if_not_exists: bool,
    },
    #[non_exhaustive]
    Update {
        table_name: String,
        /// The index named by a `"table"."index"` qualifier, if any. DynamoDB
        /// parses one here and rejects it in execution, so the parser carries
        /// it rather than failing, and the executor produces the message.
        index_name: Option<String>,
        set_clauses: Vec<SetClause>,
        remove_paths: Vec<String>,
        where_clause: Option<WhereClause>,
        /// The `RETURNING` variant when the statement ends with a `RETURNING`
        /// clause. All four variants are valid on `UPDATE`. `None` when absent.
        returning: Option<ReturningVariant>,
    },
    #[non_exhaustive]
    Delete {
        table_name: String,
        /// The index named by a `"table"."index"` qualifier, if any. Carried
        /// for the same reason as on `Update`.
        index_name: Option<String>,
        where_clause: Option<WhereClause>,
        /// The `RETURNING` variant when the statement ends with a `RETURNING`
        /// clause. DynamoDB allows only `ALL OLD *` on `DELETE`; the executor
        /// rejects the other variants. `None` when there is no clause.
        returning: Option<ReturningVariant>,
    },
}

/// A PartiQL `RETURNING` clause variant: the cross product of `ALL`/`MODIFIED`
/// and `OLD`/`NEW`, as in `RETURNING MODIFIED NEW *`.
///
/// All four are valid on `UPDATE`; only `AllOld` is valid on `DELETE` (the
/// executor rejects the others). The parser recognises any well-formed variant
/// and defers semantic validity to the executor, so the rejection can carry
/// DynamoDB's exact `ValidationException` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturningVariant {
    AllOld,
    ModifiedOld,
    AllNew,
    ModifiedNew,
}

impl ReturningVariant {
    /// The clause keywords as they appear after `RETURNING` (e.g. `ALL OLD`),
    /// used to reconstruct DynamoDB's error message for a rejected variant.
    pub fn as_sql(self) -> &'static str {
        match self {
            ReturningVariant::AllOld => "ALL OLD",
            ReturningVariant::ModifiedOld => "MODIFIED OLD",
            ReturningVariant::AllNew => "ALL NEW",
            ReturningVariant::ModifiedNew => "MODIFIED NEW",
        }
    }
}

/// Extract the table name from a parsed statement.
pub fn table_name(stmt: &Statement) -> Option<&str> {
    match stmt {
        Statement::Select { table_name, .. }
        | Statement::Insert { table_name, .. }
        | Statement::Update { table_name, .. }
        | Statement::Delete { table_name, .. } => Some(table_name),
    }
}

/// The index a statement's `FROM` clause qualified its table with, as in
/// `SELECT * FROM "table"."index"`. `None` when the statement names a table
/// alone.
///
/// Carried for every statement kind rather than for `SELECT` alone, because
/// DynamoDB rejects a qualifier on `UPDATE` and `DELETE` semantically rather
/// than at parse time, and the executor needs the name to say so.
pub fn index_name(stmt: &Statement) -> Option<&str> {
    match stmt {
        Statement::Select { index_name, .. }
        | Statement::Update { index_name, .. }
        | Statement::Delete { index_name, .. } => index_name.as_deref(),
        // An INSERT never carries one: DynamoDB rejects a qualified name there
        // at parse time, so the statement does not survive to be executed.
        Statement::Insert { .. } => None,
    }
}

/// The `RETURNING` variant a statement carries, if any. Only `UPDATE` and
/// `DELETE` can carry one; every other statement returns `None`.
pub fn returning_variant(stmt: &Statement) -> Option<ReturningVariant> {
    match stmt {
        Statement::Update { returning, .. } | Statement::Delete { returning, .. } => *returning,
        _ => None,
    }
}

/// DynamoDB's rejection message for a `COUNT` projection, if the statement
/// carries one.
///
/// Real DynamoDB does not support aggregate functions in PartiQL: a `COUNT`
/// token in a SELECT's projection list is rejected with a bare
/// `ValidationException` reading `Unexpected path component at
/// <line>:<column>:<length>`, where line and column are the 1-based position
/// of the token and the length is always 5 (the `COUNT` keyword itself,
/// whatever is inside the parens). The rejection fires before the
/// table-existence check (captured eu-west-2, 2026-07-29).
///
/// Returns `Some(message)` when `statement` is a SELECT and, before its first
/// unquoted `FROM`, an unquoted word-bounded `count` (any case) is followed by
/// optional whitespace and `(`. String literals and quoted identifiers are
/// skipped, so a `'count('` literal or a quoted `"COUNT"` attribute name never
/// triggers. Returns `None` otherwise.
pub(crate) fn count_projection_rejection(statement: &str) -> Option<String> {
    fn advance(c: char, line: &mut usize, col: &mut usize) {
        if c == '\n' {
            *line += 1;
            *col = 1;
        } else {
            *col += 1;
        }
    }

    let chars: Vec<char> = statement.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut line = 1usize;
    let mut col = 1usize;
    let mut first_word_seen = false;

    while i < len {
        let c = chars[i];

        // Skip string literals and quoted identifiers wholesale, honouring the
        // doubled-quote escapes ('' and "") the tokenizer accepts.
        if c == '\'' || c == '"' {
            let quote = c;
            advance(c, &mut line, &mut col);
            i += 1;
            while i < len {
                let ch = chars[i];
                advance(ch, &mut line, &mut col);
                i += 1;
                if ch == quote {
                    if i < len && chars[i] == quote {
                        advance(chars[i], &mut line, &mut col);
                        i += 1; // escaped quote, still inside
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        if c.is_ascii_alphabetic() || c == '_' {
            let word_line = line;
            let word_col = col;
            let start = i;
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                advance(chars[i], &mut line, &mut col);
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();

            if !first_word_seen {
                first_word_seen = true;
                if !word.eq_ignore_ascii_case("SELECT") {
                    return None;
                }
                continue;
            }
            if word.eq_ignore_ascii_case("FROM") {
                return None;
            }
            if word.eq_ignore_ascii_case("COUNT") {
                let mut j = i;
                while j < len && chars[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j < len && chars[j] == '(' {
                    return Some(format!(
                        "Unexpected path component at {word_line}:{word_col}:5"
                    ));
                }
            }
            continue;
        }

        advance(c, &mut line, &mut col);
        i += 1;
    }

    None
}

/// A SET clause in an UPDATE statement.
#[derive(Debug, Clone)]
pub struct SetClause {
    pub path: String,
    pub value: SetValue,
}

/// A value on the right-hand side of a SET assignment.
/// Supports simple values and binary arithmetic expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum SetValue {
    /// A simple value (literal or parameter).
    Simple(PartiqlValue),
    /// `path + value` — add the value to the attribute at path.
    Add(String, PartiqlValue),
    /// `path - value` — subtract the value from the attribute at path.
    Sub(String, PartiqlValue),
    /// `list_append(path, value)` or `list_append(value, path)`.
    ListAppend(PartiqlValue, PartiqlValue),
}

/// A WHERE clause with OR-group semantics.
///
/// Groups are OR-joined; conditions within each group are AND-joined.
/// `WHERE a = 1 AND b = 2 OR c = 3` parses as `[[a=1, b=2], [c=3]]`.
#[derive(Debug, Clone)]
pub struct WhereClause {
    /// OR-groups: outer = OR, inner = AND.
    pub groups: Vec<Vec<WhereCondition>>,
}

impl WhereClause {
    /// Create a WhereClause from a single group of AND-joined conditions.
    pub fn from_conditions(conditions: Vec<WhereCondition>) -> Self {
        Self {
            groups: vec![conditions],
        }
    }

    /// Create a WhereClause from multiple OR-groups.
    pub fn from_groups(groups: Vec<Vec<WhereCondition>>) -> Self {
        Self { groups }
    }
}

/// A single condition in a WHERE clause — either a comparison or a function call.
#[derive(Debug, Clone)]
pub enum WhereCondition {
    Comparison(Condition),
    Exists(String),
    NotExists(String),
    BeginsWith(String, PartiqlValue),
    NotBeginsWith(String, PartiqlValue),
    Between(String, PartiqlValue, PartiqlValue),
    In(String, Vec<PartiqlValue>),
    Contains(String, PartiqlValue),
    IsMissing(String),
    IsNotMissing(String),
}

/// A comparison condition (path op value).
#[derive(Debug, Clone)]
pub struct Condition {
    pub path: String,
    pub op: CompOp,
    pub value: PartiqlValue,
}

/// Comparison operator.
///
/// The condition-expression engine's operator, rather than a second enum with
/// the same shape. The two comparison implementations behind them have already
/// drifted once; sharing the operator keeps the seam between the surfaces
/// narrow enough that it cannot happen again by accident.
pub use crate::expressions::condition::CompOp;

/// A value in a PartiQL expression — either a literal or a parameter placeholder.
#[derive(Debug, Clone, PartialEq)]
pub enum PartiqlValue {
    Literal(AttributeValue),
    Parameter(usize), // 0-based index into the Parameters array
}

/// Parse a PartiQL statement string.
pub fn parse(input: &str) -> Result<Statement, ParseError> {
    let mut tokenizer = Tokenizer::new(input)?;
    let first = tokenizer
        .next_token()?
        .ok_or("Empty statement")?
        .to_uppercase();

    match first.as_str() {
        "SELECT" => parse_select(&mut tokenizer),
        "INSERT" => parse_insert(&mut tokenizer),
        "UPDATE" => parse_update(&mut tokenizer),
        "DELETE" => parse_delete(&mut tokenizer),
        // A statement that does not begin with a DML keyword is not valid
        // PartiQL; DynamoDB reports this as "Expected data manipulation".
        _ => Err("Expected data manipulation".into()),
    }
}

fn parse_select(t: &mut Tokenizer) -> Result<Statement, ParseError> {
    // Parse projections
    let projections = parse_projections(t)?;

    // Expect FROM
    expect_keyword(t, "FROM")?;

    // Parse table name, plus any index the FROM clause qualified it with
    let target = parse_table_name(t)?;

    // Optional WHERE clause
    let where_clause = parse_optional_where(t)?;

    Ok(Statement::Select {
        table_name: target.name,
        index_name: target.index,
        projections,
        where_clause,
    })
}

fn parse_projections(t: &mut Tokenizer) -> Result<Vec<String>, String> {
    let tok = t.peek_token()?.ok_or("Expected projection")?;

    if tok == "*" {
        t.next_token()?; // consume *
        return Ok(Vec::new());
    }

    // COUNT is not special-cased: the actions reject it with DynamoDB's
    // "Unexpected path component" message before parsing, so for library
    // callers it parses as an attribute token and the following '(' is a
    // parse error.
    let mut projections = Vec::new();
    loop {
        let name = t
            .next_token()?
            .ok_or("Expected projection attribute name")?;
        let mut path = unquote(&name);

        // Greedily consume dot-separated segments and array indexes
        loop {
            match t.peek_token()? {
                Some(ref s) if s == "." => {
                    t.next_token()?; // consume dot
                    let segment = t.next_token()?.ok_or("Expected attribute name after '.'")?;
                    path.push('.');
                    path.push_str(&unquote(&segment));
                }
                Some(ref s) if s == "[" => {
                    t.next_token()?; // consume [
                    let idx = t.next_token()?.ok_or("Expected index in '[]'")?;
                    let close = t.next_token()?.ok_or("Expected ']'")?;
                    if close != "]" {
                        return Err(format!("Expected ']' but got '{close}'"));
                    }
                    path.push('[');
                    path.push_str(&idx);
                    path.push(']');
                }
                _ => break,
            }
        }

        projections.push(path);

        match t.peek_token()? {
            Some(ref s) if s == "," => {
                t.next_token()?; // consume comma
            }
            _ => break,
        }
    }

    Ok(projections)
}

fn parse_insert(t: &mut Tokenizer) -> Result<Statement, ParseError> {
    expect_keyword(t, "INTO")?;
    let target = parse_table_name(t)?;
    // Unlike UPDATE and DELETE, which parse a qualifier and reject it in
    // execution, DynamoDB rejects one on INSERT here, and reports where the
    // table name sat. Captured eu-west-2 2026-08-15.
    if target.index.is_some() {
        return Err(ParseError::Validation(format!(
            "FROM clause may only contain a single table name in data \
             manipulation statements at {}",
            t.position(target.span)
        )));
    }
    let table_name = target.name;
    expect_keyword(t, "VALUE")?;

    // Parse the item literal as a map of possibly-parameterised values
    let item = parse_item_literal_partiql(t)?;

    // Check for IF NOT EXISTS
    let if_not_exists = if let Some(ref tok) = t.peek_token()? {
        if tok.eq_ignore_ascii_case("IF") {
            t.next_token()?; // consume IF
            expect_keyword(t, "NOT")?;
            expect_keyword(t, "EXISTS")?;
            true
        } else {
            false
        }
    } else {
        false
    };

    Ok(Statement::Insert {
        table_name,
        item,
        if_not_exists,
    })
}

fn parse_update(t: &mut Tokenizer) -> Result<Statement, ParseError> {
    let target = parse_table_name(t)?;

    // SET and REMOVE are both optional but at least one must be present.
    // Parse SET clauses if the next keyword is SET.
    let mut set_clauses = Vec::new();
    let mut remove_paths = Vec::new();

    if let Some(ref tok) = t.peek_token()? {
        if tok.eq_ignore_ascii_case("SET") {
            t.next_token()?; // consume SET
            loop {
                let path_tok = t.next_token()?.ok_or("Expected attribute path in SET")?;
                let path = parse_dotted_path_from_token(&path_tok, t)?;

                let eq = t.next_token()?.ok_or("Expected '='")?;
                if eq != "=" {
                    return Err(format!("Expected '=' but got '{eq}'").into());
                }

                let value = parse_set_value(t)?;
                set_clauses.push(SetClause { path, value });

                match t.peek_token()? {
                    Some(ref s) if s == "," => {
                        t.next_token()?; // consume comma
                    }
                    _ => break,
                }
            }
        }
    }

    // Check for REMOVE keyword
    if let Some(ref tok) = t.peek_token()? {
        if tok.eq_ignore_ascii_case("REMOVE") {
            t.next_token()?; // consume REMOVE
            loop {
                let path_tok = t.next_token()?.ok_or("Expected attribute path in REMOVE")?;
                let path = parse_dotted_path_from_token(&path_tok, t)?;
                remove_paths.push(path);
                match t.peek_token()? {
                    Some(ref s) if s == "," => {
                        t.next_token()?;
                    }
                    _ => break,
                }
            }
        }
    }

    if set_clauses.is_empty() && remove_paths.is_empty() {
        return Err("UPDATE requires at least one SET or REMOVE clause".into());
    }

    let where_clause = parse_optional_where(t)?;
    let returning = parse_optional_returning(t)?;

    Ok(Statement::Update {
        table_name: target.name,
        index_name: target.index,
        set_clauses,
        remove_paths,
        where_clause,
        returning,
    })
}

/// Parse the right-hand side of a SET assignment: `value`, `path + value`, `path - value`,
/// or `list_append(a, b)`.
fn parse_set_value(t: &mut Tokenizer) -> Result<SetValue, String> {
    // Check for list_append function
    if let Some(ref tok) = t.peek_token()? {
        if tok.eq_ignore_ascii_case("list_append") {
            t.next_token()?; // consume list_append
            expect_char(t, "(")?;
            let first = parse_value(t)?;
            let comma = t.next_token()?.ok_or("Expected ',' in list_append")?;
            if comma != "," {
                return Err(format!("Expected ',' but got '{comma}'"));
            }
            let second = parse_value(t)?;
            expect_char(t, ")")?;
            return Ok(SetValue::ListAppend(first, second));
        }
    }

    let first = parse_value(t)?;

    // Peek for + or -
    match t.peek_token()? {
        Some(ref s) if s == "+" => {
            t.next_token()?; // consume +
            let second = parse_value(t)?;
            // The first value should be a path reference (attribute name).
            // In PartiQL, `SET x = x + 1` means add 1 to the current value of x.
            let attr_path = match &first {
                PartiqlValue::Literal(AttributeValue::S(s)) => s.clone(),
                // If first is an unquoted identifier that was mistakenly parsed as something
                // else, we need to handle it. But identifiers in SET RHS would have been
                // consumed as unknown tokens and errored. We'll handle the common case
                // where parse_value can't parse an identifier — see below.
                _ => {
                    return Err(
                        "Expected attribute path on left side of '+' expression".to_string()
                    );
                }
            };
            Ok(SetValue::Add(attr_path, second))
        }
        Some(ref s) if s == "-" => {
            t.next_token()?; // consume -
            let second = parse_value(t)?;
            let attr_path = match &first {
                PartiqlValue::Literal(AttributeValue::S(s)) => s.clone(),
                _ => {
                    return Err(
                        "Expected attribute path on left side of '-' expression".to_string()
                    );
                }
            };
            Ok(SetValue::Sub(attr_path, second))
        }
        _ => Ok(SetValue::Simple(first)),
    }
}

fn parse_delete(t: &mut Tokenizer) -> Result<Statement, ParseError> {
    expect_keyword(t, "FROM")?;
    let target = parse_table_name(t)?;
    let where_clause = parse_optional_where(t)?;
    let returning = parse_optional_returning(t)?;

    Ok(Statement::Delete {
        table_name: target.name,
        index_name: target.index,
        where_clause,
        returning,
    })
}

/// Parse an optional trailing `RETURNING <ALL|MODIFIED> <OLD|NEW> *` clause.
///
/// Recognises any of the four well-formed variants and returns it; a malformed
/// clause (missing `*`, unknown keywords, trailing tokens) is a syntax error.
/// Which variants a given command actually permits is enforced by the executor,
/// so it can surface DynamoDB's exact validation message.
fn parse_optional_returning(t: &mut Tokenizer) -> Result<Option<ReturningVariant>, String> {
    match t.peek_token()? {
        Some(ref s) if s.eq_ignore_ascii_case("RETURNING") => {
            t.next_token()?; // consume RETURNING
            let first = t
                .next_token()?
                .ok_or("Expected returning variant after RETURNING")?;
            let second = t
                .next_token()?
                .ok_or("Expected returning variant after RETURNING")?;
            let star = t.next_token()?.ok_or("Expected '*' in RETURNING clause")?;
            if star != "*" || t.peek_token()?.is_some() {
                return Err(
                    "Malformed RETURNING clause; expected RETURNING <ALL|MODIFIED> <OLD|NEW> *"
                        .to_string(),
                );
            }
            let variant = match (
                first.to_ascii_uppercase().as_str(),
                second.to_ascii_uppercase().as_str(),
            ) {
                ("ALL", "OLD") => ReturningVariant::AllOld,
                ("MODIFIED", "OLD") => ReturningVariant::ModifiedOld,
                ("ALL", "NEW") => ReturningVariant::AllNew,
                ("MODIFIED", "NEW") => ReturningVariant::ModifiedNew,
                _ => {
                    return Err(format!(
                        "Unsupported RETURNING clause: RETURNING {} {} *",
                        first.to_ascii_uppercase(),
                        second.to_ascii_uppercase()
                    ));
                }
            };
            Ok(Some(variant))
        }
        _ => Ok(None),
    }
}

/// A `FROM` target: a table, and the index its name was qualified with.
///
/// `"table"."index"` is DynamoDB's way of naming an index in PartiQL. The
/// tokenizer emits `.` as its own token, so the qualifier has to be consumed
/// here; leaving it in the stream makes the next clause parse against `.` and
/// the index name, which silently drops that clause.
struct TableTarget {
    name: String,
    index: Option<String>,
    /// Where the table name sat in the source, for the `INSERT` rejection.
    span: Span,
}

fn parse_table_name(t: &mut Tokenizer) -> Result<TableTarget, ParseError> {
    let span = t.span_at(t.pos);
    let raw = t.next_token()?.ok_or("Expected table name")?;
    let name = unquote(&raw);
    if name.is_empty() {
        return Err(ParseError::Validation(
            "Path component cannot be an empty string".to_string(),
        ));
    }

    let index = match t.peek_token()? {
        Some(ref dot) if dot == "." => {
            t.next_token()?; // consume '.'
            let raw = t.next_token()?.ok_or("Expected index name after '.'")?;
            let index = unquote(&raw);
            if index.is_empty() {
                return Err(ParseError::Validation(
                    "Path component cannot be an empty string".to_string(),
                ));
            }
            // A third component is its own rejection rather than a generic
            // parse failure, matching DynamoDB.
            if matches!(t.peek_token()?, Some(ref dot) if dot == ".") {
                return Err(ParseError::Validation(
                    "A path may contain at most 2 components in the FROM clause".to_string(),
                ));
            }
            Some(index)
        }
        _ => None,
    };

    Ok(TableTarget { name, index, span })
}

fn parse_optional_where(t: &mut Tokenizer) -> Result<Option<WhereClause>, String> {
    match t.peek_token()? {
        Some(ref s) if s.eq_ignore_ascii_case("WHERE") => {
            t.next_token()?; // consume WHERE
            let groups = parse_conditions_with_or(t)?;
            Ok(Some(WhereClause::from_groups(groups)))
        }
        _ => Ok(None),
    }
}

/// Parse conditions supporting both AND and OR.
/// Returns a list of OR-groups, where each group is a list of AND-joined conditions.
fn parse_conditions_with_or(t: &mut Tokenizer) -> Result<Vec<Vec<WhereCondition>>, String> {
    let mut groups: Vec<Vec<WhereCondition>> = Vec::new();
    let mut current_group: Vec<WhereCondition> = Vec::new();

    loop {
        let condition = parse_single_condition(t)?;
        current_group.push(condition);

        match t.peek_token()? {
            Some(ref s) if s.eq_ignore_ascii_case("AND") => {
                t.next_token()?; // consume AND — continue in current group
            }
            Some(ref s) if s.eq_ignore_ascii_case("OR") => {
                t.next_token()?; // consume OR — start new group
                groups.push(current_group);
                current_group = Vec::new();
            }
            _ => break,
        }
    }

    groups.push(current_group);
    Ok(groups)
}

/// Parse a single condition (comparison, function call, etc.).
fn parse_single_condition(t: &mut Tokenizer) -> Result<WhereCondition, String> {
    let tok = t.next_token()?.ok_or("Expected condition in WHERE")?;
    let tok_upper = tok.to_uppercase();

    match tok_upper.as_str() {
        "EXISTS" => {
            expect_char(t, "(")?;
            let path = parse_function_path(t)?;
            expect_char(t, ")")?;
            Ok(WhereCondition::Exists(path))
        }
        "BEGINS_WITH" => {
            expect_char(t, "(")?;
            let path = parse_function_path(t)?;
            let comma = t.next_token()?.ok_or("Expected ',' in BEGINS_WITH")?;
            if comma != "," {
                return Err(format!("Expected ',' but got '{comma}'"));
            }
            let value = parse_value(t)?;
            expect_char(t, ")")?;
            Ok(WhereCondition::BeginsWith(path, value))
        }
        "CONTAINS" => {
            expect_char(t, "(")?;
            let path = parse_function_path(t)?;
            let comma = t.next_token()?.ok_or("Expected ',' in CONTAINS")?;
            if comma != "," {
                return Err(format!("Expected ',' but got '{comma}'"));
            }
            let value = parse_value(t)?;
            expect_char(t, ")")?;
            Ok(WhereCondition::Contains(path, value))
        }
        "NOT" => {
            let func = t.next_token()?.ok_or("Expected function name after NOT")?;
            if func.eq_ignore_ascii_case("EXISTS") {
                expect_char(t, "(")?;
                let path = parse_function_path(t)?;
                expect_char(t, ")")?;
                Ok(WhereCondition::NotExists(path))
            } else if func.eq_ignore_ascii_case("BEGINS_WITH") {
                expect_char(t, "(")?;
                let path = parse_function_path(t)?;
                let comma = t.next_token()?.ok_or("Expected ',' in NOT BEGINS_WITH")?;
                if comma != "," {
                    return Err(format!("Expected ',' but got '{comma}'"));
                }
                let value = parse_value(t)?;
                expect_char(t, ")")?;
                Ok(WhereCondition::NotBeginsWith(path, value))
            } else {
                Err(format!("Unsupported NOT function: {func}"))
            }
        }
        _ => {
            // Regular comparison or BETWEEN / IN / IS MISSING
            // The token might be the start of a dotted path
            let path = parse_dotted_path_from_token(&tok, t)?;

            // Peek at the next token to decide which form this is
            let next = t
                .peek_token()?
                .ok_or("Expected operator after attribute path")?;
            let next_upper = next.to_uppercase();

            match next_upper.as_str() {
                "BETWEEN" => {
                    t.next_token()?; // consume BETWEEN
                    let low = parse_value(t)?;
                    expect_keyword(t, "AND")?;
                    let high = parse_value(t)?;
                    Ok(WhereCondition::Between(path, low, high))
                }
                "IN" => {
                    t.next_token()?; // consume IN
                    // Accept both the parenthesised form `IN (...)` (DynamoDB
                    // FilterExpression style) and the bracket form `IN [...]`
                    // (PartiQL style); the closing token must match the opener.
                    let open = t.next_token()?.ok_or("Expected '(' or '[' after IN")?;
                    let close_char = match open.as_str() {
                        "(" => ")",
                        "[" => "]",
                        other => {
                            return Err(format!("Expected '(' or '[' after IN, got '{other}'"));
                        }
                    };
                    let mut values = Vec::new();
                    loop {
                        let peek = t.peek_token()?.ok_or("Unexpected end of IN list")?;
                        if peek == close_char {
                            t.next_token()?; // consume closing bracket
                            break;
                        }
                        if peek == "," {
                            t.next_token()?; // consume comma
                            continue;
                        }
                        values.push(parse_value(t)?);
                    }
                    Ok(WhereCondition::In(path, values))
                }
                "IS" => {
                    t.next_token()?; // consume IS
                    let kw = t.next_token()?.ok_or("Expected MISSING or NOT after IS")?;
                    let kw_upper = kw.to_uppercase();
                    match kw_upper.as_str() {
                        "MISSING" => Ok(WhereCondition::IsMissing(path)),
                        "NOT" => {
                            expect_keyword(t, "MISSING")?;
                            Ok(WhereCondition::IsNotMissing(path))
                        }
                        other => Err(format!(
                            "Expected MISSING or NOT MISSING after IS, got '{other}'"
                        )),
                    }
                }
                _ => {
                    // Standard comparison: path op value
                    let op_tok = t.next_token()?.ok_or("Expected comparison operator")?;
                    let op = match op_tok.as_str() {
                        "=" => CompOp::Eq,
                        "<>" | "!=" => CompOp::Ne,
                        "<" => CompOp::Lt,
                        "<=" => CompOp::Le,
                        ">" => CompOp::Gt,
                        ">=" => CompOp::Ge,
                        other => return Err(format!("Unknown operator: {other}")),
                    };
                    let value = parse_value(t)?;
                    Ok(WhereCondition::Comparison(Condition { path, op, value }))
                }
            }
        }
    }
}

/// Parse a dotted path starting from an already-consumed first token.
/// Greedily consumes `.segment` continuations.
fn parse_dotted_path_from_token(first_tok: &str, t: &mut Tokenizer) -> Result<String, String> {
    let mut path = unquote(first_tok);
    while let Some(ref next) = t.peek_token()? {
        if next == "." {
            t.next_token()?; // consume dot
            let seg = t.next_token()?.ok_or("Expected attribute name after '.'")?;
            path.push('.');
            path.push_str(&unquote(&seg));
        } else if next == "[" {
            t.next_token()?; // consume [
            let idx = t.next_token()?.ok_or("Expected index in '[]'")?;
            let close = t.next_token()?.ok_or("Expected ']'")?;
            if close != "]" {
                return Err(format!("Expected ']' but got '{close}'"));
            }
            path.push('[');
            path.push_str(&idx);
            path.push(']');
        } else {
            break;
        }
    }
    Ok(path)
}

/// Parse a path inside a function call (e.g. EXISTS, BEGINS_WITH, CONTAINS).
/// Supports dotted paths like `address.city`.
fn parse_function_path(t: &mut Tokenizer) -> Result<String, String> {
    let tok = t.next_token()?.ok_or("Expected path in function")?;
    parse_dotted_path_from_token(&tok, t)
}

fn expect_char(t: &mut Tokenizer, expected: &str) -> Result<(), String> {
    let tok = t.next_token()?.ok_or(format!("Expected '{expected}'"))?;
    if tok != expected {
        return Err(format!("Expected '{expected}' but got '{tok}'"));
    }
    Ok(())
}

fn parse_value(t: &mut Tokenizer) -> Result<PartiqlValue, String> {
    let tok = t.next_token()?.ok_or("Expected value")?;

    if tok == "?" {
        let idx = t.next_param_index();
        return Ok(PartiqlValue::Parameter(idx));
    }

    // String literal: 'value'
    if tok.starts_with('\'') && tok.ends_with('\'') && tok.len() >= 2 {
        let s = tok[1..tok.len() - 1].to_string();
        return Ok(PartiqlValue::Literal(AttributeValue::S(s)));
    }

    // Set literal: << val1, val2 >>
    if tok == "<" {
        if let Some(ref next) = t.peek_token()? {
            if next == "<" {
                t.next_token()?; // consume second <
                let mut elements = Vec::new();
                loop {
                    let peek = t.peek_token()?.ok_or("Unexpected end of set literal")?;
                    if peek == ">" {
                        t.next_token()?; // consume first >
                        // Consume second >
                        let next_close = t.peek_token()?;
                        if next_close.as_deref() == Some(">") {
                            t.next_token()?;
                        }
                        break;
                    }
                    if peek == "," {
                        t.next_token()?;
                        continue;
                    }
                    elements.push(parse_value(t)?);
                }
                return set_literal_to_value(elements);
            }
        }
    }

    // List literal: [val1, val2]
    if tok == "[" {
        let mut items = Vec::new();
        loop {
            let peek = t.peek_token()?.ok_or("Unexpected end of list")?;
            if peek == "]" {
                t.next_token()?;
                break;
            }
            if peek == "," {
                t.next_token()?;
                continue;
            }
            items.push(parse_value(t)?);
        }
        // We can only produce a Literal list if all elements are literals
        let mut avs = Vec::new();
        for item in items {
            match item {
                PartiqlValue::Literal(av) => avs.push(av),
                PartiqlValue::Parameter(_) => {
                    // Can't build a static list with parameters in it at parse time.
                    // For now, return an error — a more complete solution would
                    // defer resolution.
                    return Err(
                        "Parameter placeholders inside list literals are not yet supported"
                            .to_string(),
                    );
                }
            }
        }
        return Ok(PartiqlValue::Literal(AttributeValue::L(avs)));
    }

    // Map literal: { 'key': value, ... }
    if tok == "{" {
        let mut map = HashMap::new();
        loop {
            let peek = t.peek_token()?.ok_or("Unexpected end of map literal")?;
            if peek == "}" {
                t.next_token()?;
                break;
            }
            if peek == "," {
                t.next_token()?;
                continue;
            }
            let key_tok = t.next_token()?.ok_or("Expected key in map literal")?;
            let key = unquote(&key_tok);
            let colon = t.next_token()?.ok_or("Expected ':'")?;
            if colon != ":" {
                return Err(format!("Expected ':' but got '{colon}'"));
            }
            let val = parse_value(t)?;
            match val {
                PartiqlValue::Literal(av) => {
                    map.insert(key, av);
                }
                PartiqlValue::Parameter(_) => {
                    return Err(
                        "Parameter placeholders inside map literals are not yet supported"
                            .to_string(),
                    );
                }
            }
        }
        return Ok(PartiqlValue::Literal(AttributeValue::M(map)));
    }

    // Negative number: `-` followed by a numeric token
    if tok == "-" || tok == "+" {
        if let Some(ref next) = t.peek_token()? {
            if next.starts_with(|c: char| c.is_ascii_digit()) {
                let num = t.next_token()?.unwrap();
                return Ok(PartiqlValue::Literal(AttributeValue::N(format!(
                    "{tok}{num}"
                ))));
            }
        }
    }

    // Numeric literal
    if tok.starts_with(|c: char| c.is_ascii_digit()) {
        return Ok(PartiqlValue::Literal(AttributeValue::N(tok)));
    }

    // Boolean / null
    match tok.to_uppercase().as_str() {
        "TRUE" => return Ok(PartiqlValue::Literal(AttributeValue::BOOL(true))),
        "FALSE" => return Ok(PartiqlValue::Literal(AttributeValue::BOOL(false))),
        "NULL" => return Ok(PartiqlValue::Literal(AttributeValue::NULL(true))),
        _ => {}
    }

    // Bare identifier — treat as a string (attribute name reference in SET expressions)
    // This handles cases like `SET x = x + 1` where `x` on the RHS is an identifier.
    if tok
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    {
        // Consume any dotted path continuation
        let mut path = tok.clone();
        while let Some(ref next) = t.peek_token()? {
            if next == "." {
                t.next_token()?;
                let seg = t.next_token()?.ok_or("Expected attribute name after '.'")?;
                path.push('.');
                path.push_str(&unquote(&seg));
            } else {
                break;
            }
        }
        return Ok(PartiqlValue::Literal(AttributeValue::S(path)));
    }

    Err(format!("Unexpected value token: {tok}"))
}

/// Convert parsed set literal elements into a DynamoDB set type (SS, NS, or BS).
fn set_literal_to_value(elements: Vec<PartiqlValue>) -> Result<PartiqlValue, String> {
    if elements.is_empty() {
        return Err("Set literals cannot be empty".to_string());
    }

    // Determine type from first element
    let first = match &elements[0] {
        PartiqlValue::Literal(av) => av,
        PartiqlValue::Parameter(_) => {
            return Err("Parameter placeholders in set literals are not supported".to_string());
        }
    };

    match first {
        AttributeValue::S(_) => {
            let mut ss = Vec::new();
            for elem in &elements {
                match elem {
                    PartiqlValue::Literal(AttributeValue::S(s)) => ss.push(s.clone()),
                    _ => return Err("Mixed types in string set literal".to_string()),
                }
            }
            Ok(PartiqlValue::Literal(AttributeValue::SS(ss)))
        }
        AttributeValue::N(_) => {
            let mut ns = Vec::new();
            for elem in &elements {
                match elem {
                    PartiqlValue::Literal(AttributeValue::N(n)) => ns.push(n.clone()),
                    _ => return Err("Mixed types in number set literal".to_string()),
                }
            }
            Ok(PartiqlValue::Literal(AttributeValue::NS(ns)))
        }
        _ => Err(format!(
            "Unsupported element type in set literal: {first:?}"
        )),
    }
}

/// Parse a `{ 'key': 'value', ... }` item literal into a DynamoDB attribute map.
fn parse_item_literal(t: &mut Tokenizer) -> Result<HashMap<String, AttributeValue>, String> {
    let open = t.next_token()?.ok_or("Expected '{'")?;
    if open != "{" {
        return Err(format!("Expected '{{' but got '{open}'"));
    }

    let mut item = HashMap::new();

    loop {
        let tok = t.peek_token()?.ok_or("Unexpected end of item literal")?;
        if tok == "}" {
            t.next_token()?; // consume }
            break;
        }

        // Skip commas between entries
        if tok == "," {
            t.next_token()?;
            continue;
        }

        // Parse key
        let key_tok = t.next_token()?.ok_or("Expected key in item literal")?;
        let key = unquote(&key_tok);

        let colon = t.next_token()?.ok_or("Expected ':'")?;
        if colon != ":" {
            return Err(format!("Expected ':' but got '{colon}'"));
        }

        // Parse value
        let val = parse_item_value(t)?;
        item.insert(key, val);
    }

    Ok(item)
}

/// Parse a value inside an item literal (supports nested maps, lists, set literals, etc.).
fn parse_item_value(t: &mut Tokenizer) -> Result<AttributeValue, String> {
    let tok = t.peek_token()?.ok_or("Expected value")?;

    if tok == "{" {
        // Nested map
        let inner = parse_item_literal(t)?;
        return Ok(AttributeValue::M(inner));
    }

    if tok == "[" {
        // List
        t.next_token()?; // consume [
        let mut items = Vec::new();
        loop {
            let peek = t.peek_token()?.ok_or("Unexpected end of list")?;
            if peek == "]" {
                t.next_token()?;
                break;
            }
            if peek == "," {
                t.next_token()?;
                continue;
            }
            items.push(parse_item_value(t)?);
        }
        return Ok(AttributeValue::L(items));
    }

    // Set literal: << val1, val2 >>
    if tok == "<" {
        if let Some(ref next_tok) = t.peek_token_at(1)? {
            if next_tok == "<" {
                t.next_token()?; // consume first <
                t.next_token()?; // consume second <
                let mut elements = Vec::new();
                loop {
                    let peek = t.peek_token()?.ok_or("Unexpected end of set literal")?;
                    if peek == ">" {
                        t.next_token()?; // consume first >
                        if t.peek_token()?.as_deref() == Some(">") {
                            t.next_token()?; // consume second >
                        }
                        break;
                    }
                    if peek == "," {
                        t.next_token()?;
                        continue;
                    }
                    elements.push(parse_item_value(t)?);
                }
                return item_value_set_literal(elements);
            }
        }
    }

    // Scalar value
    let tok = t.next_token()?.ok_or("Expected value")?;

    // String
    if tok.starts_with('\'') && tok.ends_with('\'') && tok.len() >= 2 {
        return Ok(AttributeValue::S(tok[1..tok.len() - 1].to_string()));
    }

    // Negative number: `-` followed by a numeric token
    if tok == "-" || tok == "+" {
        if let Some(ref next) = t.peek_token()? {
            if next.starts_with(|c: char| c.is_ascii_digit()) {
                let num = t.next_token()?.unwrap();
                return Ok(AttributeValue::N(format!("{tok}{num}")));
            }
        }
    }

    // Number
    if tok.starts_with(|c: char| c.is_ascii_digit()) {
        return Ok(AttributeValue::N(tok));
    }

    match tok.to_uppercase().as_str() {
        "TRUE" => Ok(AttributeValue::BOOL(true)),
        "FALSE" => Ok(AttributeValue::BOOL(false)),
        "NULL" => Ok(AttributeValue::NULL(true)),
        _ => Err(format!("Unexpected value in item literal: {tok}")),
    }
}

/// Convert a list of item-literal values into a DynamoDB set type.
fn item_value_set_literal(elements: Vec<AttributeValue>) -> Result<AttributeValue, String> {
    if elements.is_empty() {
        return Err("Set literals cannot be empty".to_string());
    }
    match &elements[0] {
        AttributeValue::S(_) => {
            let mut ss = Vec::new();
            for e in elements {
                match e {
                    AttributeValue::S(s) => ss.push(s),
                    _ => return Err("Mixed types in string set literal".to_string()),
                }
            }
            Ok(AttributeValue::SS(ss))
        }
        AttributeValue::N(_) => {
            let mut ns = Vec::new();
            for e in elements {
                match e {
                    AttributeValue::N(n) => ns.push(n),
                    _ => return Err("Mixed types in number set literal".to_string()),
                }
            }
            Ok(AttributeValue::NS(ns))
        }
        _ => Err(format!(
            "Unsupported element type in set literal: {:?}",
            elements[0]
        )),
    }
}

/// Parse a `{ 'key': value, ... }` item literal where values may be `?` parameter placeholders.
/// Returns `PartiqlValue` wrappers so parameters can be resolved at execution time.
fn parse_item_literal_partiql(t: &mut Tokenizer) -> Result<HashMap<String, PartiqlValue>, String> {
    let open = t.next_token()?.ok_or("Expected '{'")?;
    if open != "{" {
        return Err(format!("Expected '{{' but got '{open}'"));
    }

    let mut item = HashMap::new();

    loop {
        let tok = t.peek_token()?.ok_or("Unexpected end of item literal")?;
        if tok == "}" {
            t.next_token()?; // consume }
            break;
        }

        // Skip commas between entries
        if tok == "," {
            t.next_token()?;
            continue;
        }

        // Parse key
        let key_tok = t.next_token()?.ok_or("Expected key in item literal")?;
        let key = unquote(&key_tok);

        let colon = t.next_token()?.ok_or("Expected ':'")?;
        if colon != ":" {
            return Err(format!("Expected ':' but got '{colon}'"));
        }

        // Parse value (may be a parameter placeholder)
        let val = parse_item_value_partiql(t)?;
        item.insert(key, val);
    }

    Ok(item)
}

/// Parse a value inside an item literal, supporting `?` parameter placeholders
/// and nested maps/lists (which are stored as `PartiqlValue::Literal`).
fn parse_item_value_partiql(t: &mut Tokenizer) -> Result<PartiqlValue, String> {
    let tok = t.peek_token()?.ok_or("Expected value")?;

    if tok == "?" {
        t.next_token()?; // consume ?
        let idx = t.next_param_index();
        return Ok(PartiqlValue::Parameter(idx));
    }

    // For lists, use parse_value which supports `?` inside list elements
    if tok == "[" {
        return parse_value(t);
    }

    // For nested maps, use recursive partiql parsing to support `?`
    if tok == "{" {
        // Parse nested map with partiql-aware parser
        let inner = parse_item_literal_partiql(t)?;
        // Check if all values are literals — if so, collapse to a single Literal
        let mut map = HashMap::new();
        for (k, v) in inner {
            match v {
                PartiqlValue::Literal(av) => {
                    map.insert(k, av);
                }
                PartiqlValue::Parameter(_) => {
                    // Can't represent a map with parameter values as a single Literal.
                    // For now, return an error.
                    return Err(
                        "Parameter placeholders inside nested map literals are not yet fully supported"
                            .to_string(),
                    );
                }
            }
        }
        return Ok(PartiqlValue::Literal(AttributeValue::M(map)));
    }

    // For set literals << >>, delegate to parse_item_value which handles them
    // For other scalar values, use parse_item_value and wrap
    let av = parse_item_value(t)?;
    Ok(PartiqlValue::Literal(av))
}

/// Remove surrounding single or double quotes from a string.
fn unquote(s: &str) -> String {
    // The length guard is not redundant with the tokenizer's unterminated-quote
    // rejection: a bare `"` starts and ends with the same character, so without
    // it this slices s[1..0] and panics rather than returning anything.
    if s.len() >= 2
        && ((s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')))
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn expect_keyword(t: &mut Tokenizer, kw: &str) -> Result<(), String> {
    let tok = t.next_token()?.ok_or(format!("Expected '{kw}'"))?;
    if !tok.eq_ignore_ascii_case(kw) {
        return Err(format!("Expected '{kw}' but got '{tok}'"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Simple tokenizer for PartiQL
// ---------------------------------------------------------------------------

struct Tokenizer {
    tokens: Vec<String>,
    spans: Vec<Span>,
    /// The statement as written, kept so a span can be turned into a line and
    /// column when a rejection needs one.
    input: String,
    pos: usize,
    param_counter: usize,
}

impl Tokenizer {
    fn new(input: &str) -> Result<Self, String> {
        let (tokens, spans) = tokenize(input)?;
        Ok(Self {
            tokens,
            spans,
            input: input.to_string(),
            pos: 0,
            param_counter: 0,
        })
    }

    /// The source span of the token at `index`, for the one rejection message
    /// that reports a position. Out of range yields a zero span rather than
    /// panicking, so a malformed statement still produces an error rather than
    /// bringing the process down.
    fn span_at(&self, index: usize) -> Span {
        self.spans
            .get(index)
            .copied()
            .unwrap_or(Span { start: 0, len: 0 })
    }

    /// A span as DynamoDB reports it: `line:column:length`, all 1-based, with
    /// the length in source characters.
    fn position(&self, span: Span) -> String {
        let (line, col) = span.line_col(&self.input);
        format!("{line}:{col}:{}", span.len)
    }

    fn next_token(&mut self) -> Result<Option<String>, String> {
        if self.pos >= self.tokens.len() {
            return Ok(None);
        }
        let tok = self.tokens[self.pos].clone();
        self.pos += 1;
        Ok(Some(tok))
    }

    fn peek_token(&self) -> Result<Option<String>, String> {
        if self.pos >= self.tokens.len() {
            return Ok(None);
        }
        Ok(Some(self.tokens[self.pos].clone()))
    }

    /// Peek at a token at a given offset from the current position.
    fn peek_token_at(&self, offset: usize) -> Result<Option<String>, String> {
        let idx = self.pos + offset;
        if idx >= self.tokens.len() {
            return Ok(None);
        }
        Ok(Some(self.tokens[idx].clone()))
    }

    fn next_param_index(&mut self) -> usize {
        let idx = self.param_counter;
        self.param_counter += 1;
        idx
    }
}

/// Where a token started in the source, and how long it was as written.
///
/// DynamoDB reports a table name's position when it rejects a qualified name on
/// an `INSERT`, as `line:column:length` counted from 1 with the length measured
/// in source characters (so a quoted name includes its quotes). Reproducing that
/// message means the position has to survive tokenisation. Captured eu-west-2
/// 2026-08-15.
#[derive(Debug, Clone, Copy)]
struct Span {
    /// Character offset of the token's first character.
    start: usize,
    len: usize,
}

impl Span {
    /// The 1-based line and column of the token's first character.
    ///
    /// Walked from the start of the statement on demand. Only one rejection
    /// message reports a position, so paying for it per parse would mean
    /// counting newlines for every statement to serve a case that almost never
    /// fires.
    fn line_col(&self, input: &str) -> (usize, usize) {
        let (mut line, mut col) = (1usize, 1usize);
        for c in input.chars().take(self.start) {
            if c == '\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
        }
        (line, col)
    }
}

fn emit(tokens: &mut Vec<String>, spans: &mut Vec<Span>, tok: String, start: usize, end: usize) {
    spans.push(Span {
        start,
        len: end - start,
    });
    tokens.push(tok);
}

/// Tokenise a PartiQL string into tokens and their source spans.
fn tokenize(input: &str) -> Result<(Vec<String>, Vec<Span>), String> {
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut tokens = Vec::new();
    // Roughly one token per four characters on the statements this sees; the
    // exact figure does not matter, only that it stops reallocating from zero.
    let mut spans = Vec::with_capacity(len / 4 + 1);

    let mut i = 0;

    while i < len {
        // Skip whitespace
        if chars[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }

        let start = i;

        // Single-char tokens
        match chars[i] {
            '{' | '}' | '[' | ']' | '(' | ')' | ',' | ':' | '*' | '?' | '+' | '-' | '.' => {
                // Check for multi-char - or +  as start of number? No, treat as separate.
                let tok = chars[i].to_string();
                i += 1;
                emit(&mut tokens, &mut spans, tok, start, i);
                continue;
            }
            _ => {}
        }

        // Two-char operators
        if i + 1 < len {
            let two = format!("{}{}", chars[i], chars[i + 1]);
            match two.as_str() {
                "<>" | "<=" | ">=" | "!=" => {
                    i += 2;
                    emit(&mut tokens, &mut spans, two, start, i);
                    continue;
                }
                _ => {}
            }
        }

        // Single-char operators
        if matches!(chars[i], '=' | '<' | '>') {
            let tok = chars[i].to_string();
            i += 1;
            emit(&mut tokens, &mut spans, tok, start, i);
            continue;
        }

        // String literal (single-quoted), with '' escape support
        if chars[i] == '\'' {
            let mut s = String::from('\'');
            i += 1;
            while i < len {
                if chars[i] == '\'' {
                    // Check for '' escape sequence
                    if i + 1 < len && chars[i + 1] == '\'' {
                        s.push('\'');
                        i += 2;
                    } else {
                        break; // end of string
                    }
                } else {
                    s.push(chars[i]);
                    i += 1;
                }
            }
            if i >= len {
                return Err("Unterminated string literal".to_string());
            }
            s.push('\'');
            i += 1;
            emit(&mut tokens, &mut spans, s, start, i);
            continue;
        }

        // Double-quoted identifier, with "" escape support
        if chars[i] == '"' {
            let mut s = String::from('"');
            i += 1;
            while i < len {
                if chars[i] == '"' {
                    // Check for "" escape sequence
                    if i + 1 < len && chars[i + 1] == '"' {
                        s.push('"');
                        i += 2;
                    } else {
                        break; // end of identifier
                    }
                } else {
                    s.push(chars[i]);
                    i += 1;
                }
            }
            // An unterminated identifier used to be emitted as a token that
            // still carried its opening quote. `unquote` then saw a string that
            // both starts and ends with `"` when the quote was the only
            // character, and sliced s[1..0], which panics. Rejecting it here
            // fixes the crash at its source and leaves `unquote` total.
            if i >= len {
                return Err("Unterminated quoted identifier".to_string());
            }
            s.push('"');
            i += 1;
            emit(&mut tokens, &mut spans, s, start, i);
            continue;
        }

        // Number
        if chars[i].is_ascii_digit() {
            let mut s = String::new();
            while i < len && (chars[i].is_ascii_digit() || chars[i] == '.') {
                s.push(chars[i]);
                i += 1;
            }
            emit(&mut tokens, &mut spans, s, start, i);
            continue;
        }

        // Identifier / keyword
        if chars[i].is_ascii_alphabetic() || chars[i] == '_' {
            let mut s = String::new();
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                s.push(chars[i]);
                i += 1;
            }
            emit(&mut tokens, &mut spans, s, start, i);
            continue;
        }

        // Unknown character — report an error rather than silently skipping
        return Err(format!("Unexpected character: '{}'", chars[i]));
    }

    Ok((tokens, spans))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_star() {
        let stmt = parse("SELECT * FROM \"TestTable\"").unwrap();
        match stmt {
            Statement::Select {
                table_name,
                projections,
                where_clause,
                ..
            } => {
                assert_eq!(table_name, "TestTable");
                assert!(projections.is_empty());
                assert!(where_clause.is_none());
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let stmt = parse("SELECT * FROM \"T\" WHERE pk = 'hello'").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.groups[0].len(), 1);
                match &wc.groups[0][0] {
                    WhereCondition::Comparison(c) => {
                        assert_eq!(c.path, "pk");
                        assert_eq!(c.op, CompOp::Eq);
                    }
                    _ => panic!("Expected Comparison"),
                }
            }
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_select_with_projection() {
        let stmt = parse("SELECT name, age FROM \"Users\"").unwrap();
        match stmt {
            Statement::Select { projections, .. } => {
                assert_eq!(projections, vec!["name", "age"]);
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt =
            parse("INSERT INTO \"TestTable\" VALUE {'pk': 'key1', 'data': 'hello'}").unwrap();
        match stmt {
            Statement::Insert {
                table_name, item, ..
            } => {
                assert_eq!(table_name, "TestTable");
                assert_eq!(
                    item.get("pk"),
                    Some(&PartiqlValue::Literal(AttributeValue::S(
                        "key1".to_string()
                    )))
                );
                assert_eq!(
                    item.get("data"),
                    Some(&PartiqlValue::Literal(AttributeValue::S(
                        "hello".to_string()
                    )))
                );
            }
            _ => panic!("Expected INSERT"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse("UPDATE \"T\" SET name = 'Bob' WHERE pk = 'k1'").unwrap();
        match stmt {
            Statement::Update {
                table_name,
                set_clauses,
                where_clause,
                ..
            } => {
                assert_eq!(table_name, "T");
                assert_eq!(set_clauses.len(), 1);
                assert_eq!(set_clauses[0].path, "name");
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected UPDATE"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse("DELETE FROM \"T\" WHERE pk = 'k1'").unwrap();
        match stmt {
            Statement::Delete {
                table_name,
                where_clause,
                returning,
                ..
            } => {
                assert_eq!(table_name, "T");
                assert!(where_clause.is_some());
                assert!(returning.is_none());
            }
            _ => panic!("Expected DELETE"),
        }
    }

    #[test]
    fn test_parse_delete_returning_all_old() {
        let stmt = parse("DELETE FROM \"T\" WHERE pk = 'k1' RETURNING ALL OLD *").unwrap();
        match stmt {
            Statement::Delete {
                table_name,
                where_clause,
                returning,
                ..
            } => {
                assert_eq!(table_name, "T");
                assert!(where_clause.is_some());
                assert_eq!(returning, Some(ReturningVariant::AllOld));
            }
            _ => panic!("Expected DELETE"),
        }
    }

    #[test]
    fn test_parse_delete_returning_is_case_insensitive() {
        let stmt = parse("DELETE FROM \"T\" WHERE pk = 'k1' returning all old *").unwrap();
        match stmt {
            Statement::Delete { returning, .. } => {
                assert_eq!(returning, Some(ReturningVariant::AllOld))
            }
            _ => panic!("Expected DELETE"),
        }
    }

    #[test]
    fn test_parse_delete_returning_recognises_all_variants() {
        // All four well-formed variants parse; DynamoDB's semantic rule (only
        // ALL OLD is valid on DELETE) is enforced by the executor so the
        // rejection can carry the exact validation message.
        let cases = [
            ("RETURNING ALL OLD *", ReturningVariant::AllOld),
            ("RETURNING MODIFIED OLD *", ReturningVariant::ModifiedOld),
            ("RETURNING ALL NEW *", ReturningVariant::AllNew),
            ("RETURNING MODIFIED NEW *", ReturningVariant::ModifiedNew),
        ];
        for (clause, expected) in cases {
            let stmt = parse(&format!("DELETE FROM \"T\" WHERE pk = 'k1' {clause}")).unwrap();
            match stmt {
                Statement::Delete { returning, .. } => {
                    assert_eq!(returning, Some(expected), "clause {clause}")
                }
                _ => panic!("Expected DELETE for {clause}"),
            }
        }
    }

    #[test]
    fn test_parse_update_returning_recognises_all_variants() {
        let cases = [
            ("RETURNING ALL OLD *", ReturningVariant::AllOld),
            ("RETURNING MODIFIED OLD *", ReturningVariant::ModifiedOld),
            ("RETURNING ALL NEW *", ReturningVariant::AllNew),
            ("RETURNING MODIFIED NEW *", ReturningVariant::ModifiedNew),
        ];
        for (clause, expected) in cases {
            let stmt = parse(&format!(
                "UPDATE \"T\" SET data = 'x' WHERE pk = 'k1' {clause}"
            ))
            .unwrap();
            match stmt {
                Statement::Update { returning, .. } => {
                    assert_eq!(returning, Some(expected), "clause {clause}")
                }
                _ => panic!("Expected UPDATE for {clause}"),
            }
        }
    }

    #[test]
    fn test_parse_delete_returning_rejects_incomplete_clause() {
        assert!(parse("DELETE FROM \"T\" WHERE pk = 'k1' RETURNING ALL OLD").is_err());
        assert!(parse("DELETE FROM \"T\" WHERE pk = 'k1' RETURNING").is_err());
        assert!(parse("DELETE FROM \"T\" WHERE pk = 'k1' RETURNING ALL OLD attr").is_err());
    }

    #[test]
    fn test_parse_returning_rejects_invalid_keyword_pairs() {
        // Well-formed shape (two keywords + star) but not one of the four valid
        // ALL/MODIFIED x OLD/NEW pairs -> the catch-all rejection.
        for clause in [
            "RETURNING OLD ALL *",
            "RETURNING NEW MODIFIED *",
            "RETURNING ALL ALL *",
            "RETURNING FOO BAR *",
        ] {
            let err = parse(&format!("DELETE FROM \"T\" WHERE pk = 'k1' {clause}")).unwrap_err();
            assert!(
                err.to_string().contains("Unsupported RETURNING clause"),
                "clause {clause} gave unexpected error: {err}"
            );
        }
    }

    #[test]
    fn test_parse_parameter() {
        let stmt = parse("SELECT * FROM \"T\" WHERE pk = ?").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => match &wc.groups[0][0] {
                WhereCondition::Comparison(c) => match &c.value {
                    PartiqlValue::Parameter(0) => {}
                    other => panic!("Expected Parameter(0), got {other:?}"),
                },
                _ => panic!("Expected Comparison"),
            },
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_numeric_literal() {
        let stmt = parse("SELECT * FROM \"T\" WHERE age > 42").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => match &wc.groups[0][0] {
                WhereCondition::Comparison(c) => {
                    assert_eq!(c.op, CompOp::Gt);
                    match &c.value {
                        PartiqlValue::Literal(AttributeValue::N(n)) => assert_eq!(n, "42"),
                        other => panic!("Expected N(42), got {other:?}"),
                    }
                }
                _ => panic!("Expected Comparison"),
            },
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_insert_with_number() {
        let stmt = parse("INSERT INTO \"T\" VALUE {'pk': 'k1', 'age': 25}").unwrap();
        match stmt {
            Statement::Insert { item, .. } => {
                assert_eq!(
                    item.get("age"),
                    Some(&PartiqlValue::Literal(AttributeValue::N("25".to_string())))
                );
            }
            _ => panic!("Expected INSERT"),
        }
    }

    #[test]
    fn test_invalid_statement() {
        let result = parse("MERGE INTO \"T\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_statement() {
        let result = parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_between() {
        let stmt = parse("SELECT * FROM \"T\" WHERE age BETWEEN 18 AND 65").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.groups[0].len(), 1);
                match &wc.groups[0][0] {
                    WhereCondition::Between(path, low, high) => {
                        assert_eq!(path, "age");
                        match low {
                            PartiqlValue::Literal(AttributeValue::N(n)) => assert_eq!(n, "18"),
                            other => panic!("Expected N(18), got {other:?}"),
                        }
                        match high {
                            PartiqlValue::Literal(AttributeValue::N(n)) => assert_eq!(n, "65"),
                            other => panic!("Expected N(65), got {other:?}"),
                        }
                    }
                    other => panic!("Expected Between, got {other:?}"),
                }
            }
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_between_and_other_condition() {
        let stmt = parse("SELECT * FROM \"T\" WHERE x BETWEEN 1 AND 10 AND y = 'hello'").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.groups[0].len(), 2);
                assert!(matches!(&wc.groups[0][0], WhereCondition::Between(..)));
                assert!(matches!(&wc.groups[0][1], WhereCondition::Comparison(..)));
            }
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_in() {
        let stmt = parse("SELECT * FROM \"T\" WHERE status IN ('ACTIVE', 'PENDING')").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.groups[0].len(), 1);
                match &wc.groups[0][0] {
                    WhereCondition::In(path, values) => {
                        assert_eq!(path, "status");
                        assert_eq!(values.len(), 2);
                    }
                    other => panic!("Expected In, got {other:?}"),
                }
            }
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_contains() {
        let stmt = parse("SELECT * FROM \"T\" WHERE CONTAINS(name, 'john')").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.groups[0].len(), 1);
                match &wc.groups[0][0] {
                    WhereCondition::Contains(path, val) => {
                        assert_eq!(path, "name");
                        match val {
                            PartiqlValue::Literal(AttributeValue::S(s)) => {
                                assert_eq!(s, "john")
                            }
                            other => panic!("Expected S(john), got {other:?}"),
                        }
                    }
                    other => panic!("Expected Contains, got {other:?}"),
                }
            }
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_is_missing() {
        let stmt = parse("SELECT * FROM \"T\" WHERE email IS MISSING").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.groups[0].len(), 1);
                match &wc.groups[0][0] {
                    WhereCondition::IsMissing(path) => assert_eq!(path, "email"),
                    other => panic!("Expected IsMissing, got {other:?}"),
                }
            }
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_is_not_missing() {
        let stmt = parse("SELECT * FROM \"T\" WHERE email IS NOT MISSING").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.groups[0].len(), 1);
                match &wc.groups[0][0] {
                    WhereCondition::IsNotMissing(path) => assert_eq!(path, "email"),
                    other => panic!("Expected IsNotMissing, got {other:?}"),
                }
            }
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_nested_projection() {
        let stmt = parse("SELECT a.b.c, d FROM \"T\"").unwrap();
        match stmt {
            Statement::Select { projections, .. } => {
                assert_eq!(projections, vec!["a.b.c", "d"]);
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_array_index_projection() {
        let stmt = parse("SELECT items[0].name FROM \"T\"").unwrap();
        match stmt {
            Statement::Select { projections, .. } => {
                assert_eq!(projections, vec!["items[0].name"]);
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_update_with_remove() {
        let stmt =
            parse("UPDATE \"T\" SET name = 'Bob' REMOVE age, email WHERE pk = 'k1'").unwrap();
        match stmt {
            Statement::Update {
                set_clauses,
                remove_paths,
                where_clause,
                ..
            } => {
                assert_eq!(set_clauses.len(), 1);
                assert_eq!(remove_paths, vec!["age", "email"]);
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected UPDATE"),
        }
    }

    #[test]
    fn test_parse_update_remove_only() {
        let stmt = parse("UPDATE \"T\" REMOVE old_field WHERE pk = 'k1'").unwrap();
        match stmt {
            Statement::Update {
                set_clauses,
                remove_paths,
                ..
            } => {
                assert!(set_clauses.is_empty());
                assert_eq!(remove_paths, vec!["old_field"]);
            }
            _ => panic!("Expected UPDATE"),
        }
    }

    #[test]
    fn test_parse_set_expression_add() {
        let stmt = parse("UPDATE \"T\" SET count = count + 1 WHERE pk = 'k1'").unwrap();
        match stmt {
            Statement::Update { set_clauses, .. } => {
                assert_eq!(set_clauses.len(), 1);
                match &set_clauses[0].value {
                    SetValue::Add(attr, val) => {
                        assert_eq!(attr, "count");
                        assert_eq!(
                            val,
                            &PartiqlValue::Literal(AttributeValue::N("1".to_string()))
                        );
                    }
                    other => panic!("Expected Add, got {other:?}"),
                }
            }
            _ => panic!("Expected UPDATE"),
        }
    }

    #[test]
    fn test_parse_count_star_is_a_parse_error() {
        // The actions reject a COUNT projection before parsing; the parser has
        // no special case, so the '(' after the attribute token fails here.
        assert!(parse("SELECT COUNT(*) FROM \"T\"").is_err());
    }

    #[test]
    fn test_count_rejection_reports_the_token_position() {
        assert_eq!(
            count_projection_rejection("SELECT COUNT(*) FROM \"T\""),
            Some("Unexpected path component at 1:8:5".to_string())
        );
        // The column tracks the token, the length stays 5 regardless of case
        // or what is inside the parens.
        assert_eq!(
            count_projection_rejection("SELECT  count(pk) FROM \"T\""),
            Some("Unexpected path component at 1:9:5".to_string())
        );
    }

    #[test]
    fn test_count_rejection_tracks_lines() {
        assert_eq!(
            count_projection_rejection("SELECT\n COUNT(*) FROM \"T\""),
            Some("Unexpected path component at 2:2:5".to_string())
        );
    }

    #[test]
    fn test_count_rejection_ignores_quotes_and_word_boundaries() {
        // A quoted "COUNT" identifier, a 'count(' string literal, and a word
        // merely containing count are all fine.
        assert_eq!(
            count_projection_rejection("SELECT \"COUNT\" FROM \"T\""),
            None
        );
        assert_eq!(
            count_projection_rejection("SELECT 'count(' FROM \"T\""),
            None
        );
        assert_eq!(
            count_projection_rejection("SELECT recount(x) FROM \"T\""),
            None
        );
    }

    #[test]
    fn test_count_rejection_only_applies_before_from_on_a_select() {
        // Non-SELECT statements and anything after FROM are out of scope.
        assert_eq!(
            count_projection_rejection("UPDATE \"T\" SET count = count + 1 WHERE pk = 'k1'"),
            None
        );
        assert_eq!(
            count_projection_rejection("SELECT pk FROM \"T\" WHERE count(x)"),
            None
        );
    }

    #[test]
    fn test_parse_set_literal() {
        let stmt = parse("INSERT INTO \"T\" VALUE {'pk': 'k1', 'tags': <<'a', 'b'>>}").unwrap();
        match stmt {
            Statement::Insert { item, .. } => match item.get("tags") {
                Some(PartiqlValue::Literal(AttributeValue::SS(ss))) => {
                    assert!(ss.contains(&"a".to_string()));
                    assert!(ss.contains(&"b".to_string()));
                }
                other => panic!("Expected SS, got {other:?}"),
            },
            _ => panic!("Expected INSERT"),
        }
    }

    #[test]
    fn test_parse_or_condition() {
        let stmt = parse("SELECT * FROM \"T\" WHERE status = 'A' OR status = 'B'").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.groups.len(), 2);
                assert_eq!(wc.groups[0].len(), 1);
                assert_eq!(wc.groups[1].len(), 1);
            }
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_and_or_mixed() {
        let stmt = parse("SELECT * FROM \"T\" WHERE a = 1 AND b = 2 OR c = 3").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.groups.len(), 2);
                assert_eq!(wc.groups[0].len(), 2); // a = 1 AND b = 2
                assert_eq!(wc.groups[1].len(), 1); // c = 3
            }
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    #[test]
    fn test_parse_insert_if_not_exists() {
        let stmt =
            parse("INSERT INTO \"T\" VALUE {'pk': 'k1', 'name': 'A'} IF NOT EXISTS").unwrap();
        match stmt {
            Statement::Insert { if_not_exists, .. } => {
                assert!(if_not_exists);
            }
            _ => panic!("Expected INSERT"),
        }
    }

    #[test]
    fn test_parse_nested_path_in_where_function() {
        let stmt = parse("SELECT * FROM \"T\" WHERE BEGINS_WITH(address.city, 'Lon')").unwrap();
        match stmt {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => match &wc.groups[0][0] {
                WhereCondition::BeginsWith(path, _) => {
                    assert_eq!(path, "address.city");
                }
                other => panic!("Expected BeginsWith, got {other:?}"),
            },
            _ => panic!("Expected SELECT with WHERE"),
        }
    }

    // An index qualifier is `"table"."index"`. The tokenizer emits `.` as its
    // own token, so a table name parser that takes one token leaves `.` and the
    // index name in the stream, and whatever clause follows is read against
    // those instead. Every statement kind goes through `parse_table_name`, so
    // all four lose their clauses the same way. Captured eu-west-2 2026-08-15.

    #[test]
    fn select_index_qualifier_keeps_the_where_clause() {
        let stmt = parse("SELECT * FROM \"T\".\"idx\" WHERE pk = 'p'").unwrap();
        match stmt {
            Statement::Select {
                table_name,
                where_clause,
                ..
            } => {
                assert_eq!(table_name, "T");
                assert!(
                    where_clause.is_some(),
                    "the qualifier stranded the WHERE clause"
                );
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn delete_index_qualifier_keeps_the_where_clause() {
        let stmt = parse("DELETE FROM \"T\".\"idx\" WHERE pk = 'p'").unwrap();
        match stmt {
            Statement::Delete {
                table_name,
                where_clause,
                ..
            } => {
                assert_eq!(table_name, "T");
                // The parser accepts a DELETE with no WHERE and the executor
                // rejects it, so a stranded clause surfaces there as "DELETE
                // requires a WHERE clause" on a statement that plainly has one.
                assert!(
                    where_clause.is_some(),
                    "the qualifier stranded the WHERE clause"
                );
            }
            _ => panic!("Expected DELETE"),
        }
    }

    #[test]
    fn update_index_qualifier_keeps_its_clauses() {
        let stmt = parse("UPDATE \"T\".\"idx\" SET a = 'x' WHERE pk = 'p'")
            .expect("the qualifier stranded the SET clause");
        match stmt {
            Statement::Update {
                table_name,
                set_clauses,
                where_clause,
                ..
            } => {
                assert_eq!(table_name, "T");
                assert_eq!(set_clauses.len(), 1);
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected UPDATE"),
        }
    }

    #[test]
    fn select_index_qualifier_is_captured() {
        let stmt = parse("SELECT * FROM \"T\".\"idx\"").unwrap();
        assert_eq!(index_name(&stmt), Some("idx"));
        assert_eq!(table_name(&stmt), Some("T"));
    }

    #[test]
    fn select_without_a_qualifier_carries_no_index() {
        let stmt = parse("SELECT * FROM \"T\" WHERE pk = 'p'").unwrap();
        assert_eq!(index_name(&stmt), None);
    }

    #[test]
    fn unquoted_index_qualifier_parses() {
        let stmt = parse("SELECT * FROM T.idx").unwrap();
        assert_eq!(table_name(&stmt), Some("T"));
        assert_eq!(index_name(&stmt), Some("idx"));
    }

    #[test]
    fn write_statements_carry_the_qualifier_for_the_executor_to_reject() {
        // AWS rejects a qualifier on all three, but UPDATE and DELETE parse it
        // and reject semantically while INSERT fails at parse. Parsing it here
        // lets the executor carry DynamoDB's own message rather than a parse
        // error, which is how the RETURNING variants are already handled.
        let stmt = parse("UPDATE \"T\".\"idx\" SET a = 'x' WHERE pk = 'p'").unwrap();
        assert_eq!(index_name(&stmt), Some("idx"));
        let stmt = parse("DELETE FROM \"T\".\"idx\" WHERE pk = 'p'").unwrap();
        assert_eq!(index_name(&stmt), Some("idx"));
    }

    #[test]
    fn insert_rejects_a_qualifier_and_reports_the_table_name_position() {
        // `line:column:length`, counted from 1, with the length measured in
        // source characters so a quoted name includes its quotes. Every figure
        // below was read off eu-west-2 on 2026-08-15.
        let err = parse("INSERT INTO \"abcdefgh\".\"idx\" VALUE {'pk':'a'}").unwrap_err();
        // Bare, not wrapped: AWS reports this one on its own terms.
        assert_eq!(
            err,
            ParseError::Validation(
                "FROM clause may only contain a single table name in data \
                 manipulation statements at 1:13:10"
                    .to_string()
            )
        );

        // The column tracks real whitespace rather than being a constant.
        let err = parse("INSERT   INTO   \"abcdefgh\".\"idx\" VALUE {'pk':'a'}").unwrap_err();
        assert!(err.to_string().ends_with("at 1:17:10"), "got {err}");

        // A newline before the table name moves the line and resets the column.
        let err = parse("INSERT INTO\n\"abcdefgh\".\"idx\" VALUE {'pk':'a'}").unwrap_err();
        assert!(err.to_string().ends_with("at 2:1:10"), "got {err}");

        // An unquoted name is measured without quotes it never had.
        let err = parse("INSERT INTO abcdefgh.idx VALUE {'pk':'a'}").unwrap_err();
        assert!(err.to_string().ends_with("at 1:13:8"), "got {err}");
    }

    #[test]
    fn insert_without_a_qualifier_is_unaffected() {
        let stmt = parse("INSERT INTO \"T\" VALUE {'pk':'a'}").unwrap();
        assert_eq!(table_name(&stmt), Some("T"));
        assert_eq!(index_name(&stmt), None);
    }

    #[test]
    fn an_unterminated_quote_is_rejected_rather_than_panicking() {
        // `unquote` sliced s[1..0] on a lone `"`, which panics, and the release
        // profile sets panic = "abort", so this aborted the server process. The
        // bare-table form predates the index qualifier; the qualifier added a
        // second way in.
        for stmt in [
            "SELECT * FROM \"",
            "SELECT * FROM \"T\".\"",
            "SELECT * FROM \"unterminated",
            "SELECT * FROM \"T\" WHERE pk = 'oops",
        ] {
            let err = parse(stmt).unwrap_err();
            assert!(
                matches!(err, ParseError::Syntax(_)),
                "{stmt:?} should be a syntax error, got {err:?}"
            );
        }
    }

    #[test]
    fn unquote_is_total() {
        // Called on every name the parser reads, so it must not panic on any
        // token the tokenizer can produce.
        for input in ["", "\"", "'", "\"\"", "''", "\"a\"", "a"] {
            let _ = unquote(input);
        }
    }

    #[test]
    fn a_trailing_dot_with_no_index_name_is_an_error() {
        assert!(parse("SELECT * FROM \"T\".").is_err());
    }

    #[test]
    fn an_empty_index_component_is_rejected() {
        let err = parse("SELECT * FROM \"T\".\"\"").unwrap_err();
        assert_eq!(
            err,
            ParseError::Validation("Path component cannot be an empty string".to_string())
        );
    }

    #[test]
    fn a_three_part_name_is_rejected() {
        let err = parse("SELECT * FROM \"T\".\"idx\".\"more\"").unwrap_err();
        assert_eq!(
            err,
            ParseError::Validation(
                "A path may contain at most 2 components in the FROM clause".to_string()
            )
        );
    }
}
