//! PartiQL statement parser.
//!
//! Parses a subset of PartiQL relevant to DynamoDB:
//! - `SELECT [projections] FROM "table" [WHERE conditions]`
//! - INSERT INTO "table" VALUE { ... } [IF NOT EXISTS]
//! - UPDATE "table" SET path = value [REMOVE attr1] [REMOVE attr2] [WHERE conditions]
//! - DELETE FROM "table" [WHERE conditions]

use crate::types::AttributeValue;
use std::collections::HashMap;
use std::sync::Arc;

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
#[derive(Debug, Clone)]
pub enum Statement {
    Select {
        table_name: String,
        /// The index named by a `"table"."index"` qualifier, if any.
        index_name: Option<String>,
        projections: Vec<String>, // empty = SELECT *
        where_clause: Option<WhereClause>,
        /// The `ORDER BY` terms, in the order written. Empty when there is no
        /// clause.
        order_by: Vec<OrderByTerm>,
    },
    Insert {
        table_name: String,
        item: HashMap<String, PartiqlValue>,
        if_not_exists: bool,
    },
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

/// One term of a SELECT's `ORDER BY` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByTerm {
    /// The attribute path the term names, unquoted.
    pub path: String,
    /// Whether the term was written `DESC`. `ASC`, the default, reads as false.
    pub descending: bool,
    /// DynamoDB's rejection when the term is not a plain attribute name: a
    /// nested path or a string. It orders by a top-level attribute only, and
    /// says so naming where the term starts, which only the parser knows. It
    /// is raised after the table is resolved, so it is carried rather than
    /// returned from the parse.
    pub rejection: Option<String>,
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

/// The table and index a `SELECT`'s `FROM` clause names, read without parsing
/// the rest of the statement. A statement refused before it is parsed, such as
/// one with a `COUNT` projection, still has its index checked first when that
/// index is one DynamoDB refuses every read of.
pub(crate) fn select_target(input: &str) -> Option<(String, Option<String>)> {
    let mut t = Tokenizer::new(input).ok()?;
    if !is_keyword(&t.next_token().ok()??, "SELECT") {
        return None;
    }
    // A quoted identifier comes back with its quotes, so only the keyword
    // matches here.
    while !is_keyword(&t.next_token().ok()??, "FROM") {}
    let target = parse_table_name(&mut t).ok()?;
    Some((target.name, target.index))
}

/// DynamoDB's rejection message for a call in a SELECT list, if the statement
/// carries one.
///
/// DynamoDB's PartiQL takes no function in a projection, aggregate or not: the
/// first call is rejected with a bare `ValidationException` reading
/// `Unexpected path component at <line>:<column>:<length>`, where line and
/// column are the 1-based position of the function's name and the length is
/// the name's, whatever is inside the parentheses: 5 for `COUNT`, 16 for
/// `attribute_exists`. The rejection fires before the table-existence check
/// (captured eu-west-2, 2026-07-29, for COUNT; eu-west-2 and us-east-1,
/// 2026-10-09, for the others).
///
/// Returns `Some(message)` when `statement` is a SELECT and, before its first
/// unquoted `FROM`, an unquoted word (any case) is followed by optional
/// whitespace and `(`. String literals and quoted identifiers are skipped, so
/// a `'count('` literal or a quoted `"COUNT"` attribute name never triggers.
/// Returns `None` otherwise.
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
            let mut j = i;
            while j < len && chars[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < len && chars[j] == '(' {
                let length = word.chars().count();
                return Some(format!(
                    "Unexpected path component at {word_line}:{word_col}:{length}"
                ));
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

/// A term of a SET assignment's right-hand side: an attribute of the item,
/// written bare, a value, or a call to `list_append` or `if_not_exists`. A
/// quoted string is a value, never an attribute's name.
#[derive(Debug, Clone, PartialEq)]
pub enum SetTerm {
    Path(String),
    Value(PartiqlValue),
    /// `list_append(a, b)`: the two lists joined, `a` first.
    ListAppend(Box<SetTerm>, Box<SetTerm>),
    /// `if_not_exists(path, value)`: the attribute at `path` when the item
    /// has it, and `value` when it doesn't.
    IfNotExists(String, Box<SetTerm>),
    /// Terms added and subtracted, as an operand of `list_append` or
    /// `if_not_exists`: `if_not_exists(q, n + 1)`.
    Sum(Vec<SumTerm>),
}

/// Whether a term of a SET assignment's arithmetic is added or subtracted, or
/// which of `+` and `-` was written beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetSign {
    Plus,
    Minus,
}

/// One term of a SET sum.
#[derive(Debug, Clone, PartialEq)]
pub struct SumTerm {
    /// Whether the term is added or subtracted once any brackets are opened.
    pub sign: SetSign,
    /// The operator a type fault in the term names: the one written beside
    /// it, which brackets don't flip.
    pub named: SetSign,
    pub term: SetTerm,
}

/// A value on the right-hand side of a SET assignment.
#[derive(Debug, Clone, PartialEq)]
pub enum SetValue {
    /// One term: a literal, a parameter, an attribute of the item, which is
    /// copied, or a call.
    Simple(SetTerm),
    /// Terms added and subtracted in turn, `n + 1`, `1 + 1 + 1`, `n - (1 +
    /// m)`, `if_not_exists(n, 0) + 1`.
    Sum(Vec<SumTerm>),
    /// `set_add(path, set)` or `set_delete(path, set)`: the set at `path`,
    /// which the call assigns back to, with the members added or removed.
    /// `refusal_at` is where DynamoDB points when the members turn out not to
    /// be a set, which a parameter only shows once it is bound.
    SetCall {
        delete: bool,
        path: String,
        members: PartiqlValue,
        refusal_at: String,
    },
}

/// A WHERE clause with OR-group semantics.
///
/// Groups are OR-joined; conditions within each group are AND-joined.
/// `WHERE a = 1 AND b = 2 OR c = 3` parses as `[[a=1, b=2], [c=3]]`.
#[derive(Debug, Clone)]
pub struct WhereClause {
    /// OR-groups: outer = OR, inner = AND.
    ///
    /// Each condition is shared rather than owned. Distributing an AND over
    /// ORs repeats a condition in every group it lands in, so a long literal
    /// or path written once would otherwise be held once per group; shared, it
    /// is held once however many groups name it.
    pub groups: Vec<Vec<Arc<WhereCondition>>>,
    /// False when expansion exceeded the planning budget. `groups` then
    /// contains only conditions required by every branch; `tree` remains the
    /// complete filter and must be used for evaluation and operand checks.
    pub(crate) fully_expanded: bool,
    /// The equalities among the clause's top-level conditions: those joined to
    /// the rest by `AND` alone, outside any `OR` or `NOT`, parentheses aside.
    /// An UPDATE or DELETE takes its key from these and only these, as
    /// DynamoDB does: `pk = 'a' OR pk = 'a'` names one item but is refused,
    /// and `pk = 'a' AND (b = 1 OR c = 2)` is accepted (captured eu-west-2 and
    /// us-east-1, 2026-10-09).
    pub(crate) top_level_equalities: Vec<Condition>,
    /// The clause as written once `NOT` has been pushed down to its
    /// conditions, before it is flattened into `groups`. The overlap check
    /// reads it, because DynamoDB splits a read only on an OR with a key
    /// condition under it (captured eu-west-2 and us-east-1, 2026-10-09).
    pub(crate) tree: ConditionTree,
    /// The clause as written, before `NOT` is pushed down. A `NOT` over an OR
    /// pushed down reads as an AND of negations, which can't be told apart
    /// from the same AND written out, yet DynamoDB refuses one and runs the
    /// other when they name a key attribute (captured eu-west-2 and
    /// us-east-1, 2026-11-05). The key-filter check reads this instead.
    pub(crate) written: WrittenClause,
}

/// A WHERE clause's shape as written: its ANDs, ORs and `NOT`s, with each
/// condition as parsed. Parentheses leave no node of their own.
#[derive(Debug, Clone)]
pub(crate) enum WrittenClause {
    Leaf(Arc<WhereCondition>),
    And(Vec<WrittenClause>),
    Or(Vec<WrittenClause>),
    Not(Box<WrittenClause>),
    /// A value standing where a condition should, `size(s) AND ...`. Its own
    /// operands are checked where it stands; the operator it stands under
    /// refuses it after its other terms, as a [`WhereCondition::Refused`]
    /// leaf. Only a refused clause holds one.
    Operand(Operand),
}

impl WrittenClause {
    fn of(expr: &BoolExpr) -> Self {
        match expr {
            BoolExpr::Leaf(cond) => WrittenClause::Leaf(Arc::new(cond.clone())),
            BoolExpr::And(terms) => Self::joined(terms, "AND", WrittenClause::And),
            BoolExpr::Or(terms) => Self::joined(terms, "OR", WrittenClause::Or),
            BoolExpr::Not(inner) => match inner.as_ref() {
                BoolExpr::NonBoolean(kind, operand) => {
                    Self::non_boolean(vec![Self::Operand(operand.clone())], Some("NOT"), kind)
                }
                inner => WrittenClause::Not(Box::new(Self::of(inner))),
            },
            BoolExpr::NonBoolean(kind, operand) => {
                Self::non_boolean(vec![Self::Operand(operand.clone())], None, kind)
            }
        }
    }

    /// The terms of an AND or an OR, with the operator's refusal of the first
    /// term that is a value after them all.
    fn joined(terms: &[BoolExpr], operator: &'static str, join: fn(Vec<Self>) -> Self) -> Self {
        let mut written = Vec::with_capacity(terms.len());
        let mut kind = None;
        for term in terms {
            match term {
                BoolExpr::NonBoolean(term_kind, operand) => {
                    kind.get_or_insert(*term_kind);
                    written.push(Self::Operand(operand.clone()));
                }
                term => written.push(Self::of(term)),
            }
        }
        match kind {
            Some(kind) => Self::non_boolean(written, Some(operator), kind),
            None => join(written),
        }
    }

    /// `terms` followed by the refusal of a value of type `kind` standing
    /// where a condition should, under `operator` or as the whole clause.
    fn non_boolean(mut terms: Vec<Self>, operator: Option<&str>, kind: &str) -> Self {
        let message = match operator {
            Some(operator) => format!(
                "Incorrect operand type for operator or function; operator or function: \
                 {operator}, operand type: {kind}"
            ),
            None => "WHERE clause must contain an expression that resolves to a boolean".into(),
        };
        terms.push(WrittenClause::Leaf(Arc::new(WhereCondition::Refused(
            Box::new(Refused {
                operands: Vec::new(),
                message,
            }),
        ))));
        WrittenClause::And(terms)
    }
}

/// A WHERE clause as written, with every `NOT` pushed down to a condition.
///
/// Its leaves are the conditions `groups` shares, so the tree costs the clause
/// nothing beyond its own shape.
#[derive(Debug, Clone)]
pub(crate) enum ConditionTree {
    Leaf(Arc<WhereCondition>),
    And(Vec<ConditionTree>),
    Or(Vec<ConditionTree>),
}

impl ConditionTree {
    /// The tree of an expression `push_not_down` has already run over.
    fn of(expr: BoolExpr) -> Self {
        match expr {
            BoolExpr::Leaf(cond) => ConditionTree::Leaf(Arc::new(cond)),
            BoolExpr::And(terms) => ConditionTree::And(terms.into_iter().map(Self::of).collect()),
            BoolExpr::Or(terms) => ConditionTree::Or(terms.into_iter().map(Self::of).collect()),
            BoolExpr::Not(_) => {
                unreachable!("push_not_down removes every Not before the tree is kept")
            }
            // A clause holding one is refused before it is flattened.
            BoolExpr::NonBoolean(..) => {
                unreachable!("a clause with a value for a condition is never flattened")
            }
        }
    }

    /// Every condition, once, in statement order, without distributing AND.
    pub(crate) fn conditions(&self) -> Vec<&WhereCondition> {
        fn visit<'a>(tree: &'a ConditionTree, out: &mut Vec<&'a WhereCondition>) {
            match tree {
                ConditionTree::Leaf(condition) => out.push(condition),
                ConditionTree::And(terms) | ConditionTree::Or(terms) => {
                    for term in terms {
                        visit(term, out);
                    }
                }
            }
        }
        let mut out = Vec::new();
        visit(self, &mut out);
        out
    }

    /// Conditions that hold on every matching row. Used as a conservative
    /// read plan when the exact OR-of-ANDs would exceed the expansion budget.
    fn required_conditions(&self) -> Vec<Arc<WhereCondition>> {
        match self {
            Self::Leaf(condition) => vec![condition.clone()],
            Self::And(terms) => terms.iter().flat_map(Self::required_conditions).collect(),
            Self::Or(terms) => {
                let Some((first, rest)) = terms.split_first() else {
                    return Vec::new();
                };
                let mut common = first.required_conditions();
                for term in rest {
                    let required = term.required_conditions();
                    common.retain(|condition| required.contains(condition));
                }
                common
            }
        }
    }

    /// The bytes the tree's conditions hold in paths and literal values, each
    /// counted once: every leaf is its own condition, and flattening only
    /// shares them. Saturates rather than overflowing.
    fn bytes(&self) -> usize {
        match self {
            ConditionTree::Leaf(cond) => condition_bytes(cond),
            ConditionTree::And(terms) | ConditionTree::Or(terms) => terms
                .iter()
                .fold(0usize, |total, term| total.saturating_add(term.bytes())),
        }
    }
}

/// A single condition in a WHERE clause: either a comparison or a function call.
#[derive(Debug, Clone, PartialEq)]
pub enum WhereCondition {
    Comparison(Condition),
    /// `NOT` of a comparison, in the same shape as `NotBeginsWith` and
    /// `NotContains` rather than as a flipped operator.
    ///
    /// The flip is wrong on a row that lacks the attribute. `a = 'x'` on an item
    /// with no `a` is false, so its negation is true and the row belongs in the
    /// result; rewriting it to `a <> 'x'` asks a comparison of a value that is
    /// not there, which is false again, and the row is dropped. Wrapping keeps
    /// the missing path a match, which is also how `NOT CONTAINS` already
    /// behaved, so the two now agree.
    NotComparison(Condition),
    Exists(String),
    NotExists(String),
    BeginsWith(String, PartiqlValue),
    NotBeginsWith(String, PartiqlValue),
    Between(String, PartiqlValue, PartiqlValue),
    /// `NOT path BETWEEN low AND high`, kept whole so its bounds are checked
    /// as BETWEEN's are. A row without the attribute matches, as it does for
    /// the other negated conditions.
    NotBetween(String, PartiqlValue, PartiqlValue),
    In(String, Vec<PartiqlValue>),
    Contains(String, PartiqlValue),
    /// `NOT CONTAINS(path, value)`. Reachable only through a `NOT` in front of
    /// a `CONTAINS`, which De Morgan may also produce from a negated group.
    NotContains(String, PartiqlValue),
    IsMissing(String),
    IsNotMissing(String),
    /// `path IS <type>` for a type other than MISSING.
    IsType(String, TypeTest),
    /// `path IS NOT <type>`: true for a missing attribute too.
    IsNotType(String, TypeTest),
    /// A comparison whose sides aren't a path and a value: `size(s) = 3`,
    /// `'a' = 'b'`, `s = s2`.
    Compare(Operand, CompOp, Operand),
    /// `operand BETWEEN low AND high` for an operand that isn't a path.
    CompareBetween(Operand, Operand, Operand),
    /// `operand IN [...]` for an operand that isn't a path.
    CompareIn(Operand, Vec<Operand>),
    /// `attribute_type(operand, type)`: the operand's DynamoDB type is `type`.
    /// A type that isn't a value is refused before the statement runs.
    AttributeType(Operand, Operand),
    /// `begins_with(a, b)` or `contains(a, b)` on operands other than a path
    /// and a value, which keep the `BeginsWith` and `Contains` forms the key
    /// pushdown reads: `begins_with('hello', s)`, `contains(?, ?)`,
    /// `begins_with(s, attribute_exists(n))`.
    Search(Search, Operand, Operand),
    /// A negated condition with no negated form of its own.
    Not(Box<WhereCondition>),
    /// A condition DynamoDB refuses for itself, once its operands are checked:
    /// a call given the wrong number of operands, an operand that isn't
    /// distinct, an IN with too many. Never evaluated: the statement is
    /// refused before it runs.
    Refused(Box<Refused>),
}

/// The two condition functions that search one operand for another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Search {
    BeginsWith,
    Contains,
}

impl Search {
    /// The function's name, as DynamoDB writes it in a refusal.
    pub fn name(self) -> &'static str {
        match self {
            Search::BeginsWith => "begins_with",
            Search::Contains => "contains",
        }
    }
}

/// What DynamoDB refuses a call or an operator for, with the operands it was
/// written with. The operands are checked first, in the order written, so a
/// refusal inside one of them is the one reported.
#[derive(Debug, Clone, PartialEq)]
pub struct Refused {
    pub operands: Vec<Operand>,
    pub message: String,
}

/// One side of a comparison that isn't `path op value`.
#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Path(String),
    Value(PartiqlValue),
    /// `size(operand)`: a string's length in UTF-16 code units, a binary's in
    /// bytes, and a set's, list's or map's member count; nothing for any other
    /// value, which then compares as a missing one does.
    Size(Box<Operand>),
    /// A condition function's call, `attribute_exists(a)` and the like, as
    /// the boolean it returns for an item.
    Condition(Box<WhereCondition>),
    /// A call DynamoDB refuses for itself, `size(s, s)`. Never evaluated.
    Refused(Box<Refused>),
}

/// What `IS <type>` tests in a WHERE clause: the types DynamoDB supports there
/// other than MISSING, which has its own condition (captured eu-west-2 and
/// us-east-1, 2026-10-09).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeTest {
    Null,
    Boolean,
    Blob,
    String,
    Numeric,
    /// `STRUCT` or `TUPLE`: a map.
    Struct,
    List,
}

impl TypeTest {
    /// Whether a stored value is of this type. A set is none of them.
    pub fn matches(self, value: &AttributeValue) -> bool {
        matches!(
            (self, value),
            (TypeTest::Null, AttributeValue::NULL(_))
                | (TypeTest::Boolean, AttributeValue::BOOL(_))
                | (TypeTest::Blob, AttributeValue::B(_))
                | (TypeTest::String, AttributeValue::S(_))
                | (TypeTest::Numeric, AttributeValue::N(_))
                | (TypeTest::Struct, AttributeValue::M(_))
                | (TypeTest::List, AttributeValue::L(_))
        )
    }
}

/// A comparison condition (path op value).
#[derive(Debug, Clone, PartialEq)]
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

/// A value in a PartiQL expression: either a literal or a parameter placeholder.
#[derive(Debug, Clone, PartialEq)]
pub enum PartiqlValue {
    Literal(AttributeValue),
    Parameter(usize), // 0-based index into the Parameters array
}

/// Why a statement could not be used: it did not parse, or it parsed and a
/// literal in it is one DynamoDB refuses. The two are kept apart because a
/// transaction reports the second against the statement's index.
pub(crate) enum ParseFailure {
    Statement(ParseError),
    Literal(String),
}

impl From<ParseError> for ParseFailure {
    fn from(error: ParseError) -> Self {
        ParseFailure::Statement(error)
    }
}

impl From<String> for ParseFailure {
    fn from(message: String) -> Self {
        ParseFailure::Statement(ParseError::Syntax(message))
    }
}

impl From<&str> for ParseFailure {
    fn from(message: &str) -> Self {
        ParseFailure::Statement(ParseError::Syntax(message.to_string()))
    }
}

/// Parse a PartiQL statement string with no parameters, for tests, and make
/// the WHERE clause check a request makes, so a statement holding a fault
/// that check finds is refused as a request would be, along with a refused
/// `set_add` or `set_delete` call. A request is read through
/// `executor::checked_statement`.
#[cfg(test)]
pub(crate) fn parse(input: &str) -> Result<Statement, ParseError> {
    let parsed = parse_statement(input).map_err(|failure| match failure {
        ParseFailure::Statement(error) => error,
        ParseFailure::Literal(message) => ParseError::Validation(message),
    })?;
    let fault =
        crate::partiql::executor::where_clause_refusal(&parsed.statement).or(parsed.set_refusal);
    match fault {
        Some(message) => Err(ParseError::Validation(message)),
        None => Ok(parsed.statement),
    }
}

/// DynamoDB's refusal of a statement with more `?` than the request has
/// parameters.
pub(crate) const PARAMETER_COUNT_MISMATCH: &str =
    "Number of parameters in request and statement don't match.";

/// A statement as read, before the checks DynamoDB makes of a statement that
/// reads: how many `?` placeholders it holds, and the first refusal of a
/// `set_add` or `set_delete` call in its SET clause, reported after the WHERE
/// clause's operands are checked (captured eu-west-2, 2026-11-11).
pub(crate) struct ParsedStatement {
    pub(crate) statement: Statement,
    pub(crate) placeholders: usize,
    pub(crate) set_refusal: Option<String>,
}

/// Parse a PartiQL statement, saying whether a failure came from a literal
/// DynamoDB refuses. A request's statement is read through
/// `executor::checked_statement`, which makes the checks that follow.
pub(crate) fn parse_statement(input: &str) -> Result<ParsedStatement, ParseFailure> {
    let mut tokenizer = Tokenizer::new(input)?;
    let first = tokenizer
        .next_token()?
        .ok_or("Empty statement")?
        .to_uppercase();

    // A verb with nothing it needs after it is refused as DynamoDB refuses it:
    // an INSERT with no INTO is a keyword where a term should be, and the
    // others name what is missing (captured eu-west-2 and us-east-1,
    // 2026-10-09).
    let next = tokenizer.peek_token()?;
    let statement = match first.as_str() {
        "SELECT" if next.is_none() => Err(UNEXPECTED_TERM.into()),
        "SELECT" => parse_select(&mut tokenizer),
        "INSERT" if !next.as_deref().is_some_and(|tok| is_keyword(tok, "INTO")) => {
            Err(UNEXPECTED_KEYWORD.into())
        }
        "INSERT" => parse_insert(&mut tokenizer),
        "UPDATE"
            if !next.as_deref().is_some_and(|tok| {
                tok.starts_with('"')
                    || tok.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            }) =>
        {
            Err(EXPECTED_SIMPLE_PATH.into())
        }
        "UPDATE" => parse_update(&mut tokenizer),
        "DELETE" if !next.as_deref().is_some_and(|tok| is_keyword(tok, "FROM")) => {
            Err("Expected FROM after DELETE".into())
        }
        "DELETE" => parse_delete(&mut tokenizer),
        "FROM" if next.as_deref().is_some_and(is_name) => parse_from_first(&mut tokenizer),
        // A statement that does not begin with a DML keyword is not valid
        // PartiQL, and DynamoDB says why by reading it as an expression.
        _ => {
            tokenizer.pos = 0;
            Err(refuse_non_dml(&mut tokenizer)?)
        }
    }?;

    // Every token has to belong to the statement. Without this check a token
    // left over after the WHERE expression was dropped, so `WHERE pk = 1 foo`
    // ran as `WHERE pk = 1`, where DynamoDB refuses it (captured eu-west-2,
    // 2026-10-08, for the WHERE case; the same rule is applied to the other
    // statements). One semicolon may end the statement; anything after it is
    // a second statement (captured eu-west-2 and us-east-1, 2026-10-09).
    end_of_statement(&mut tokenizer)?;

    // A term or literal DynamoDB refuses is refused once the statement has
    // parsed and before anything is looked up, without the syntax envelope:
    // the statement checks, a missing table and a missing item all come
    // second.
    // A literal's message is DynamoDB's own, with the path inside the value,
    // `under root.x` (captured eu-west-2 and us-east-1, 2026-10-09).
    if let Some(failure) = tokenizer.first_refusal() {
        return Err(failure);
    }

    Ok(ParsedStatement {
        statement,
        placeholders: tokenizer.param_counter,
        set_refusal: tokenizer.set_refusal,
    })
}

/// Check that the statement has ended: nothing left, or one semicolon and
/// nothing after it.
fn end_of_statement(t: &mut Tokenizer) -> Result<(), String> {
    match t.peek_token()?.as_deref() {
        None => Ok(()),
        Some(";") => {
            t.next_token()?;
            if t.peek_token()?.is_some() {
                Err("Unexpected token after semicolon. (Only one query is allowed.)".to_string())
            } else {
                Ok(())
            }
        }
        Some(_) => Err("Unexpected token after expression".to_string()),
    }
}

/// What a statement read as an expression turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprKind {
    /// A number, which a sign in front of it stays part of; reported as `Lit`.
    Number,
    Lit,
    Missing,
    List,
    Struct,
    Bag,
    Parameter,
    Id,
    Path,
    IsType,
    SimpleCase,
    SearchedCase,
    /// An operator, a comparison, a function call or a subquery: anything else.
    Other,
}

/// Why a statement could not be read as an expression, in DynamoDB's words.
type ExprError = &'static str;

const UNEXPECTED_TERM: ExprError = "Unexpected term";
const UNEXPECTED_KEYWORD: ExprError = "Unexpected keyword";
const UNEXPECTED_OPERATOR: ExprError = "Unexpected operator";
const MISSING_RIGHT_HAND_SIDE: ExprError = "Missing right-hand side expression of infix operator";
const EXPECTED_SIMPLE_PATH: ExprError = "Expected identifier for simple path";

/// What reading part of an expression gave: what it was, or where it broke
/// off. The outer error is the tokenizer's.
type ExprResult = Result<Result<ExprKind, ExprError>, String>;

/// The kind a part of an expression read as, or its error returned at once.
macro_rules! term {
    ($read:expr) => {
        match $read? {
            Ok(kind) => kind,
            Err(e) => return Ok(Err(e)),
        }
    };
}

/// Words that work as operators: one of them where a term should start is an
/// unexpected operator.
const OPERATOR_WORDS: &[&str] = &["AND", "OR", "NOT", "IS", "IN", "LIKE", "BETWEEN"];

/// Words that can't start a term; one where a term should start is an
/// unexpected keyword. The ones that can (`SELECT`, `CASE`, `EXISTS (`,
/// `FROM`, the literals) are read before this list is consulted.
const KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "INTO",
    "VALUE",
    "VALUES",
    "UPDATE",
    "DELETE",
    "SET",
    "REMOVE",
    "ORDER",
    "BY",
    "GROUP",
    "HAVING",
    "LIMIT",
    "AS",
    "AT",
    "ON",
    "RETURNING",
    "UNION",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "ASC",
    "DESC",
    "EXISTS",
    "TRUE",
    "FALSE",
    "NULL",
    "MISSING",
];

/// Keywords that can't start a WHERE condition: the clause and statement
/// words. The literals and `EXISTS` are left to the condition parser, which
/// reads or refuses them itself.
const CLAUSE_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "INTO",
    "VALUE",
    "VALUES",
    "UPDATE",
    "DELETE",
    "SET",
    "REMOVE",
    "ORDER",
    "BY",
    "GROUP",
    "HAVING",
    "LIMIT",
    "AS",
    "AT",
    "ON",
    "RETURNING",
    "UNION",
    "ASC",
    "DESC",
];

/// A type name as `IS` reads it.
enum TypeName {
    Missing,
    Test(TypeTest),
    /// A type PartiQL knows that DynamoDB doesn't test for in a WHERE clause,
    /// by the name its refusal gives it.
    Unsupported(&'static str),
}

/// Read the type name after `IS` or `IS NOT`: one word, `DOUBLE PRECISION` or
/// `CHARACTER VARYING`, with an optional `(n)` or `(n, m)` after it. `None`
/// when the words aren't a type name, which DynamoDB refuses as `Expected type
/// name` (captured eu-west-2 and us-east-1, 2026-10-09).
fn read_type_name(t: &mut Tokenizer) -> Result<Option<TypeName>, String> {
    use TypeName::{Missing, Test, Unsupported};
    let Some(word) = t.next_token()? else {
        return Ok(None);
    };
    let mut then = |next: &str| -> Result<bool, String> {
        let follows = t.peek_token()?.is_some_and(|tok| is_keyword(&tok, next));
        if follows {
            t.next_token()?;
        }
        Ok(follows)
    };
    let name = match word.to_ascii_uppercase().as_str() {
        "NULL" => Test(TypeTest::Null),
        "MISSING" => Missing,
        "BOOL" | "BOOLEAN" => Test(TypeTest::Boolean),
        "BLOB" => Test(TypeTest::Blob),
        "STRING" => Test(TypeTest::String),
        "NUMERIC" => Test(TypeTest::Numeric),
        "STRUCT" | "TUPLE" => Test(TypeTest::Struct),
        "LIST" => Test(TypeTest::List),
        "SMALLINT" => Unsupported("SmallintType"),
        "INT" | "INTEGER" => Unsupported("IntegerType"),
        "INT4" => Unsupported("Integer4Type"),
        "INT8" | "BIGINT" => Unsupported("Integer8Type"),
        "FLOAT" => Unsupported("FloatType"),
        "REAL" => Unsupported("RealType"),
        "DOUBLE" if then("PRECISION")? => Unsupported("DoublePrecisionType"),
        "DECIMAL" => Unsupported("DecimalType"),
        "TIMESTAMP" => Unsupported("TimestampType"),
        "DATE" => Unsupported("DateType"),
        "TIME" => Unsupported("TimeType"),
        "CHARACTER" if then("VARYING")? => Unsupported("CharacterVaryingType"),
        "CHAR" | "CHARACTER" => Unsupported("CharacterType"),
        "VARCHAR" => Unsupported("CharacterVaryingType"),
        "SYMBOL" => Unsupported("SymbolType"),
        "CLOB" => Unsupported("ClobType"),
        "SEXP" => Unsupported("SexpType"),
        "BAG" => Unsupported("BagType"),
        "ANY" => Unsupported("AnyType"),
        _ => return Ok(None),
    };
    if t.peek_token()?.as_deref() == Some("(") {
        t.next_token()?;
        loop {
            match t.next_token()?.as_deref() {
                Some(")") => break,
                Some(tok) if tok == "," || is_number_token(tok) => {}
                _ => return Ok(None),
            }
        }
    }
    Ok(Some(name))
}

fn is_keyword(tok: &str, word: &str) -> bool {
    tok.eq_ignore_ascii_case(word)
}

fn is_any(tok: &str, words: &[&str]) -> bool {
    words.iter().any(|w| is_keyword(tok, w))
}

/// An identifier: a quoted name, or a bare word that isn't a keyword.
/// The words DynamoDB's PartiQL reads as keywords, which can't be a path's dot
/// component unless quoted: `m.inner` is `Invalid path dot component` and
/// `m."inner"` reads the attribute. Each of the 249 was refused in eu-west-2 and
/// us-east-1 (2026-10-09), out of 286 tried; the date parts (`year`, `day`,
/// `hour` ...), `both`, `leading`, `trailing`, `every`, `to_string`,
/// `to_timestamp` and `txctx` were accepted, as were names DynamoDB doesn't
/// reserve. Lower case, sorted.
const DOT_COMPONENT_KEYWORDS: &[&str] = &[
    "absolute",
    "action",
    "add",
    "all",
    "allocate",
    "alter",
    "and",
    "any",
    "are",
    "as",
    "asc",
    "assertion",
    "at",
    "authorization",
    "avg",
    "bag",
    "begin",
    "between",
    "bigint",
    "bit",
    "bit_length",
    "blob",
    "bool",
    "boolean",
    "by",
    "can_cast",
    "can_lossless_cast",
    "cascade",
    "cascaded",
    "case",
    "cast",
    "catalog",
    "char",
    "char_length",
    "character",
    "character_length",
    "check",
    "clob",
    "close",
    "coalesce",
    "collate",
    "collation",
    "column",
    "commit",
    "conflict",
    "connect",
    "connection",
    "constraint",
    "constraints",
    "continue",
    "convert",
    "corresponding",
    "count",
    "create",
    "cross",
    "current",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "cursor",
    "date",
    "date_add",
    "date_diff",
    "deallocate",
    "dec",
    "decimal",
    "declare",
    "default",
    "deferrable",
    "deferred",
    "delete",
    "desc",
    "describe",
    "descriptor",
    "diagnostics",
    "disconnect",
    "distinct",
    "do",
    "domain",
    "double",
    "drop",
    "else",
    "end",
    "escape",
    "except",
    "exception",
    "exec",
    "execute",
    "exists",
    "external",
    "extract",
    "false",
    "fetch",
    "first",
    "float",
    "for",
    "foreign",
    "found",
    "from",
    "full",
    "get",
    "global",
    "go",
    "goto",
    "grant",
    "group",
    "having",
    "identity",
    "immediate",
    "in",
    "index",
    "indicator",
    "initially",
    "inner",
    "input",
    "insensitive",
    "insert",
    "int",
    "integer",
    "integer2",
    "integer4",
    "integer8",
    "intersect",
    "interval",
    "into",
    "is",
    "isolation",
    "join",
    "key",
    "language",
    "last",
    "left",
    "let",
    "level",
    "like",
    "limit",
    "list",
    "local",
    "lower",
    "match",
    "max",
    "min",
    "missing",
    "modified",
    "module",
    "names",
    "national",
    "natural",
    "nchar",
    "new",
    "next",
    "no",
    "not",
    "nothing",
    "null",
    "nullif",
    "nulls",
    "numeric",
    "octet_length",
    "of",
    "offset",
    "old",
    "on",
    "only",
    "open",
    "option",
    "or",
    "order",
    "outer",
    "output",
    "overlaps",
    "pad",
    "partial",
    "pivot",
    "position",
    "precision",
    "prepare",
    "preserve",
    "primary",
    "prior",
    "privileges",
    "procedure",
    "public",
    "read",
    "real",
    "references",
    "relative",
    "remove",
    "restrict",
    "returning",
    "revoke",
    "right",
    "rollback",
    "rows",
    "schema",
    "scroll",
    "section",
    "select",
    "session",
    "session_user",
    "set",
    "sexp",
    "size",
    "smallint",
    "some",
    "space",
    "sql",
    "sqlcode",
    "sqlerror",
    "sqlstate",
    "string",
    "struct",
    "substring",
    "sum",
    "symbol",
    "system_user",
    "table",
    "temporary",
    "then",
    "time",
    "timestamp",
    "to",
    "transaction",
    "translate",
    "trim",
    "true",
    "tuple",
    "union",
    "unique",
    "unknown",
    "unpivot",
    "update",
    "upper",
    "usage",
    "user",
    "using",
    "value",
    "values",
    "varchar",
    "varying",
    "view",
    "when",
    "whenever",
    "where",
    "with",
    "work",
    "write",
    "zone",
];

/// Refuse a dot component that is a keyword, unless it is quoted.
fn check_dot_component(segment: &str) -> Result<(), String> {
    let lower = segment.to_ascii_lowercase();
    if !segment.starts_with('"')
        && DOT_COMPONENT_KEYWORDS
            .binary_search(&lower.as_str())
            .is_ok()
    {
        return Err("Invalid path dot component".to_string());
    }
    Ok(())
}

fn is_name(tok: &str) -> bool {
    tok.starts_with('"')
        || (tok.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && !is_any(tok, KEYWORDS)
            && !is_any(tok, OPERATOR_WORDS))
}

/// The refusal for a statement that doesn't start with SELECT, INSERT, UPDATE
/// or DELETE. DynamoDB reads it as a PartiQL expression (captured eu-west-2 and
/// us-east-1, 2026-10-09):
///
/// - a whole expression is refused for what it is, without the envelope:
///   `Unsupported operation: Id` for `FOO`, `Path` for `FOO.BAR`, `Lit`,
///   `Missing`, `List`, `Struct`, `Bag`, `Parameter`, `IsType`, `SimpleCase`
///   or `SearchedCase`, and `EXISTS can only be used in ExecuteTransaction
///   write requests.` for an operator, comparison, call or subquery
///   (`FOO AND BAR`, `1 + 1`, `FOO(1)`);
/// - one followed by more tokens is a token after the expression (`INSRT INTO
///   ...`), or after a semicolon;
/// - one that breaks off is refused with the reason: an operator with nothing
///   after it, a closing bracket or keyword missing, a keyword or operator
///   where a term should start, or a `FROM` with a table after it, which
///   DynamoDB reads as the start of a statement with no verb (`SLECT * FROM t`
///   is "Expected data manipulation");
/// - `SET a = 1` and `REMOVE a` are statements with no table, and refused for
///   that once they read.
fn refuse_non_dml(t: &mut Tokenizer) -> Result<ParseError, String> {
    if t.peek_token()?
        .is_some_and(|first| is_keyword(&first, "SET") || is_keyword(&first, "REMOVE"))
    {
        if let Err(e) = read_tableless_dml(t)? {
            return Ok(ParseError::Syntax(e.to_string()));
        }
        return Ok(match end_of_statement(t) {
            Ok(()) => ParseError::Validation(
                "Only select from a single table or index is supported.".to_string(),
            ),
            Err(message) => ParseError::Syntax(message),
        });
    }
    let kind = match read_expression(t)? {
        Ok(kind) => kind,
        Err(e) => return Ok(ParseError::Syntax(e.to_string())),
    };
    if let Err(message) = end_of_statement(t) {
        return Ok(ParseError::Syntax(message));
    }
    let name = match kind {
        ExprKind::Number | ExprKind::Lit => "Lit",
        ExprKind::Missing => "Missing",
        ExprKind::List => "List",
        ExprKind::Struct => "Struct",
        ExprKind::Bag => "Bag",
        ExprKind::Parameter => "Parameter",
        ExprKind::Id => "Id",
        ExprKind::Path => "Path",
        ExprKind::IsType => "IsType",
        ExprKind::SimpleCase => "SimpleCase",
        ExprKind::SearchedCase => "SearchedCase",
        ExprKind::Other => {
            return Ok(ParseError::Validation(
                "EXISTS can only be used in ExecuteTransaction write requests.".to_string(),
            ));
        }
    };
    Ok(ParseError::Validation(format!(
        "Unsupported operation: {name}"
    )))
}

/// Read `SET path = value [, ...]` or `REMOVE path`.
fn read_tableless_dml(t: &mut Tokenizer) -> ExprResult {
    let remove = t
        .next_token()?
        .is_some_and(|verb| is_keyword(&verb, "REMOVE"));
    loop {
        match t.next_token()? {
            Some(name) if is_name(&name) => {}
            _ => return Ok(Err(EXPECTED_SIMPLE_PATH)),
        }
        term!(read_path_steps(t, ExprKind::Id));
        if remove {
            return Ok(Ok(ExprKind::Other));
        }
        if t.next_token()?.as_deref() != Some("=") {
            return Ok(Err("Expected '='"));
        }
        term!(read_expression(t));
        if t.peek_token()?.as_deref() != Some(",") {
            return Ok(Ok(ExprKind::Other));
        }
        t.next_token()?;
    }
}

/// An operand after an infix operator: missing when the statement ends there.
fn read_operand(t: &mut Tokenizer, read: fn(&mut Tokenizer) -> ExprResult) -> ExprResult {
    if t.peek_token()?.is_none() {
        return Ok(Err(MISSING_RIGHT_HAND_SIDE));
    }
    read(t)
}

/// Read with one more level of nesting. A statement nested past
/// [`MAX_EXPR_DEPTH`] (brackets, list or bag openers, CASE, path indexes) is
/// refused as too deep to parse rather than read any further, so no input can
/// run the reader out of stack. DynamoDB's own parser answers a 500 for a
/// statement of 8,192 `(` (captured eu-west-2 and us-east-1, 2026-10-10);
/// refusing it is a deliberate difference.
fn nested(t: &mut Tokenizer, read: impl FnOnce(&mut Tokenizer) -> ExprResult) -> ExprResult {
    if t.depth > MAX_EXPR_DEPTH {
        return Err(TOO_DEEP.to_string());
    }
    t.depth += 1;
    let result = read(t);
    t.depth -= 1;
    result
}

fn read_expression(t: &mut Tokenizer) -> ExprResult {
    nested(t, |t| {
        let mut kind = term!(read_and(t));
        while t.peek_token()?.is_some_and(|tok| is_keyword(&tok, "OR")) {
            t.next_token()?;
            term!(read_operand(t, read_and));
            kind = ExprKind::Other;
        }
        Ok(Ok(kind))
    })
}

fn read_and(t: &mut Tokenizer) -> ExprResult {
    let mut kind = term!(read_not(t));
    while t.peek_token()?.is_some_and(|tok| is_keyword(&tok, "AND")) {
        t.next_token()?;
        term!(read_operand(t, read_not));
        kind = ExprKind::Other;
    }
    Ok(Ok(kind))
}

fn read_not(t: &mut Tokenizer) -> ExprResult {
    // A run of NOTs is read in a loop: it nests nothing, and a run with no
    // term after it ends in "Unexpected term", as on DynamoDB.
    let mut negated = false;
    while t.peek_token()?.is_some_and(|tok| is_keyword(&tok, "NOT")) {
        t.next_token()?;
        negated = true;
    }
    let kind = term!(read_comparison(t));
    Ok(Ok(if negated { ExprKind::Other } else { kind }))
}

fn read_comparison(t: &mut Tokenizer) -> ExprResult {
    let kind = term!(read_additive(t));
    let Some(next) = t.peek_token()? else {
        return Ok(Ok(kind));
    };
    // `>>` closes a bag rather than comparing.
    let closes_bag = next == ">" && t.peek_token_at(1)?.as_deref() == Some(">");
    if !closes_bag && matches!(next.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") {
        t.next_token()?;
        term!(read_operand(t, read_additive));
        return Ok(Ok(ExprKind::Other));
    }
    if is_keyword(&next, "IS") {
        t.next_token()?;
        let negated = t.peek_token()?.is_some_and(|tok| is_keyword(&tok, "NOT"));
        if negated {
            t.next_token()?;
        }
        return Ok(match read_type_name(t)? {
            Some(_) => Ok(if negated {
                ExprKind::Other
            } else {
                ExprKind::IsType
            }),
            None => Err("Expected type name"),
        });
    }
    let negated = is_keyword(&next, "NOT");
    let operator = if negated {
        t.peek_token_at(1)?
    } else {
        Some(next)
    };
    let Some(operator) = operator else {
        return Ok(Ok(kind));
    };
    if !is_any(&operator, &["IN", "LIKE", "BETWEEN"]) {
        return Ok(Ok(kind));
    }
    if negated {
        t.next_token()?;
    }
    t.next_token()?;
    term!(read_operand(t, read_additive));
    if is_keyword(&operator, "LIKE")
        && t.peek_token()?
            .is_some_and(|tok| is_keyword(&tok, "ESCAPE"))
    {
        t.next_token()?;
        term!(read_operand(t, read_additive));
    }
    if is_keyword(&operator, "BETWEEN") {
        match t.next_token()? {
            Some(tok) if is_keyword(&tok, "AND") => {}
            _ => return Ok(Err("Expected AND keyword")),
        }
        if t.peek_token()?.is_none() {
            return Ok(Err("Expected expression after AND"));
        }
        term!(read_additive(t));
    }
    Ok(Ok(ExprKind::Other))
}

fn read_additive(t: &mut Tokenizer) -> ExprResult {
    let mut kind = term!(read_multiplicative(t));
    while t.peek_token()?.is_some_and(|tok| tok == "+" || tok == "-") {
        t.next_token()?;
        term!(read_operand(t, read_multiplicative));
        kind = ExprKind::Other;
    }
    Ok(Ok(kind))
}

fn read_multiplicative(t: &mut Tokenizer) -> ExprResult {
    let mut kind = term!(read_unary(t));
    while t.peek_token()?.is_some_and(|tok| tok == "*") {
        t.next_token()?;
        term!(read_operand(t, read_unary));
        kind = ExprKind::Other;
    }
    Ok(Ok(kind))
}

fn read_unary(t: &mut Tokenizer) -> ExprResult {
    // `NOT` can start a term too (`FOO = NOT BAR`), and signs stack (`- -1`).
    // Both are read in a loop rather than by recursion, so a long run of them
    // nests nothing and ends in "Unexpected term" if no term follows.
    let (mut negated, mut signed) = (false, false);
    loop {
        match t.peek_token()? {
            Some(tok) if is_keyword(&tok, "NOT") => negated = true,
            Some(tok) if tok == "-" || tok == "+" => signed = true,
            _ => break,
        }
        t.next_token()?;
    }
    let kind = term!(read_primary(t));
    // A sign on a number is part of the number (`-1`, `- -1`); on anything
    // else it is an operation (`-'a'`, `-FOO`), and so is a NOT.
    Ok(Ok(match kind {
        _ if negated => ExprKind::Other,
        ExprKind::Number => ExprKind::Number,
        _ if signed => ExprKind::Other,
        kind => kind,
    }))
}

/// Read the elements of a list, a bag or a call's arguments up to `close`,
/// refusing anything else after an element with `missing`.
fn read_elements(
    t: &mut Tokenizer,
    close: &[&str],
    missing: ExprError,
) -> Result<Result<(), ExprError>, String> {
    let closes = |t: &Tokenizer| -> Result<bool, String> {
        Ok(close
            .iter()
            .enumerate()
            .all(|(i, c)| t.peek_token_at(i).ok().flatten().as_deref() == Some(*c)))
    };
    let consume = |t: &mut Tokenizer| -> Result<(), String> {
        for _ in close {
            t.next_token()?;
        }
        Ok(())
    };
    if closes(t)? {
        return Ok(Ok(consume(t)?));
    }
    loop {
        if let Err(e) = read_expression(t)? {
            return Ok(Err(e));
        }
        if t.peek_token()?.as_deref() == Some(",") {
            t.next_token()?;
            continue;
        }
        if closes(t)? {
            return Ok(Ok(consume(t)?));
        }
        return Ok(Err(missing));
    }
}

/// Read a subquery after its SELECT, as far as the bracket that closes it.
fn read_subquery(t: &mut Tokenizer) -> ExprResult {
    if t.peek_token()?.is_none() {
        return Ok(Err(UNEXPECTED_TERM));
    }
    let mut depth = 0usize;
    while let Some(tok) = t.peek_token()? {
        match tok.as_str() {
            "(" => depth += 1,
            ")" if depth == 0 => break,
            ")" => depth -= 1,
            _ => {}
        }
        t.next_token()?;
    }
    Ok(Ok(ExprKind::Other))
}

/// Read `(...)` after `EXISTS`, or around an expression, up to its `)`.
fn read_parenthesised(t: &mut Tokenizer) -> ExprResult {
    let kind = if t
        .peek_token()?
        .is_some_and(|tok| is_keyword(&tok, "SELECT"))
    {
        t.next_token()?;
        term!(read_subquery(t))
    } else {
        term!(read_expression(t))
    };
    if t.next_token()?.as_deref() != Some(")") {
        return Ok(Err("Expected RIGHT_PAREN"));
    }
    Ok(Ok(kind))
}

/// Read a CASE expression after its CASE.
fn read_case(t: &mut Tokenizer) -> ExprResult {
    let Some(next) = t.peek_token()? else {
        return Ok(Err(UNEXPECTED_TERM));
    };
    let searched = is_keyword(&next, "WHEN");
    if !searched {
        term!(read_expression(t));
    }
    let mut arms = 0;
    while t.peek_token()?.is_some_and(|tok| is_keyword(&tok, "WHEN")) {
        t.next_token()?;
        term!(read_expression(t));
        if !t.next_token()?.is_some_and(|tok| is_keyword(&tok, "THEN")) {
            return Ok(Err("Expected THEN keyword"));
        }
        term!(read_expression(t));
        arms += 1;
    }
    if arms == 0 {
        return Ok(Err("Expected a WHEN clause in CASE"));
    }
    if t.peek_token()?.is_some_and(|tok| is_keyword(&tok, "ELSE")) {
        t.next_token()?;
        term!(read_expression(t));
    }
    if !t.next_token()?.is_some_and(|tok| is_keyword(&tok, "END")) {
        return Ok(Err("Expected END keyword"));
    }
    Ok(Ok(if searched {
        ExprKind::SearchedCase
    } else {
        ExprKind::SimpleCase
    }))
}

/// Read any `.name`, `.*`, `[index]` or `[*]` steps after a term; one makes it
/// a path.
fn read_path_steps(t: &mut Tokenizer, kind: ExprKind) -> ExprResult {
    let mut kind = kind;
    loop {
        match t.peek_token()?.as_deref() {
            Some(".") => {
                t.next_token()?;
                match t.next_token()? {
                    Some(step)
                        if step == "*"
                            || step.starts_with('"')
                            || step.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') => {}
                    _ => return Ok(Err("Invalid path dot component")),
                }
            }
            Some("[") => {
                t.next_token()?;
                if t.peek_token()?.as_deref() == Some("*") {
                    t.next_token()?;
                } else {
                    term!(read_expression(t));
                }
                if t.next_token()?.as_deref() != Some("]") {
                    return Ok(Err("Expected RIGHT_BRACKET"));
                }
            }
            _ => return Ok(Ok(kind)),
        }
        kind = ExprKind::Path;
    }
}

fn read_primary(t: &mut Tokenizer) -> ExprResult {
    let Some(tok) = t.next_token()? else {
        return Ok(Err(UNEXPECTED_TERM));
    };
    if is_number_token(&tok) {
        return Ok(Ok(ExprKind::Number));
    }
    // Path steps can follow any other term: `'a'.b` and `FOO(1).a` are paths.
    let kind = match tok.as_str() {
        _ if tok.starts_with('\'') && tok.len() >= 2 => ExprKind::Lit,
        _ if is_any(&tok, &["TRUE", "FALSE", "NULL"]) => ExprKind::Lit,
        _ if is_keyword(&tok, "MISSING") => ExprKind::Missing,
        "?" => ExprKind::Parameter,
        "[" => {
            if let Err(e) = read_elements(t, &["]"], "Expected RIGHT_BRACKET")? {
                return Ok(Err(e));
            }
            ExprKind::List
        }
        // An empty bag, which the tokenizer reads as `<`, `<>`, `>`.
        "<" if t.peek_token()?.as_deref() == Some("<>")
            && t.peek_token_at(1)?.as_deref() == Some(">") =>
        {
            t.next_token()?;
            t.next_token()?;
            ExprKind::Bag
        }
        "<" if t.peek_token()?.as_deref() == Some("<") => {
            t.next_token()?;
            if let Err(e) = read_elements(t, &[">", ">"], "Expected RIGHT_DOUBLE_ANGLE_BRACKET")? {
                return Ok(Err(e));
            }
            ExprKind::Bag
        }
        "(" => term!(read_parenthesised(t)),
        "{" => {
            if t.peek_token()?.as_deref() == Some("}") {
                t.next_token()?;
            } else {
                loop {
                    term!(read_expression(t));
                    if t.next_token()?.as_deref() != Some(":") {
                        return Ok(Err("Expected COLON"));
                    }
                    term!(read_expression(t));
                    match t.next_token()?.as_deref() {
                        Some(",") => {}
                        Some("}") => break,
                        _ => return Ok(Err("Expected RIGHT_CURLY")),
                    }
                }
            }
            ExprKind::Struct
        }
        _ if is_keyword(&tok, "CASE") => return read_case(t),
        _ if is_keyword(&tok, "SELECT") => return read_subquery(t),
        _ if is_keyword(&tok, "EXISTS") => {
            if t.peek_token()?.as_deref() != Some("(") {
                return Ok(Err(UNEXPECTED_KEYWORD));
            }
            t.next_token()?;
            term!(read_parenthesised(t));
            return Ok(Ok(ExprKind::Other));
        }
        _ if is_keyword(&tok, "FROM") => {
            // A FROM in a term is the start of a statement with no verb.
            return Ok(Err(if t.peek_token()?.is_some_and(|next| is_name(&next)) {
                "Expected data manipulation"
            } else {
                UNEXPECTED_TERM
            }));
        }
        "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=" => return Ok(Err(UNEXPECTED_OPERATOR)),
        _ if is_any(&tok, OPERATOR_WORDS) => return Ok(Err(UNEXPECTED_OPERATOR)),
        _ if is_any(&tok, KEYWORDS) => return Ok(Err(UNEXPECTED_KEYWORD)),
        _ if is_name(&tok) => {
            if t.peek_token()?.as_deref() == Some("(") {
                t.next_token()?;
                if let Err(e) = read_elements(t, &[")"], "Expected RIGHT_PAREN")? {
                    return Ok(Err(e));
                }
                ExprKind::Other
            } else {
                ExprKind::Id
            }
        }
        _ => return Ok(Err(UNEXPECTED_TERM)),
    };
    read_path_steps(t, kind)
}

/// Read an `ORDER BY path [ASC|DESC] [, ...]` clause. It has to be whole and
/// last, so nothing can hide behind it.
fn parse_order_by(t: &mut Tokenizer) -> Result<Vec<OrderByTerm>, String> {
    expect_keyword(t, "ORDER")?;
    expect_keyword(t, "BY")?;
    let mut terms = Vec::new();
    // Only the first rejected term is ever reported (the executor stops at
    // it), so a position is worked out for that one alone: each walks the
    // statement from its start.
    let mut rejected = false;
    loop {
        let span = t.span_at(t.pos);
        let tok = t
            .next_token()?
            .ok_or("Expected attribute path in ORDER BY")?;
        if tok.eq_ignore_ascii_case("ASC") || tok.eq_ignore_ascii_case("DESC") {
            return Err(format!(
                "Expected attribute path in ORDER BY but got '{tok}'"
            ));
        }
        // DynamoDB names what it found and where the term starts, the length
        // being the first token's as written: `ORDER BY m.x` gets `Type: PATH
        // at 1:65:1` and `ORDER BY 'sk'` gets `Type: sk at 1:65:4` (captured
        // eu-west-2 and us-east-1, 2026-10-09).
        let found = if tok.starts_with('\'') && tok.ends_with('\'') && tok.len() >= 2 {
            Some(tok[1..tok.len() - 1].to_string())
        } else if matches!(t.peek_token()?.as_deref(), Some(".") | Some("[")) {
            Some("PATH".to_string())
        } else {
            None
        };
        let rejection = found.filter(|_| !rejected).map(|found| {
            format!(
                "Argument to ORDER BY must be of type VariableReference. Type: {found} at {}",
                t.position(span)
            )
        });
        rejected |= rejection.is_some();
        let path = parse_dotted_path_from_token(&tok, t)?;
        let mut descending = false;
        if let Some(direction) = t
            .peek_token()?
            .filter(|s| s.eq_ignore_ascii_case("ASC") || s.eq_ignore_ascii_case("DESC"))
        {
            t.next_token()?; // consume the direction
            descending = direction.eq_ignore_ascii_case("DESC");
        }
        // `NULLS FIRST` and `NULLS LAST` are accepted after a term (captured
        // eu-west-2, 2026-10-09). A key is never null, so neither changes the
        // order.
        if t.peek_token()?
            .is_some_and(|s| s.eq_ignore_ascii_case("NULLS"))
        {
            t.next_token()?; // consume NULLS
            let which = t
                .next_token()?
                .ok_or("Expected FIRST or LAST after NULLS")?;
            if !(which.eq_ignore_ascii_case("FIRST") || which.eq_ignore_ascii_case("LAST")) {
                return Err(format!(
                    "Expected FIRST or LAST after NULLS but got '{which}'"
                ));
            }
        }
        terms.push(OrderByTerm {
            path,
            descending,
            rejection,
        });
        match t.peek_token()? {
            Some(ref s) if s == "," => {
                t.next_token()?; // consume comma
            }
            _ => return Ok(terms),
        }
    }
}

fn parse_select(t: &mut Tokenizer) -> Result<Statement, ParseError> {
    // Parse projections
    let projections = parse_projections(t)?;

    // Expect FROM
    expect_keyword(t, "FROM")?;
    // Nothing or a keyword where the table should be (captured eu-west-2 and
    // us-east-1, 2026-10-09).
    match t.peek_token()?.as_deref() {
        None | Some(";") => return Err(UNEXPECTED_TERM.into()),
        Some(tok) if is_any(tok, KEYWORDS) => return Err(UNEXPECTED_KEYWORD.into()),
        _ => {}
    }

    // Parse table name, plus any index the FROM clause qualified it with
    let target = parse_table_name(t)?;
    reject_alias(t)?;

    // Optional WHERE clause
    let where_clause = parse_optional_where(t)?;

    // Optional ORDER BY, which DynamoDB accepts on a SELECT only.
    let order_by = if t
        .peek_token()?
        .is_some_and(|tok| tok.eq_ignore_ascii_case("ORDER"))
    {
        parse_order_by(t)?
    } else {
        Vec::new()
    };

    Ok(Statement::Select {
        table_name: target.name,
        index_name: target.index,
        projections,
        where_clause,
        order_by,
    })
}

fn parse_projections(t: &mut Tokenizer) -> Result<Vec<String>, String> {
    let tok = t.peek_token()?.ok_or("Expected projection")?;
    // Captured eu-west-2 and us-east-1, 2026-10-09.
    if is_keyword(&tok, "FROM") {
        return Err(
            "Expected identifier or an expression but found unexpected keyword 'from' \
                    in a select list."
                .to_string(),
        );
    }

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
        let mut path = path_segment(&name);

        // Greedily consume dot-separated segments and array indexes
        loop {
            match t.peek_token()? {
                Some(ref s) if s == "." => {
                    t.next_token()?; // consume dot
                    let segment = t.next_token()?.ok_or("Expected attribute name after '.'")?;
                    check_dot_component(&segment)?;
                    path.push('.');
                    path.push_str(&path_segment(&segment));
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
    // Captured eu-west-2 and us-east-1, 2026-10-09.
    if t.peek_token()?.is_none() {
        return Err(EXPECTED_SIMPLE_PATH.into());
    }
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
    // DynamoDB reads an alias on INSERT as a token left over before VALUE,
    // not as the alias SELECT, UPDATE and DELETE refuse (captured eu-west-2,
    // 2026-10-09).
    if let Some(tok) = t.peek_token()?
        && is_alias(&tok)
        && (tok.eq_ignore_ascii_case("AS")
            || t.peek_token_at(1)?
                .is_some_and(|next| next.eq_ignore_ascii_case("VALUE")))
    {
        return Err("Unexpected token after expression".into());
    }
    expect_keyword(t, "VALUE")?;

    // Parse the item literal as a map of possibly-parameterised values
    t.clause = LiteralClause::Value;
    let item = parse_item_literal_partiql(t, 0)?;

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
    reject_alias(t)?;

    let (set_clauses, remove_paths) = parse_update_clauses(t)?;
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

/// Read an UPDATE's `SET` and `REMOVE` clauses: any number of each, in any
/// order, each a comma-separated list (captured eu-west-2 and us-east-1,
/// 2026-10-09: `SET c = 1 REMOVE a SET d = 2` and `REMOVE a REMOVE b` both
/// run). A keyword with nothing after it is a path missing.
fn parse_update_clauses(t: &mut Tokenizer) -> Result<(Vec<SetClause>, Vec<String>), String> {
    let mut set_clauses = Vec::new();
    let mut remove_paths = Vec::new();
    while let Some(keyword) = t.peek_token()? {
        let remove = if is_keyword(&keyword, "SET") {
            false
        } else if is_keyword(&keyword, "REMOVE") {
            true
        } else {
            break;
        };
        t.next_token()?;
        loop {
            let path_tok = t.next_token()?.ok_or(EXPECTED_SIMPLE_PATH)?;
            let path = parse_dotted_path_from_token(&path_tok, t)?;
            if remove {
                remove_paths.push(path);
            } else {
                let eq = t.next_token()?.ok_or("Expected '='")?;
                if eq != "=" {
                    return Err(format!("Expected '=' but got '{eq}'"));
                }
                let value = parse_set_value(t, &path)?;
                set_clauses.push(SetClause { path, value });
            }
            // SET takes a list of assignments; REMOVE takes one path, and a
            // comma after it is a token left over (eu-west-2, 18 October 2026).
            match t.peek_token()? {
                Some(ref s) if s == "," && !remove => {
                    t.next_token()?;
                }
                _ => break,
            }
        }
    }
    Ok((set_clauses, remove_paths))
}

/// `FROM "T" [WHERE ...] SET ...|REMOVE ... [RETURNING ...]`, which DynamoDB
/// runs as an UPDATE. The WHERE clause comes before the SET and REMOVE
/// clauses; one after them is a token left over. Without a SET or REMOVE the
/// statement has no verb, and a WHERE with nothing before its SET reads as
/// one with no verb too (captured eu-west-2 and us-east-1, 2026-10-09).
fn parse_from_first(t: &mut Tokenizer) -> Result<Statement, ParseError> {
    const NO_VERB: &str = "Expected data manipulation";
    let target = parse_table_name(t)?;
    reject_alias(t)?;
    let is_dml_keyword = |tok: Option<String>| {
        tok.is_some_and(|tok| is_keyword(&tok, "SET") || is_keyword(&tok, "REMOVE"))
    };
    if t.peek_token()?.is_some_and(|tok| is_keyword(&tok, "WHERE"))
        && is_dml_keyword(t.peek_token_at(1)?)
    {
        return Err(NO_VERB.into());
    }
    let where_clause = parse_optional_where(t)?;
    if !is_dml_keyword(t.peek_token()?) {
        return Err(NO_VERB.into());
    }
    let (set_clauses, remove_paths) = parse_update_clauses(t)?;
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

/// Parse the right-hand side of a SET assignment to `target`: a term, terms
/// added and subtracted, or `set_add` and `set_delete`.
fn parse_set_value(t: &mut Tokenizer, target: &str) -> Result<SetValue, String> {
    t.clause = LiteralClause::Set;
    if let Some(tok) = t.peek_token()?
        && let Some(delete) = set_function(&tok)
        && t.peek_token_at(1)?.as_deref() == Some("(")
    {
        return parse_set_call(t, target, delete);
    }
    Ok(match read_set_operands(t, 0)? {
        SetTerm::Sum(terms) => SetValue::Sum(terms),
        term => SetValue::Simple(term),
    })
}

/// A SET right-hand side, or an operand of `list_append` or `if_not_exists`,
/// read the same way at every depth: one term, or terms added and subtracted.
/// A call in it is classified by [`read_set_operand`] wherever it stands, so
/// `if_not_exists(q, size(s))` refuses `size` as `SET z = size(s)` does, and
/// `if_not_exists(q, n + 1)` computes (captured eu-west-2, 12 November 2026).
fn read_set_operands(t: &mut Tokenizer, depth: usize) -> Result<SetTerm, String> {
    let mut terms = Vec::new();
    read_set_sum(t, SetSign::Plus, SetSign::Plus, &mut terms, depth)?;
    Ok(match terms.as_slice() {
        [
            SumTerm {
                sign: SetSign::Plus,
                term,
                ..
            },
        ] => term.clone(),
        // Only a refused operand holds no term, and it is never evaluated.
        [] => refused_set_term(),
        _ => SetTerm::Sum(terms),
    })
}

/// What stands for a SET term that is refused. It is never evaluated.
fn refused_set_term() -> SetTerm {
    SetTerm::Value(PartiqlValue::Literal(AttributeValue::NULL(true)))
}

/// How DynamoDB refuses an operator SET doesn't compute.
const SET_OPERATOR_REFUSAL: &str = "Unsupported operator in Update Expression. Operator: ";

/// The terms of a SET right-hand side, or of a bracket in one, added and
/// subtracted in any mix and chained: `n + 1`, `1 + n`, `n + n`, `3 - 1`,
/// `1 + 1 + 1` and `(1 + 1)` all compute (captured eu-west-2, 12 November
/// 2026). `sign` is the sign the first term takes. A bracket of several terms
/// is pushed as one term, a group read with its own signs; a bracket of one
/// term is that term, with the sign written before the bracket. `written` is
/// the operator written before the bracket, which its first term is named by
/// if the bracket holds nothing else.
///
/// A type fault names the operator written beside the faulty operand, not its
/// sign once the brackets are opened: the one before it, or the one after it
/// for the first term of a run (captured eu-west-2, 12 November 2026:
/// `5 - (2 - 'a')` names `-` and `1 - ('a' + 1)` names `+`). `*` and `/` are
/// refused naming them. Each `+`, `-`, `*` and `/` counts towards the
/// expression's operator limit, as a WHERE clause's operators do.
fn read_set_sum(
    t: &mut Tokenizer,
    sign: SetSign,
    written: SetSign,
    terms: &mut Vec<SumTerm>,
    depth: usize,
) -> Result<(), String> {
    if depth > MAX_EXPR_DEPTH {
        return Err(TOO_DEEP.to_string());
    }
    let first = terms.len();
    read_set_operand(t, sign, written, terms, depth)?;
    // The run's first operand, when it is a single term, is named by the
    // operator after it.
    let mut unnamed = (terms.len() == first + 1).then_some(first);
    while let Some(op) = t.peek_token()? {
        let (next, operator) = match op.as_str() {
            "+" => (sign, SetSign::Plus),
            "-" => (flipped(sign), SetSign::Minus),
            "*" | "/" => {
                t.note_set_refusal(format!("{SET_OPERATOR_REFUSAL}{op}"));
                (sign, SetSign::Plus)
            }
            _ => break,
        };
        t.count_set_operator();
        if let Some(index) = unnamed.take() {
            terms[index].named = operator;
        }
        t.next_token()?;
        read_set_operand(t, next, operator, terms, depth)?;
    }
    Ok(())
}

/// The other sign.
fn flipped(sign: SetSign) -> SetSign {
    match sign {
        SetSign::Plus => SetSign::Minus,
        SetSign::Minus => SetSign::Plus,
    }
}

/// One operand of a SET right-hand side, pushed with `sign` and named by
/// `written`, the operator written before it: a term, or a bracket of terms.
///
/// A call to a condition function, or to `size`, is refused naming the
/// function whatever its operands, ahead of anything wrong with them, and read
/// past (captured eu-west-2, 12 November 2026: `size(true)`, `size(nope)` and
/// `size(s) + 1` all report it). A call to a function DynamoDB doesn't know,
/// `foo(1)` or `abs(n)`, is unrecognised, naming the function and where it
/// stands (captured eu-west-2, 12 November 2026).
///
/// A `-` before anything but a number is an operator with one operand, which
/// DynamoDB refuses (`- - n` and `-(n)`); before a number it is the number's
/// sign (`- 5`, `n - -1`). A run of them is read in a loop, so its length
/// costs no stack.
fn read_set_operand(
    t: &mut Tokenizer,
    sign: SetSign,
    written: SetSign,
    terms: &mut Vec<SumTerm>,
    depth: usize,
) -> Result<(), String> {
    let mut tok = t.peek_token()?.ok_or(UNEXPECTED_TERM)?;
    while tok == "-"
        && !t
            .peek_token_at(1)?
            .is_some_and(|next| is_number_token(&next))
    {
        t.note_set_refusal(operand_count_refusal("-", 1));
        t.next_token()?;
        tok = t.peek_token()?.ok_or(UNEXPECTED_TERM)?;
    }
    if tok == "(" {
        t.next_token()?;
        // A bracket of several terms is one term of the run it stands in, its
        // sign applied to the group's total, so the group is worked out on its
        // own first. Each term in it is named as it would be in the run, and a
        // bracket holding one term is that term.
        let mut group = Vec::new();
        read_set_sum(t, SetSign::Plus, written, &mut group, depth + 1)?;
        expect_char(t, ")")?;
        match <[SumTerm; 1]>::try_from(group) {
            Ok([only]) => terms.push(SumTerm {
                sign: if only.sign == SetSign::Plus {
                    sign
                } else {
                    flipped(sign)
                },
                ..only
            }),
            Err(group) if group.is_empty() => {}
            Err(group) => terms.push(SumTerm {
                sign,
                named: written,
                term: SetTerm::Sum(group),
            }),
        }
        return Ok(());
    }
    let calls = t.peek_token_at(1)?.as_deref() == Some("(");
    if calls && is_any(&tok, WHERE_FUNCTIONS) {
        t.note_set_refusal(format!(
            "The function is not allowed in an update expression; function: {}",
            tok.to_lowercase()
        ));
        return skip_call_operands(t);
    }
    if calls && set_term_function(&tok).is_none() && is_name(&tok) && !tok.starts_with('"') {
        t.note_unrecognized_function(t.pos);
        return skip_call_operands(t);
    }
    terms.push(SumTerm {
        sign,
        named: written,
        term: parse_set_term(t, depth)?,
    });
    Ok(())
}

/// The functions a SET term can call: `list_append` and `if_not_exists`.
fn set_term_function(tok: &str) -> Option<&'static str> {
    ["list_append", "if_not_exists"]
        .into_iter()
        .find(|name| is_keyword(tok, name))
}

/// A term of a SET right-hand side, or an operand of `list_append` or
/// `if_not_exists`. A call to either function is a term, so it can be
/// bracketed and added to (captured eu-west-2, 12 November 2026:
/// `if_not_exists(n, 0) + 1` and `(list_append(l, [3]))` run). A name, bare
/// or quoted, starts an attribute's path, dotted or indexed, so `SET z = s`
/// copies `s` and `SET z = l[0]` its first element (captured eu-west-2, 12
/// November 2026); anything else is a value, so `'abc'` is a string and not
/// the attribute `abc`.
fn parse_set_term(t: &mut Tokenizer, depth: usize) -> Result<SetTerm, String> {
    if depth > MAX_EXPR_DEPTH {
        return Err(TOO_DEEP.to_string());
    }
    if let Some(tok) = t.peek_token()?
        && let Some(name) = set_term_function(&tok)
        && t.peek_token_at(1)?.as_deref() == Some("(")
    {
        return parse_set_term_call(t, name, depth);
    }
    let names_attribute = t.peek_token()?.is_some_and(|tok| {
        tok.starts_with('"')
            || (tok.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                && !is_any(&tok, &["TRUE", "FALSE", "NULL"])
                && !is_any(&tok, CLAUSE_KEYWORDS)
                && !is_any(&tok, OPERATOR_WORDS))
    });
    if names_attribute {
        let tok = t.next_token()?.unwrap_or_default();
        return Ok(SetTerm::Path(parse_dotted_path_from_token(&tok, t)?));
    }
    Ok(SetTerm::Value(parse_value(t, 0)?))
}

/// `list_append(a, b)` or `if_not_exists(path, value)`, with its name next.
/// Anything but two operands is refused for its count, and an
/// `if_not_exists` whose first operand isn't a path as needing one (captured
/// eu-west-2, 12 November 2026).
fn parse_set_term_call(t: &mut Tokenizer, name: &str, depth: usize) -> Result<SetTerm, String> {
    t.next_token()?; // the name
    t.next_token()?; // (
    let mut operands = Vec::new();
    if t.peek_token()?.as_deref() == Some(")") {
        t.next_token()?;
    } else {
        loop {
            operands.push(read_set_operands(t, depth + 1)?);
            match t.next_token()?.as_deref() {
                Some(",") => continue,
                Some(")") => break,
                Some(other) => return Err(format!("Expected ')' but got '{other}'")),
                None => return Err("Expected ')'".to_string()),
            }
        }
    }
    let [first, second] = match <[SetTerm; 2]>::try_from(operands) {
        Ok(pair) => pair,
        Err(operands) => {
            t.note_set_refusal(operand_count_refusal(name, operands.len()));
            return Ok(refused_set_term());
        }
    };
    Ok(match (name, first) {
        ("if_not_exists", SetTerm::Path(path)) => SetTerm::IfNotExists(path, Box::new(second)),
        ("if_not_exists", _) => {
            t.note_set_refusal(format!(
                "Operator or function requires a document path; operator or function: {name}"
            ));
            refused_set_term()
        }
        (_, first) => SetTerm::ListAppend(Box::new(first), Box::new(second)),
    })
}

/// Whether a name calls `set_add` (false) or `set_delete` (true).
fn set_function(tok: &str) -> Option<bool> {
    if is_keyword(tok, "set_add") {
        Some(false)
    } else if is_keyword(tok, "set_delete") {
        Some(true)
    } else {
        None
    }
}

/// Read `set_add(...)` or `set_delete(...)` as the value assigned to `target`.
///
/// DynamoDB judges the call before it looks the table up, without the syntax
/// envelope, and names where it stands: first that it has two arguments, then
/// that the first is the path it assigns to, both at the function's name, then
/// that the second is a set, at the first argument. Part of a larger
/// expression, the call is one it doesn't recognise (captured eu-west-2,
/// us-east-1, eu-west-1 and us-west-2, 2026-10-09).
fn parse_set_call(t: &mut Tokenizer, target: &str, delete: bool) -> Result<SetValue, String> {
    let name = if delete { "SET_DELETE" } else { "SET_ADD" };
    let call = t.pos;
    t.next_token()?; // the name
    t.next_token()?; // (
    // Each argument: the token it starts at, and the path it names or the
    // value it is.
    let mut args: Vec<(usize, Result<String, PartiqlValue>)> = Vec::new();
    if t.peek_token()?.as_deref() == Some(")") {
        t.next_token()?;
    } else {
        loop {
            let start = t.pos;
            let tok = t.next_token()?.ok_or(UNEXPECTED_TERM)?;
            let arg = if is_name(&tok) {
                Ok(parse_dotted_path_from_token(&tok, t)?)
            } else {
                t.pos -= 1;
                Err(parse_value(t, 0)?)
            };
            args.push((start, arg));
            match t.next_token()?.as_deref() {
                Some(",") => continue,
                Some(")") => break,
                Some(other) => return Err(format!("Expected ')' but got '{other}'")),
                None => return Err("Expected ')'".to_string()),
            }
        }
    }
    if t.peek_token()?.is_some_and(|tok| tok == "+" || tok == "-") {
        t.note_unrecognized_function(call);
        t.next_token()?;
        parse_value(t, 0)?;
        return Ok(SetValue::Simple(refused_set_term()));
    }
    let call_at = t.position(t.span_at(call));
    let first_at = args
        .first()
        .map(|(start, _)| t.position(t.span_at(*start)))
        .unwrap_or_default();
    let mut members = PartiqlValue::Literal(AttributeValue::NULL(true));
    match args.as_slice() {
        [(_, first), (_, second)] => {
            if first.as_deref() != Ok(target) {
                t.note_set_refusal(format!(
                    "The first argument to {name} must equal the assignment value at {call_at}"
                ));
            } else if !matches!(
                second,
                Err(PartiqlValue::Parameter(_))
                    | Err(PartiqlValue::Literal(
                        AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_)
                    ))
            ) {
                t.note_set_refusal(format!(
                    "The second argument to {name} must be a value with type SET at {first_at}"
                ));
            }
            if let Err(value) = second {
                members = value.clone();
            }
        }
        _ => t.note_set_refusal(format!(
            "{name} must have exactly two arguments at {call_at}"
        )),
    }
    Ok(SetValue::SetCall {
        delete,
        path: target.to_string(),
        members,
        refusal_at: first_at,
    })
}

fn parse_delete(t: &mut Tokenizer) -> Result<Statement, ParseError> {
    expect_keyword(t, "FROM")?;
    // Nothing, a semicolon or a keyword where the table should be (captured
    // eu-west-2 and us-east-1, 2026-10-09).
    if !t.peek_token()?.as_deref().is_some_and(is_name) {
        return Err(EXPECTED_SIMPLE_PATH.into());
    }
    let target = parse_table_name(t)?;
    reject_alias(t)?;
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
            // A semicolon may end the statement here as anywhere else; what
            // follows it is for `end_of_statement` to refuse (captured
            // eu-west-2 and us-east-1, 2026-10-10).
            if star != "*" || t.peek_token()?.is_some_and(|tok| tok != ";") {
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

/// Refuse an alias on the table a SELECT, UPDATE or DELETE names.
///
/// DynamoDB reads `FROM "T" t` and `FROM "T" AS t` as aliasing and refuses it
/// with its own message, outside the syntax-error envelope (captured
/// eu-west-2, 2026-10-09).
fn reject_alias(t: &Tokenizer) -> Result<(), ParseError> {
    if t.peek_token()?.is_some_and(|tok| is_alias(&tok)) {
        return Err(ParseError::Validation(
            "Aliasing is not supported".to_string(),
        ));
    }
    Ok(())
}

/// Whether a token straight after a table name is an alias: `AS`, or a bare
/// word that is not one of the keywords a statement can carry on with there.
/// Those keep the handling they had, since what DynamoDB says about each has
/// not been captured.
fn is_alias(tok: &str) -> bool {
    const NOT_AN_ALIAS: &[&str] = &[
        "WHERE",
        "SET",
        "REMOVE",
        "ORDER",
        "RETURNING",
        "VALUE",
        "VALUES",
        "LIMIT",
        "OFFSET",
        "GROUP",
        "HAVING",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "OUTER",
        "CROSS",
        "ON",
        "AT",
        "LET",
        "AND",
        "OR",
        "NOT",
        "IF",
    ];
    let bare_word = tok
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    bare_word
        && (tok.eq_ignore_ascii_case("AS")
            || !NOT_AN_ALIAS.iter().any(|kw| tok.eq_ignore_ascii_case(kw)))
}

fn parse_optional_where(t: &mut Tokenizer) -> Result<Option<WhereClause>, String> {
    match t.peek_token()? {
        Some(ref s) if s.eq_ignore_ascii_case("WHERE") => {
            t.next_token()?; // consume WHERE
            t.clause = LiteralClause::Where;
            let parsed = parse_conditions_with_or(t)?;
            Ok(Some(WhereClause {
                groups: parsed.groups,
                fully_expanded: parsed.fully_expanded,
                top_level_equalities: parsed.top_level_equalities,
                tree: parsed.tree,
                written: parsed.written,
            }))
        }
        _ => Ok(None),
    }
}

/// Parse conditions supporting both AND and OR.
///
/// Returns a list of OR-groups, where each group is a list of AND-joined
/// conditions.
fn parse_conditions_with_or(t: &mut Tokenizer) -> Result<ParsedConditions, String> {
    t.reading_where = true;
    let expr = parse_or_expr(t, 0);
    t.reading_where = false;
    let expr = expr?;
    let mut top_level_equalities = Vec::new();
    collect_top_level_equalities(&expr, &mut top_level_equalities);
    // A statement already holding a refusal is refused with it, so its clause
    // isn't flattened at all: the flattening's own budget would otherwise
    // answer first, in words DynamoDB never uses, for a statement DynamoDB
    // refuses for another reason (too many operators, or a term or literal it
    // refuses as it reads).
    let refused = || ParsedConditions {
        groups: Vec::new(),
        fully_expanded: true,
        top_level_equalities: Vec::new(),
        tree: ConditionTree::And(Vec::new()),
        written: WrittenClause::And(Vec::new()),
    };
    if t.holds_refusal() {
        return Ok(refused());
    }
    let written = WrittenClause::of(&expr);
    // A clause holding a call or an operator DynamoDB refuses for itself is
    // refused once its operands have been checked in the order written, so
    // it is kept whole, as written, for that check, and not flattened: the
    // flattening's budget would otherwise answer first for a long one.
    if t.operand_fault || holds_non_boolean(&expr) {
        return Ok(ParsedConditions {
            written,
            ..refused()
        });
    }
    let flattened = push_not_down(expr, false).and_then(|expr| {
        let tree = ConditionTree::of(expr);
        if tree.bytes() > MAX_CLAUSE_BYTES {
            return Err(TOO_COMPLEX.to_string());
        }
        let expanded = to_dnf(&tree);
        let fully_expanded = expanded.is_ok();
        let groups = expanded.unwrap_or_else(|_| vec![tree.required_conditions()]);
        Ok((tree, groups, fully_expanded))
    });
    let (tree, groups, fully_expanded) = flattened?;
    Ok(ParsedConditions {
        groups,
        fully_expanded,
        top_level_equalities,
        tree,
        written,
    })
}

/// A WHERE clause as parsed: its OR-groups, the equalities among its
/// top-level conditions, and its tree.
struct ParsedConditions {
    groups: Vec<Vec<Arc<WhereCondition>>>,
    fully_expanded: bool,
    top_level_equalities: Vec<Condition>,
    tree: ConditionTree,
    written: WrittenClause,
}

/// Collect the `path = value` conditions joined to the rest of the clause by
/// `AND` alone. Parentheses leave no node of their own, so a parenthesised
/// conjunction counts; an `OR` or a `NOT` hides everything under it.
fn collect_top_level_equalities(expr: &BoolExpr, out: &mut Vec<Condition>) {
    match expr {
        BoolExpr::And(terms) => terms
            .iter()
            .for_each(|term| collect_top_level_equalities(term, out)),
        BoolExpr::Leaf(WhereCondition::Comparison(cond)) if cond.op == CompOp::Eq => {
            out.push(cond.clone())
        }
        _ => {}
    }
}

/// Whether a value stands anywhere in an expression where a condition
/// should.
fn holds_non_boolean(expr: &BoolExpr) -> bool {
    match expr {
        BoolExpr::NonBoolean(..) => true,
        BoolExpr::And(terms) | BoolExpr::Or(terms) => terms.iter().any(holds_non_boolean),
        BoolExpr::Not(inner) => holds_non_boolean(inner),
        BoolExpr::Leaf(_) => false,
    }
}

/// A WHERE clause as written, before it is flattened.
///
/// `WhereClause` holds an OR of ANDs, which is the shape the executor and the
/// key-condition pushdown both read. Parentheses and `NOT` do not fit that
/// shape as written, so they are parsed into a tree here and flattened into it
/// afterwards. Nothing below the parser sees this type.
enum BoolExpr {
    Leaf(WhereCondition),
    And(Vec<BoolExpr>),
    Or(Vec<BoolExpr>),
    Not(Box<BoolExpr>),
    /// A value standing where a condition should, by its DynamoDB type: refused
    /// once its own operands are checked, naming the operator it stands under.
    NonBoolean(&'static str, Operand),
}

/// `depth` counts the nesting levels above this expression: one per set of
/// parentheses, the one edge that comes back round to a parser already on the
/// stack. A run of NOTs and the alternation and conjunction chains are loops
/// rather than recursion, so a long `a OR b OR c ...` does not spend it.
fn parse_or_expr(t: &mut Tokenizer, depth: usize) -> Result<BoolExpr, String> {
    if depth > MAX_EXPR_DEPTH {
        return Err(TOO_DEEP.to_string());
    }
    let mut terms = vec![parse_and_expr(t, depth)?];
    while matches!(t.peek_token()?, Some(ref s) if s.eq_ignore_ascii_case("OR")) {
        t.next_token()?;
        t.count_operator();
        require_right_hand_side(t)?;
        terms.push(parse_and_expr(t, depth)?);
    }
    Ok(if terms.len() == 1 {
        terms.pop().unwrap()
    } else {
        BoolExpr::Or(terms)
    })
}

fn parse_and_expr(t: &mut Tokenizer, depth: usize) -> Result<BoolExpr, String> {
    let mut terms = vec![parse_not_expr(t, depth)?];
    while matches!(t.peek_token()?, Some(ref s) if s.eq_ignore_ascii_case("AND")) {
        t.next_token()?;
        t.count_operator();
        require_right_hand_side(t)?;
        terms.push(parse_not_expr(t, depth)?);
    }
    Ok(if terms.len() == 1 {
        terms.pop().unwrap()
    } else {
        BoolExpr::And(terms)
    })
}

/// Refuse an `AND` or `OR` with nothing after it, in DynamoDB's words
/// (captured eu-west-2, us-east-1, eu-west-1 and us-west-2, 2026-10-09).
fn require_right_hand_side(t: &mut Tokenizer) -> Result<(), String> {
    match t.peek_token()? {
        None => Err(MISSING_RIGHT_HAND_SIDE.to_string()),
        Some(ref s) if s == ";" => Err(MISSING_RIGHT_HAND_SIDE.to_string()),
        Some(_) => Ok(()),
    }
}

/// Only reached from `parse_and_expr`, under a `parse_or_expr` that has
/// already checked `depth`.
fn parse_not_expr(t: &mut Tokenizer, depth: usize) -> Result<BoolExpr, String> {
    // A `NOT` in front of a condition is the boolean operator, a call to
    // `begins_with` or `contains` included.
    let mut nots = 0usize;
    while matches!(t.peek_token()?, Some(ref s) if s.eq_ignore_ascii_case("NOT")) {
        t.next_token()?;
        nots += 1;
    }
    let mut expr = parse_primary(t, depth)?;
    // Only the run's parity changes the meaning, and every NOT has already
    // been counted against the operator limit, so at most two are built.
    // Building one per NOT let runs separated by brackets stack into a chain
    // far deeper than the bracket limit.
    let wraps = match nots {
        0 => 0,
        n if n % 2 == 1 => 1,
        _ => 2,
    };
    for _ in 0..wraps {
        expr = BoolExpr::Not(Box::new(expr));
    }
    Ok(expr)
}

fn parse_primary(t: &mut Tokenizer, depth: usize) -> Result<BoolExpr, String> {
    if matches!(t.peek_token()?, Some(ref s) if s == "(") {
        t.next_token()?;
        let inner = parse_or_expr(t, depth + 1)?;
        match t.next_token()? {
            // Bracketed arithmetic is an operand, and is refused as it is
            // without the bracket (captured eu-west-2, 2026-11-11).
            Some(ref s) if s == ")" && is_arithmetic(&inner) => {
                if starts_operator_tail(t)? {
                    let number =
                        Operand::Value(PartiqlValue::Literal(AttributeValue::N("0".to_string())));
                    finish_operator(t, depth, number)?;
                }
                Ok(inner)
            }
            Some(ref s) if s == ")" => Ok(inner),
            other => Err(format!(
                "Expected ')' but got '{}'",
                other.unwrap_or_else(|| "end of statement".to_string())
            )),
        }
    } else {
        parse_condition(t, depth)
    }
}

/// The functions DynamoDB recognises in a WHERE clause. A call to anything
/// else is refused as an unrecognised function.
const WHERE_FUNCTIONS: &[&str] = &[
    "attribute_exists",
    "attribute_not_exists",
    "begins_with",
    "contains",
    "size",
    "attribute_type",
];

/// The types `attribute_type` tests for, in the order DynamoDB lists them.
const ATTRIBUTE_TYPES: &[&str] = &["N", "BS", "L", "B", "NULL", "M", "S", "SS", "NS", "BOOL"];

/// What DynamoDB says when `attribute_type` is given a path as the type: the
/// operand types it would take, in the order of a Java hash set. The order
/// differs from host to host (us-east-1 gave another); this is eu-west-2's
/// (captured 2026-10-09).
pub(crate) const ATTRIBUTE_TYPE_OPERAND_TYPES: &str = "{S,HDS,N,FS,BS,NULL,DOUBLE,NS,DECIMAL,SS,DECIMALSET,\
     BOOL,HD,DICT,INTSET,M,L,B,FLOAT,INT,DOUBLESET}";

/// `MISSING` anywhere but after IS. It outranks a refused operand, whichever
/// is written first, and is reported in the order written against a refused
/// term or literal, so it is noted as a term (captured eu-west-2 and
/// us-east-1, 2026-10-10).
const MISSING_COMPARED: &str = "MISSING must be compared using IS operator";

/// A comparison operator, as a WHERE clause writes one.
fn is_comparison_operator(tok: &str) -> bool {
    matches!(tok, "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=")
}

/// The operator a comparison token writes.
fn comparison_operator(tok: &str) -> Result<CompOp, String> {
    Ok(match tok {
        "=" => CompOp::Eq,
        "<>" | "!=" => CompOp::Ne,
        "<" => CompOp::Lt,
        "<=" => CompOp::Le,
        ">" => CompOp::Gt,
        ">=" => CompOp::Ge,
        other => return Err(format!("Unknown operator: {other}")),
    })
}

/// The operator that reads the same with its operands swapped: `3 < n` is
/// `n > 3`.
fn mirrored(op: CompOp) -> CompOp {
    match op {
        CompOp::Lt => CompOp::Gt,
        CompOp::Le => CompOp::Ge,
        CompOp::Gt => CompOp::Lt,
        CompOp::Ge => CompOp::Le,
        other => other,
    }
}

/// A function that is a condition: `attribute_exists`, `attribute_not_exists`,
/// `attribute_type`, `begins_with` or `contains`, as the condition it is when
/// it holds.
struct ConditionFunction {
    holds: WhereCondition,
}

impl ConditionFunction {
    /// The condition the call is true for, or the one it is false for.
    fn condition(&self, holds: bool) -> Result<BoolExpr, String> {
        if holds {
            Ok(BoolExpr::Leaf(self.holds.clone()))
        } else {
            negate_condition(self.holds.clone())
        }
    }

    /// The call as an operand: the boolean it returns for an item.
    fn operand(&self) -> Operand {
        Operand::Condition(Box::new(self.holds.clone()))
    }

    /// The call compared with an operand. It is a boolean: equal only to the
    /// same boolean, so unequal to a missing value, a size or a value of
    /// another type, and the ordering operators refuse it, once the operands
    /// are checked (captured eu-west-2 and us-east-1, 2026-10-09 for a
    /// literal, 2026-10-10 for a path, a size or a parameter). A boolean
    /// literal under `=` or `<>` settles it as the call or its negation.
    /// Anything else is one condition, read item by item rather than spelled
    /// out as alternatives, so ten `<>` comparisons ANDed stay one group and a
    /// key condition beside one keeps the read a key read, as DynamoDB runs
    /// both (captured eu-west-2 and us-east-1, 2026-10-10).
    ///
    /// `written_first` says which side the call was written on, so the
    /// operands are checked in the order written and an ordering operator is
    /// named as written.
    fn compared(&self, op: &str, value: Operand, written_first: bool) -> Result<BoolExpr, String> {
        let op = comparison_operator(op)?;
        if matches!(op, CompOp::Eq | CompOp::Ne)
            && let Operand::Value(PartiqlValue::Literal(AttributeValue::BOOL(b))) = value
        {
            return self.condition(b == (op == CompOp::Eq));
        }
        Ok(BoolExpr::Leaf(if written_first {
            WhereCondition::Compare(self.operand(), op, value)
        } else {
            WhereCondition::Compare(value, op, self.operand())
        }))
    }

    /// The call in a list: true when any member equals it (captured eu-west-2
    /// and us-east-1, 2026-10-10), as one condition.
    fn listed(&self, members: Vec<Operand>) -> BoolExpr {
        BoolExpr::Leaf(WhereCondition::CompareIn(self.operand(), members))
    }
}

/// Which condition function a name calls, by its canonical name.
fn condition_function_name(tok: &str) -> Option<&'static str> {
    ["attribute_exists", "attribute_not_exists"]
        .into_iter()
        .find(|name| is_keyword(tok, name))
}

/// Whether a name calls a function that is itself a condition.
fn is_condition_function(tok: &str) -> bool {
    condition_function_name(tok).is_some()
        || search_function(tok).is_some()
        || is_keyword(tok, "attribute_type")
}

/// Which search a name calls, `begins_with` or `contains`.
fn search_function(tok: &str) -> Option<Search> {
    [Search::BeginsWith, Search::Contains]
        .into_iter()
        .find(|search| is_keyword(tok, search.name()))
}

/// Whether a token starts a term that is a value rather than a path: a
/// literal, a parameter or `MISSING`.
fn starts_value(t: &Tokenizer, tok: &str) -> Result<bool, String> {
    Ok(tok == "?"
        || tok.starts_with(['\'', '`', '[', '{', '<'])
        || is_number_token(tok)
        || ((tok == "-" || tok == "+") && t.peek_token_at(1)?.is_some_and(|n| is_number_token(&n)))
        || is_any(tok, &["TRUE", "FALSE", "NULL", "MISSING"]))
}

/// One WHERE condition: a condition function, a subquery or a call DynamoDB
/// doesn't recognise, a comparison that starts with a value or `size(...)`,
/// or a comparison and the other single conditions on a path.
fn parse_condition(t: &mut Tokenizer, depth: usize) -> Result<BoolExpr, String> {
    let Some(tok) = t.peek_token()? else {
        return single_condition(t, depth);
    };
    let calls = |offset: usize, t: &Tokenizer| -> Result<bool, String> {
        Ok(t.peek_token_at(offset + 1)?.as_deref() == Some("("))
    };

    // A boolean compared with a condition function, `true = attribute_exists(x)`.
    if is_any(&tok, &["TRUE", "FALSE"])
        && t.peek_token_at(1)?
            .is_some_and(|op| is_comparison_operator(&op))
        && t.peek_token_at(2)?
            .is_some_and(|f| is_condition_function(&f))
        && calls(2, t)?
    {
        let left = parse_value(t, depth)?;
        let op = t.next_token()?.unwrap_or_default();
        return parse_condition_function(t, depth)?.compared(&op, Operand::Value(left), false);
    }

    // A path compared with a condition function, `b = attribute_exists(a)`:
    // the same comparison as with the call written first (captured eu-west-2
    // and us-east-1, 2026-10-10).
    if is_name(&tok) && !calls(0, t)? {
        let end = path_end(t)?;
        if t.peek_token_at(end)?
            .is_some_and(|op| is_comparison_operator(&op))
            && t.peek_token_at(end + 1)?
                .is_some_and(|f| is_condition_function(&f))
            && calls(end + 1, t)?
        {
            let first = t.next_token()?.unwrap_or_default();
            let path = parse_dotted_path_from_token(&first, t)?;
            let op = t.next_token()?.unwrap_or_default();
            return parse_condition_function(t, depth)?.compared(&op, Operand::Path(path), false);
        }
    }

    let bare_word = tok.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_');
    if bare_word && calls(0, t)? {
        // `EXISTS (SELECT ...)`: DynamoDB takes no subquery in a WHERE clause,
        // and refuses the SELECT where it stands (captured eu-west-2 and
        // us-east-1, 2026-10-09).
        if is_keyword(&tok, "EXISTS")
            && let Some(select) = subquery_select(t)
        {
            let at = t.position(t.span_at(select));
            t.note_term(format!("Unsupported token in expression: SELECT at {at}"));
            skip_call(t, depth)?;
            return Ok(refused_condition());
        }
        if is_condition_function(&tok) {
            let function = parse_condition_function(t, depth)?;
            return condition_function_tail(t, depth, function);
        }
        if is_keyword(&tok, "size") {
            let left = parse_operand(t, depth)?;
            return finish_comparison(t, depth, left);
        }
        if !is_any(&tok, WHERE_FUNCTIONS) {
            t.note_unrecognized_function(t.pos);
            skip_call(t, depth)?;
            return Ok(refused_condition());
        }
    }
    // A literal or a parameter on the left of a comparison.
    if starts_value(t, &tok)? {
        let left = parse_operand(t, depth)?;
        return finish_comparison(t, depth, left);
    }
    single_condition(t, depth)
}

/// A condition on a path, or one of the other single conditions. The only
/// wrapped negations it reads are `x NOT IN [...]` and `x NOT BETWEEN lo AND
/// hi`, which become the trees `NOT x IN [...]` and `NOT x BETWEEN lo AND hi`
/// build, so the two forms of each evaluate alike.
fn single_condition(t: &mut Tokenizer, depth: usize) -> Result<BoolExpr, String> {
    Ok(match parse_single_condition(t, depth)? {
        WhereCondition::Not(inner) => BoolExpr::Not(Box::new(BoolExpr::Leaf(*inner))),
        cond => BoolExpr::Leaf(cond),
    })
}

/// A condition standing for one the statement is refused for, so it is never
/// evaluated. It stands only where a term refusal has just been noted, and a
/// clause holding one is dropped before anything reads it, so this is a
/// placeholder rather than a refused leaf: there is nothing of it to check
/// and no message for it to carry.
fn refused_condition() -> BoolExpr {
    BoolExpr::Leaf(WhereCondition::Exists(String::new()))
}

/// Whatever follows a condition function: a comparison, BETWEEN or IN, or
/// nothing.
fn condition_function_tail(
    t: &mut Tokenizer,
    depth: usize,
    function: ConditionFunction,
) -> Result<BoolExpr, String> {
    let Some(next) = t.peek_token()? else {
        return function.condition(true);
    };
    if is_comparison_operator(&next) {
        t.next_token()?;
        let right = parse_term(t, depth)?;
        return function.compared(&next, right, true);
    }
    // IS refuses any condition function as its left side, as it does a size
    // (captured eu-west-2, 2026-11-12).
    if is_keyword(&next, "IS") {
        return Ok(BoolExpr::Leaf(WhereCondition::Refused(refuse_is(t)?)));
    }
    // `NOT BETWEEN` takes BETWEEN's refusal and `NOT IN` is the negation of
    // IN (captured eu-west-2 and us-east-1, 2026-10-10).
    let negated = is_keyword(&next, "NOT")
        && t.peek_token_at(1)?
            .is_some_and(|op| is_any(&op, &["BETWEEN", "IN"]));
    let next = if negated {
        t.next_token()?;
        t.peek_token()?.unwrap_or_default()
    } else {
        next
    };
    // BETWEEN refuses the call as the boolean it is, once its bounds are
    // checked (captured eu-west-2, 2026-11-11).
    if is_keyword(&next, "BETWEEN") {
        t.next_token()?;
        let low = parse_term(t, depth)?;
        expect_between_and(t)?;
        let high = parse_term(t, depth)?;
        let between = BoolExpr::Leaf(WhereCondition::CompareBetween(
            function.operand(),
            low,
            high,
        ));
        return Ok(if negated {
            BoolExpr::Not(Box::new(between))
        } else {
            between
        });
    }
    // Members read as any IN's do, so a bare name is an attribute rather than
    // a string (captured eu-west-2 and us-east-1, 2026-10-10).
    if is_keyword(&next, "IN") {
        t.next_token()?;
        let members = parse_in_list(t, depth, parse_term)?;
        let listed = match in_limit_refusal(members.len()) {
            Some(message) => {
                let operands = std::iter::once(function.operand()).chain(members).collect();
                BoolExpr::Leaf(WhereCondition::Refused(t.refused(operands, message)))
            }
            None => function.listed(members),
        };
        return Ok(if negated {
            BoolExpr::Not(Box::new(listed))
        } else {
            listed
        });
    }
    function.condition(true)
}

/// Read `attribute_exists(...)`, `attribute_not_exists(...)`,
/// `attribute_type(...)`, `begins_with(...)` or `contains(...)`. A call
/// DynamoDB refuses for its operands holds a [`WhereCondition::Refused`] with
/// the operands it was given.
fn parse_condition_function(t: &mut Tokenizer, depth: usize) -> Result<ConditionFunction, String> {
    if t.peek_token()?
        .is_some_and(|tok| is_keyword(&tok, "attribute_type"))
    {
        return Ok(ConditionFunction {
            holds: parse_attribute_type(t, depth)?,
        });
    }
    if let Some(search) = t.peek_token()?.as_deref().and_then(search_function) {
        return Ok(ConditionFunction {
            holds: parse_search(t, depth, search)?,
        });
    }
    let name_tok = t.next_token()?.unwrap_or_default();
    let name = condition_function_name(&name_tok).unwrap_or("attribute_exists");
    expect_char(t, "(")?;
    // Each operand as written: a path, a value or a call.
    let mut operands: Vec<Operand> = Vec::new();
    if t.peek_token()?.as_deref() == Some(")") {
        t.next_token()?;
    } else {
        loop {
            // Peeked, not read and stepped back over, so a value operand's
            // first token is read once and counted once.
            let tok = t.peek_token()?.ok_or(UNEXPECTED_TERM)?;
            if starts_operand_call(t)? {
                // A call is no document path: `attribute_exists(size(s))`
                // (captured eu-west-2, 2026-11-11).
                operands.push(parse_operand(t, depth + 1)?);
            } else if is_name(&tok) {
                t.next_token()?;
                operands.push(Operand::Path(parse_dotted_path_from_token(&tok, t)?));
            } else {
                operands.push(Operand::Value(parse_value(t, depth)?));
            }
            match t.next_token()?.as_deref() {
                Some(",") => continue,
                Some(")") => break,
                Some(other) => return Err(format!("Expected ')' but got '{other}'")),
                None => return Err("Expected ')'".to_string()),
            }
        }
    }
    let refusal = match operands.as_slice() {
        [Operand::Path(_)] => None,
        [_] => Some(format!(
            "Operator or function requires a document path; operator or function: {name}"
        )),
        _ => Some(operand_count_refusal(name, operands.len())),
    };
    let holds = match (refusal, operands.pop()) {
        (Some(message), last) => {
            operands.extend(last);
            WhereCondition::Refused(t.refused(operands, message))
        }
        (None, Some(Operand::Path(path))) if name == "attribute_exists" => {
            WhereCondition::Exists(path)
        }
        (None, Some(Operand::Path(path))) => WhereCondition::NotExists(path),
        (None, _) => unreachable!("one path operand is the only one without a refusal"),
    };
    Ok(ConditionFunction { holds })
}

fn operand_count_refusal(name: &str, count: usize) -> String {
    format!(
        "Incorrect number of operands for operator or function; operator or function: \
         {name}, number of operands: {count}"
    )
}

/// The arguments of a call, each read as an operand.
fn call_operands(t: &mut Tokenizer, depth: usize) -> Result<Vec<Operand>, String> {
    t.next_token()?;
    expect_char(t, "(")?;
    let mut operands = Vec::new();
    if t.peek_token()?.as_deref() == Some(")") {
        t.next_token()?;
        return Ok(operands);
    }
    loop {
        operands.push(parse_operand(t, depth)?);
        match t.next_token()?.as_deref() {
            Some(",") => continue,
            Some(")") => return Ok(operands),
            Some(other) => return Err(format!("Expected ')' but got '{other}'")),
            None => return Err("Expected ')'".to_string()),
        }
    }
}

/// Read `attribute_type(operand, type)`. A wrong number of operands, or a
/// type that is the operand itself, is held as a [`WhereCondition::Refused`].
/// Any other type it can't take is refused as the statement's operands are
/// checked, after the operand it tests, as a type given as a parameter is
/// once the parameter is bound (captured eu-west-2 and us-east-1, 2026-10-09;
/// a call or a size as either, captured eu-west-2, 2026-11-11).
fn parse_attribute_type(t: &mut Tokenizer, depth: usize) -> Result<WhereCondition, String> {
    let mut operands = call_operands(t, depth + 1)?;
    if operands.len() != 2 {
        let message = operand_count_refusal("attribute_type", operands.len());
        return Ok(WhereCondition::Refused(t.refused(operands, message)));
    }
    let kind = operands
        .pop()
        .unwrap_or(Operand::Value(PartiqlValue::Parameter(0)));
    let operand = operands
        .pop()
        .unwrap_or(Operand::Value(PartiqlValue::Parameter(0)));
    if let Operand::Path(path) = &kind
        && operand == kind
    {
        let message = not_distinct("attribute_type", path);
        return Ok(WhereCondition::Refused(
            t.refused(vec![operand, kind], message),
        ));
    }
    Ok(WhereCondition::AttributeType(operand, kind))
}

/// Read `begins_with(a, b)` or `contains(a, b)`. Either operand can be a
/// literal, a parameter, a path or a call (captured eu-west-2, 2026-11-12).
/// A wrong number of operands, or the same path in both places, is held as a
/// [`WhereCondition::Refused`]. A path and a value keep the form the key
/// pushdown reads; anything else is a [`WhereCondition::Search`], whose
/// operand types are checked with the statement's other operands.
fn parse_search(t: &mut Tokenizer, depth: usize, search: Search) -> Result<WhereCondition, String> {
    let [first, second] = match <[Operand; 2]>::try_from(call_operands(t, depth + 1)?) {
        Ok(pair) => pair,
        Err(operands) => {
            let message = operand_count_refusal(search.name(), operands.len());
            return Ok(WhereCondition::Refused(t.refused(operands, message)));
        }
    };
    if let Operand::Path(path) = &first
        && first == second
    {
        let message = not_distinct(search.name(), path);
        return Ok(WhereCondition::Refused(
            t.refused(vec![first, second], message),
        ));
    }
    Ok(match (search, first, second) {
        (Search::BeginsWith, Operand::Path(path), Operand::Value(value)) => {
            WhereCondition::BeginsWith(path, value)
        }
        (Search::Contains, Operand::Path(path), Operand::Value(value)) => {
            WhereCondition::Contains(path, value)
        }
        (search, first, second) => WhereCondition::Search(search, first, second),
    })
}

/// DynamoDB's refusal of a type `attribute_type` doesn't know, if it doesn't.
pub(crate) fn attribute_type_name_refusal(name: &str) -> Option<String> {
    (!ATTRIBUTE_TYPES.contains(&name)).then(|| {
        format!(
            "Invalid attribute type name found; type: {name}, valid types: \
             {{N,BS,L,B,NULL,M,S,SS,NS,BOOL}}"
        )
    })
}

/// Whether the next tokens call a function that reads as an operand:
/// `size(...)`, or a condition function as the boolean it returns.
fn starts_operand_call(t: &Tokenizer) -> Result<bool, String> {
    Ok(t.peek_token_at(1)?.as_deref() == Some("(")
        && t.peek_token()?
            .is_some_and(|tok| is_keyword(&tok, "size") || is_condition_function(&tok)))
}

/// One operand: `size(...)`, a condition function's call as the boolean it
/// returns, a path, `MISSING` (refused), or a literal or a parameter. Nothing
/// where it should be is an infix operator missing its right-hand side.
///
/// A call is read wherever an operand is: the right-hand side of a comparison
/// or a member of an IN list (captured eu-west-2 and us-east-1, 2026-10-10:
/// `attribute_exists(a) = attribute_exists(b)`, `? IN [attribute_exists(a)]`
/// and `b IN [attribute_exists(a)]` run), and a BETWEEN bound or subject or
/// the operand of `size` or `attribute_type` (captured eu-west-2,
/// 2026-11-11). A call DynamoDB refuses for its operands stands as an
/// [`Operand::Refused`], or a call to a condition function holding a
/// [`WhereCondition::Refused`], and is never evaluated.
///
/// A BETWEEN bound is read here too, so a bound can be a path, a size or a
/// call as well as a value, as an IN member can, and `size(s) IN [n]` names
/// the attribute `n` rather than the string `'n'` (captured eu-west-2 and
/// us-east-1, 2026-10-09). `MISSING` is refused here as it is anywhere else
/// an operand can stand.
///
/// Arithmetic after the operand is not read here: [`parse_term`] reads it
/// where an operand can be followed by it, and refuses it for its operator.
fn parse_operand(t: &mut Tokenizer, depth: usize) -> Result<Operand, String> {
    // `size(` and `attribute_type(` come back here for their arguments, so a
    // run of nested calls is held to the same depth as nested brackets.
    if depth > MAX_EXPR_DEPTH {
        return Err(TOO_DEEP.to_string());
    }
    let tok = match t.peek_token()? {
        None => return Err(MISSING_RIGHT_HAND_SIDE.to_string()),
        Some(tok) if tok == ";" => return Err(MISSING_RIGHT_HAND_SIDE.to_string()),
        Some(tok) => tok,
    };
    if is_keyword(&tok, "MISSING") {
        t.next_token()?;
        t.note_term(MISSING_COMPARED.to_string());
        return Ok(Operand::Value(PartiqlValue::Literal(AttributeValue::NULL(
            true,
        ))));
    }
    // A bracketed operand is the operand: `n = (5)` (captured eu-west-2,
    // 2026-11-12).
    if tok == "(" {
        t.next_token()?;
        let inner = parse_term(t, depth + 1)?;
        expect_char(t, ")")?;
        return Ok(inner);
    }
    let call = starts_operand_call(t)?;
    if call && is_condition_function(&tok) {
        return Ok(parse_condition_function(t, depth + 1)?.operand());
    }
    if call && is_keyword(&tok, "size") {
        let mut operands = call_operands(t, depth + 1)?;
        if operands.len() != 1 {
            let message = operand_count_refusal("size", operands.len());
            return Ok(Operand::Refused(t.refused(operands, message)));
        }
        let inner = operands
            .pop()
            .unwrap_or(Operand::Value(PartiqlValue::Parameter(0)));
        return Ok(Operand::Size(Box::new(inner)));
    }
    if is_name(&tok) {
        t.next_token()?;
        return Ok(Operand::Path(parse_dotted_path_from_token(&tok, t)?));
    }
    Ok(Operand::Value(parse_value(t, depth)?))
}

/// The AND between BETWEEN's bounds, with something after it. Anything else
/// where the AND should be is `Expected AND keyword`, as for a statement with
/// no verb (captured eu-west-2 and us-east-1, 2026-10-10, for `x NOT BETWEEN
/// 1`).
fn expect_between_and(t: &mut Tokenizer) -> Result<(), String> {
    if !t.next_token()?.is_some_and(|tok| is_keyword(&tok, "AND")) {
        return Err("Expected AND keyword".to_string());
    }
    match t.peek_token()?.as_deref() {
        None | Some(";") => Err("Expected expression after AND".to_string()),
        Some(_) => Ok(()),
    }
}

/// An IN list, `[...]` or `(...)`, each member read by `member`. The closing
/// token must match the opener. Its operand count is the caller's to check,
/// with `in_limit_refusal`, once it has checked anything DynamoDB reports
/// first.
fn parse_in_list<T>(
    t: &mut Tokenizer,
    depth: usize,
    member: fn(&mut Tokenizer, usize) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    let open = t.next_token()?.ok_or("Expected '(' or '[' after IN")?;
    let close = match open.as_str() {
        "(" => ")",
        "[" => "]",
        other => return Err(format!("Expected '(' or '[' after IN, got '{other}'")),
    };
    let mut values = Vec::new();
    loop {
        let peek = t.peek_token()?.ok_or("Unexpected end of IN list")?;
        if peek == close {
            t.next_token()?;
            return Ok(values);
        }
        if peek == "," {
            t.next_token()?;
            continue;
        }
        values.push(member(t, depth)?);
    }
}

/// DynamoDB's refusal of an IN list of more than `MAX_IN_OPERANDS` members,
/// if this one has more. It is the IN's own refusal, so it comes after its
/// operands', and after the list's own path among its members (captured
/// eu-west-2 and us-east-1, 2026-10-10; eu-west-2, 2026-11-11).
fn in_limit_refusal(count: usize) -> Option<String> {
    (count > MAX_IN_OPERANDS).then(|| {
        format!("The IN operator is provided with too many operands; number of operands: {count}")
    })
}

/// Every member's value, when each member is one.
fn all_values(operands: &[Operand]) -> Option<Vec<PartiqlValue>> {
    operands
        .iter()
        .map(|operand| match operand {
            Operand::Value(value) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

/// Read what follows the left operand of a comparison that isn't on a path:
/// a comparison, BETWEEN or IN, or NOT BETWEEN or NOT IN as their negations
/// (captured eu-west-2 and us-east-1, 2026-10-10, for a size and a literal; a
/// parameter is read the same way). A size on its own isn't a condition, and
/// DynamoDB says so by naming the operator it stands under once the clause is
/// read (captured eu-west-2 and us-east-1, 2026-10-09).
fn finish_comparison(t: &mut Tokenizer, depth: usize, left: Operand) -> Result<BoolExpr, String> {
    if t.peek_token()?
        .is_some_and(|op| is_arithmetic_operator(&op))
    {
        return Ok(BoolExpr::Leaf(arithmetic(t, depth, left)?));
    }
    finish_operator(t, depth, left)
}

/// `+`, `-`, `*` or `/` between two operands.
fn is_arithmetic_operator(tok: &str) -> bool {
    matches!(tok, "+" | "-" | "*" | "/")
}

/// How DynamoDB refuses arithmetic in a condition.
const ARITHMETIC_REFUSAL: &str = "Unsupported operator in Condition Expression. Operator: ";

/// Arithmetic after `left`, the operator not yet read, where it starts the
/// left side of a condition, bracketed or not. DynamoDB takes none in a
/// condition and refuses the first operator ahead of anything wrong with its
/// operands, whatever they are, so
/// `size(true) + 1 > size(s)`, `1 + size(s, s) > 0` and `n + 1 > 0` are each
/// refused for the `+` (captured eu-west-2, 2026-11-11). The terms are read in
/// a loop, so a long run of `+ 1` reads in constant stack, and the comparison
/// that follows, if one does, is read and has no say.
fn arithmetic(t: &mut Tokenizer, depth: usize, left: Operand) -> Result<WhereCondition, String> {
    let refused = arithmetic_terms(t, depth)?;
    if starts_operator_tail(t)? {
        finish_operator(t, depth, left)?;
    }
    Ok(WhereCondition::Refused(refused))
}

/// The refusal of the arithmetic operator about to be read, once it and the
/// terms after it are read past. `-`, `*` and `/` are refused as `+` is,
/// naming the operator (captured eu-west-2, 2026-11-12).
fn arithmetic_terms(t: &mut Tokenizer, depth: usize) -> Result<Box<Refused>, String> {
    let op = t.peek_token()?.unwrap_or_default();
    let refused = t.refused(Vec::new(), format!("{ARITHMETIC_REFUSAL}{op}"));
    while t
        .peek_token()?
        .is_some_and(|op| is_arithmetic_operator(&op))
    {
        t.next_token()?;
        parse_operand(t, depth)?;
    }
    Ok(refused)
}

/// An operand that may be followed by arithmetic: the right-hand side of a
/// comparison, a BETWEEN bound or an IN member. Arithmetic there is refused
/// for its operator, as it is on the left, and stands as a refused operand
/// so it is reported where it was written (captured eu-west-2, 2026-11-12:
/// `n = 1 + 1`, `0 < n + 1`, `n BETWEEN 1 + 1 AND 9` and `n IN [6 - 1]`, the
/// last no longer two members).
fn parse_term(t: &mut Tokenizer, depth: usize) -> Result<Operand, String> {
    let operand = parse_operand(t, depth)?;
    if t.peek_token()?
        .is_some_and(|op| is_arithmetic_operator(&op))
    {
        return Ok(Operand::Refused(arithmetic_terms(t, depth)?));
    }
    Ok(operand)
}

/// Whether what comes next compares the operand before it: a comparison
/// operator, BETWEEN, IN, or NOT BETWEEN or NOT IN.
fn starts_operator_tail(t: &mut Tokenizer) -> Result<bool, String> {
    Ok(match t.peek_token()? {
        Some(tok) if is_comparison_operator(&tok) => true,
        Some(tok) if is_any(&tok, &["BETWEEN", "IN"]) => true,
        Some(tok) if is_keyword(&tok, "NOT") => t
            .peek_token_at(1)?
            .is_some_and(|op| is_any(&op, &["BETWEEN", "IN"])),
        _ => false,
    })
}

/// Whether a condition is arithmetic refused where it was read, which a
/// bracket around it doesn't end: `(size(s) + 1) > 0` reads on past the
/// bracket.
fn is_arithmetic(condition: &BoolExpr) -> bool {
    matches!(condition, BoolExpr::Leaf(WhereCondition::Refused(refused))
        if refused.operands.is_empty() && refused.message.starts_with(ARITHMETIC_REFUSAL))
}

/// The operator after a comparison's left operand and what follows it.
fn finish_operator(t: &mut Tokenizer, depth: usize, left: Operand) -> Result<BoolExpr, String> {
    let next = t.peek_token()?;
    match next.as_deref() {
        Some(tok)
            if is_keyword(tok, "NOT")
                && t.peek_token_at(1)?
                    .is_some_and(|op| is_any(&op, &["BETWEEN", "IN"])) =>
        {
            t.next_token()?; // consume NOT
            Ok(BoolExpr::Not(Box::new(finish_comparison(t, depth, left)?)))
        }
        Some(op) if is_comparison_operator(op) => {
            let op_tok = t.next_token()?.unwrap_or_default();
            let op = comparison_operator(&op_tok)?;
            // A condition function on the right compares as it does written
            // first: `? = attribute_exists(a)` and `size(s) =
            // attribute_exists(a)` run (captured eu-west-2 and us-east-1,
            // 2026-10-10).
            match parse_term(t, depth)? {
                Operand::Condition(holds) => {
                    ConditionFunction { holds: *holds }.compared(&op_tok, left, false)
                }
                right => Ok(BoolExpr::Leaf(comparison(t, left, op, &op_tok, right))),
            }
        }
        Some(tok) if is_keyword(tok, "BETWEEN") => {
            t.next_token()?;
            let low = parse_term(t, depth)?;
            expect_between_and(t)?;
            let high = parse_term(t, depth)?;
            Ok(BoolExpr::Leaf(WhereCondition::CompareBetween(
                left, low, high,
            )))
        }
        Some(tok) if is_keyword(tok, "IN") => {
            t.next_token()?;
            let members = parse_in_list(t, depth, parse_term)?;
            Ok(BoolExpr::Leaf(match in_limit_refusal(members.len()) {
                Some(message) => {
                    let operands = std::iter::once(left).chain(members).collect();
                    WhereCondition::Refused(t.refused(operands, message))
                }
                None => WhereCondition::CompareIn(left, members),
            }))
        }
        // IS on a size is refused ahead of anything wrong with the size, its
        // operand count included (captured eu-west-2, 2026-11-11).
        Some(tok)
            if is_keyword(tok, "IS") && matches!(left, Operand::Size(_) | Operand::Refused(_)) =>
        {
            Ok(BoolExpr::Leaf(WhereCondition::Refused(refuse_is(t)?)))
        }
        // A size refused for its operand count stands as a size does, and the
        // walk reports its count first.
        _ if matches!(left, Operand::Size(_) | Operand::Refused(_)) => {
            Ok(BoolExpr::NonBoolean("N", left))
        }
        _ => Err("Expected operator after attribute path".to_string()),
    }
}

/// IS, about to be read, after a left side it can't take: a call. It is
/// refused ahead of anything wrong with the call, its operand count included,
/// and the type after it is read past.
fn refuse_is(t: &mut Tokenizer) -> Result<Box<Refused>, String> {
    let refused = t.refused(
        Vec::new(),
        "The left hand side argument to IS must be a Variable Reference, Literal, or Path"
            .to_string(),
    );
    t.next_token()?;
    if t.peek_token()?.is_some_and(|tok| is_keyword(&tok, "NOT")) {
        t.next_token()?;
    }
    read_type_name(t)?;
    Ok(refused)
}

/// A comparison of two operands, in the form the executor and the key
/// pushdown read when one side is a path and the other a value.
fn comparison(
    t: &mut Tokenizer,
    left: Operand,
    op: CompOp,
    op_tok: &str,
    right: Operand,
) -> WhereCondition {
    match (left, right) {
        (Operand::Path(path), Operand::Value(value)) => {
            WhereCondition::Comparison(Condition { path, op, value })
        }
        (Operand::Value(value), Operand::Path(path)) => WhereCondition::Comparison(Condition {
            path,
            op: mirrored(op),
            value,
        }),
        (Operand::Path(a), Operand::Path(b)) if a == b => {
            let message = not_distinct(op_tok, &a);
            WhereCondition::Refused(t.refused(vec![Operand::Path(a), Operand::Path(b)], message))
        }
        (left, right) => WhereCondition::Compare(left, op, right),
    }
}

/// The index of the SELECT in a call's arguments, if they hold a subquery: the
/// call's name is at `t.pos` and its `(` after it.
fn subquery_select(t: &Tokenizer) -> Option<usize> {
    let mut open = 0usize;
    for (i, tok) in t.tokens.iter().enumerate().skip(t.pos + 1) {
        match tok.as_str() {
            "(" => open += 1,
            ")" => {
                open -= 1;
                if open == 0 {
                    return None;
                }
            }
            tok if is_keyword(tok, "SELECT") => return Some(i),
            _ => {}
        }
    }
    None
}

/// Read past a call whose refusal is noted, and any comparison it is part of.
fn skip_call(t: &mut Tokenizer, depth: usize) -> Result<(), String> {
    skip_call_operands(t)?;
    skip_comparison(t, depth)
}

/// Read past a call whose refusal is noted: its name, and its operands to the
/// bracket that closes them.
fn skip_call_operands(t: &mut Tokenizer) -> Result<(), String> {
    t.next_token()?;
    let mut open = 0usize;
    loop {
        match t.next_token()?.as_deref() {
            Some("(") => open += 1,
            Some(")") if open == 1 => return Ok(()),
            Some(")") => open -= 1,
            Some(_) => {}
            None => return Err("Expected ')'".to_string()),
        }
    }
}

/// Read past a comparison after a term whose refusal is noted.
fn skip_comparison(t: &mut Tokenizer, depth: usize) -> Result<(), String> {
    if let Some(op) = t.peek_token()?
        && is_comparison_operator(&op)
    {
        t.next_token()?;
        parse_operand(t, depth)?;
    }
    Ok(())
}

/// Drive `NOT` down to the leaves by De Morgan, so the tree that reaches
/// `to_dnf` is a plain AND/OR of conditions.
fn push_not_down(expr: BoolExpr, negated: bool) -> Result<BoolExpr, String> {
    match expr {
        BoolExpr::Not(inner) => push_not_down(*inner, !negated),
        BoolExpr::And(terms) => {
            let terms = terms
                .into_iter()
                .map(|e| push_not_down(e, negated))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(if negated {
                BoolExpr::Or(terms)
            } else {
                BoolExpr::And(terms)
            })
        }
        BoolExpr::Or(terms) => {
            let terms = terms
                .into_iter()
                .map(|e| push_not_down(e, negated))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(if negated {
                BoolExpr::And(terms)
            } else {
                BoolExpr::Or(terms)
            })
        }
        BoolExpr::Leaf(cond) => {
            if negated {
                negate_condition(cond)
            } else {
                Ok(BoolExpr::Leaf(cond))
            }
        }
        BoolExpr::NonBoolean(..) => {
            unreachable!("a clause with a value for a condition is never flattened")
        }
    }
}

/// The negation of a single condition.
///
/// A comparison is wrapped rather than flipped. Flipping the operator loses the
/// rows that have no such attribute at all: the comparison is false on those,
/// so its negation must be true, and `NotComparison` is what says so. See the
/// variant's own note.
///
/// Two of these are not single conditions once negated and become subtrees:
/// `NOT BETWEEN` is a disjunction and `NOT IN` a conjunction. Both expand
/// through the wrapped form as well, so a row missing the attribute survives
/// them for the same reason.
fn negate_condition(cond: WhereCondition) -> Result<BoolExpr, String> {
    use WhereCondition as W;
    // A negated comparison on `path`, as its own condition.
    let not_cmp = |path: String, op: CompOp, value: PartiqlValue| -> BoolExpr {
        W::NotComparison(Condition { path, op, value }).into()
    };
    Ok(match cond {
        W::Comparison(c) => W::NotComparison(c).into(),
        W::NotComparison(c) => W::Comparison(c).into(),
        W::Exists(p) => W::NotExists(p).into(),
        W::NotExists(p) => W::Exists(p).into(),
        W::BeginsWith(p, v) => W::NotBeginsWith(p, v).into(),
        W::NotBeginsWith(p, v) => W::BeginsWith(p, v).into(),
        W::IsMissing(p) => W::IsNotMissing(p).into(),
        W::IsNotMissing(p) => W::IsMissing(p).into(),
        W::IsType(p, test) => W::IsNotType(p, test).into(),
        W::IsNotType(p, test) => W::IsType(p, test).into(),
        W::Contains(p, v) => W::NotContains(p, v).into(),
        W::NotContains(p, v) => W::Contains(p, v).into(),
        // Not `< low OR > high`, which would drop a row with no such attribute.
        W::Between(p, low, high) => W::NotBetween(p, low, high).into(),
        W::NotBetween(p, low, high) => W::Between(p, low, high).into(),
        // De Morgan copies the path once per value; refuse before copying a
        // long path thousands of times.
        W::In(p, values) if p.len().saturating_mul(values.len()) > MAX_CLAUSE_BYTES => {
            return Err(TOO_COMPLEX.to_string());
        }
        W::In(p, values) => BoolExpr::And(
            values
                .into_iter()
                .map(|v| not_cmp(p.clone(), CompOp::Eq, v))
                .collect(),
        ),
        W::Not(inner) => (*inner).into(),
        // Refused before it runs, so its negation is itself.
        refused @ W::Refused(_) => refused.into(),
        other @ (W::Compare(..)
        | W::CompareBetween(..)
        | W::CompareIn(..)
        | W::AttributeType(..)
        | W::Search(..)) => W::Not(Box::new(other)).into(),
    })
}

impl From<WhereCondition> for BoolExpr {
    fn from(cond: WhereCondition) -> Self {
        BoolExpr::Leaf(cond)
    }
}

/// How many OR groups a flattened clause may have.
///
/// Distributing an AND over ORs multiplies group counts, so a clause built from
/// nested alternations can expand far beyond what was written. The cap keeps a
/// pathological statement a rejection rather than an allocation.
pub(crate) const MAX_OR_GROUPS: usize = 256;

/// How deeply a statement may nest.
///
/// The companion to `MAX_OR_GROUPS`, which bounds a clause that expands rather
/// than one that nests. The boolean-expression parser, the value and operand
/// parsers and the reader for a statement with no verb are recursive descents,
/// so a WHERE clause written with hundreds of parentheses, or an item literal
/// holding hundreds of nested maps, would recurse until the stack runs out.
/// That is not a failure the engine can report: the release profile aborts on
/// panic, so a stack overflow takes the whole host process down rather than the
/// one statement. The budget keeps a pathological statement a rejection.
///
/// Every descent counts from 0 and refuses once its depth passes this value, so
/// a statement may nest 65 levels. Runs of NOT and chains of AND, OR or signs
/// are read in loops and don't spend depth.
const MAX_EXPR_DEPTH: usize = 64;

/// How many conditions an AND product may hold.
///
/// `MAX_OR_GROUPS` bounds the number of groups, not the conditions inside them,
/// and a single group can be wide: `NOT a IN [...]` is one group of up to
/// `MAX_IN_OPERANDS` leaves under De Morgan. A handful of negated conjunctions
/// then repeats that group up to `MAX_OR_GROUPS` times, and every slot is a
/// condition evaluated against every item read. Each AND budgets its product
/// before building it, which keeps that a rejection. It is per product, not
/// per clause: an alternation of products is held to `MAX_OR_SLOTS` instead.
const MAX_TOTAL_CONDITIONS: usize = 4096;

/// How many conditions an alternation's arms may hold between them.
///
/// Each arm is already held to `MAX_TOTAL_CONDITIONS`, and alternatives side by
/// side are no reason to refuse a statement DynamoDB runs, so the alternation
/// gets four times that. It is still a bound: every slot is evaluated against
/// every item read, and without one a clause of a few hundred operators could
/// hold tens of thousands of slots. The limit is Dynoxide's own; DynamoDB
/// evaluates the clause without flattening it and has no such limit.
const MAX_OR_SLOTS: usize = 4 * MAX_TOTAL_CONDITIONS;

/// The rejection every clause-complexity budget shares.
const TOO_COMPLEX: &str = "WHERE clause is too complex to evaluate";

/// How many bytes of paths and literal values a clause's conditions may hold.
///
/// Flattening shares conditions rather than copying them, so a condition
/// repeated across groups costs its bytes once, and the budget counts each
/// condition once. The one place a clause holds more than it was written with
/// is De Morgan on a negated `IN`, which copies the path once per operand; an
/// `IN` takes at most `MAX_IN_OPERANDS`, and a statement over 8,192
/// characters (up to 32 KiB of UTF-8) is refused before it is parsed, so a
/// statement DynamoDB accepts holds at most about 100 x 32 KiB, a little over
/// 3 MiB. The budget sits above that and so refuses nothing DynamoDB would
/// run; it bounds what a caller of the parser itself, without the length
/// limit, can make a clause hold.
const MAX_CLAUSE_BYTES: usize = 4 * 1024 * 1024;

/// How many operands an `IN` may take: DynamoDB refuses the 101st, negated or
/// not, as `The IN operator is provided with too many operands; number of
/// operands: 101` (captured eu-west-2, 2026-10-10).
const MAX_IN_OPERANDS: usize = 100;

/// The bytes a condition holds in paths and literal values, for
/// `MAX_CLAUSE_BYTES`. Parameters are referred to by index, so they cost
/// nothing here.
fn condition_bytes(condition: &WhereCondition) -> usize {
    fn value(v: &PartiqlValue) -> usize {
        match v {
            PartiqlValue::Literal(av) => av.size(),
            PartiqlValue::Parameter(_) => 0,
        }
    }
    fn operand(o: &Operand) -> usize {
        match o {
            Operand::Path(p) => p.len(),
            Operand::Value(v) => value(v),
            Operand::Size(inner) => operand(inner),
            Operand::Condition(inner) => condition_bytes(inner),
            Operand::Refused(refused) => refused.operands.iter().map(operand).sum(),
        }
    }
    use WhereCondition as W;
    match condition {
        W::Comparison(c) | W::NotComparison(c) => c.path.len() + value(&c.value),
        W::Exists(p) | W::NotExists(p) | W::IsMissing(p) | W::IsNotMissing(p) => p.len(),
        W::IsType(p, _) | W::IsNotType(p, _) => p.len(),
        W::BeginsWith(p, v) | W::NotBeginsWith(p, v) | W::Contains(p, v) | W::NotContains(p, v) => {
            p.len() + value(v)
        }
        W::Between(p, low, high) | W::NotBetween(p, low, high) => {
            p.len() + value(low) + value(high)
        }
        W::In(p, values) => p.len() + values.iter().map(value).sum::<usize>(),
        W::Compare(left, _, right) => operand(left) + operand(right),
        W::CompareBetween(o, low, high) => operand(o) + operand(low) + operand(high),
        W::CompareIn(o, values) => operand(o) + values.iter().map(operand).sum::<usize>(),
        W::AttributeType(o, kind) | W::Search(_, o, kind) => operand(o) + operand(kind),
        W::Not(inner) => condition_bytes(inner),
        W::Refused(refused) => refused.operands.iter().map(operand).sum(),
    }
}

/// The rejection every descent shares when it runs past `MAX_EXPR_DEPTH`.
const TOO_DEEP: &str = "Statement is nested too deeply to parse";

/// How many operators a WHERE clause may hold: DynamoDB refuses the 301st,
/// always as `operator count: 301`. AND, OR, NOT, a comparison operator, a
/// call to a WHERE function, IN, BETWEEN and IS each count one, so `pk = 'a'`
/// followed by 149 `AND x = 1` (299 operators) runs and 150 is refused,
/// `AND size(s) > 0` costs three, and so do `AND x IS NOT NULL` and
/// `AND x NOT IN [1, 2]` (captured eu-west-2 and us-east-1, 2026-10-10).
const MAX_OPERATORS: usize = 300;

/// The OR-of-ANDs a clause flattens to: shared conditions, grouped.
type Groups = Vec<Vec<Arc<WhereCondition>>>;

/// How many condition slots a set of groups holds.
fn slots(groups: &Groups) -> usize {
    groups.iter().map(Vec::len).sum()
}

/// Flatten an AND/OR tree into the OR-of-ANDs the rest of the engine reads.
///
/// An AND works out how many groups and condition slots its product will hold
/// before building it, and refuses a clause past `MAX_OR_GROUPS` or
/// `MAX_TOTAL_CONDITIONS` then, so no product larger than the budget is ever
/// built only to be thrown away. An OR caps its groups and holds its arms'
/// slots to `MAX_OR_SLOTS` between them, checking each arm before adding it.
/// The conditions themselves are shared, so a slot costs a pointer however
/// long the path or literal it names.
pub(crate) fn to_dnf(tree: &ConditionTree) -> Result<Groups, String> {
    let too_complex = || Err(TOO_COMPLEX.to_string());
    match tree {
        ConditionTree::Leaf(cond) => Ok(vec![vec![Arc::clone(cond)]]),
        ConditionTree::Or(terms) => {
            // Each arm has already held its own slots to the per-product
            // budget, and alternatives side by side are no reason to refuse:
            // DynamoDB runs far wider alternations (captured eu-west-2,
            // 2026-10-10). Together they are held to the looser `MAX_OR_SLOTS`,
            // checked before each arm is added.
            let mut out: Groups = Vec::new();
            let mut held = 0usize;
            for term in terms {
                let term_groups = to_dnf(term)?;
                let term_slots = slots(&term_groups);
                if out.len().saturating_add(term_groups.len()) > MAX_OR_GROUPS
                    || held.saturating_add(term_slots) > MAX_OR_SLOTS
                {
                    return too_complex();
                }
                held += term_slots;
                out.extend(term_groups);
            }
            Ok(out)
        }
        ConditionTree::And(terms) => {
            let mut out: Groups = vec![Vec::new()];
            let mut held = 0usize;
            for term in terms {
                let term_groups = to_dnf(term)?;
                let term_slots = slots(&term_groups);
                // Every group in hand meets every group of the term, so the
                // product holds each held slot once per term group and each
                // term slot once per group in hand.
                let next_groups = out.len().saturating_mul(term_groups.len());
                let next_slots = held
                    .saturating_mul(term_groups.len())
                    .saturating_add(term_slots.saturating_mul(out.len()));
                if next_groups > MAX_OR_GROUPS || next_slots > MAX_TOTAL_CONDITIONS {
                    return too_complex();
                }
                // The common shape: the term is a single conjunction, so the
                // cross product is just an append onto every group in hand.
                if let [only] = term_groups.as_slice() {
                    for group in out.iter_mut() {
                        group.extend(only.iter().cloned());
                    }
                } else {
                    let mut next = Vec::with_capacity(next_groups);
                    for existing in &out {
                        for group in &term_groups {
                            let mut combined = Vec::with_capacity(existing.len() + group.len());
                            combined.extend(existing.iter().cloned());
                            combined.extend(group.iter().cloned());
                            next.push(combined);
                        }
                    }
                    out = next;
                }
                held = next_slots;
            }
            Ok(out)
        }
    }
}

/// Parse a single condition (comparison, function call, etc.).
///
/// `depth` is the boolean nesting already spent getting here, and it carries on
/// into the value parser rather than restarting at zero. The two descents share
/// one stack, so a budget each let a clause nested to the limit hold a value
/// nested to the limit and spend twice the budget in frames.
fn parse_single_condition(t: &mut Tokenizer, depth: usize) -> Result<WhereCondition, String> {
    // Where a condition should start, DynamoDB reports the end of the
    // statement as an unexpected term, an operator as an unexpected operator
    // and a clause keyword as an unexpected keyword (captured eu-west-2,
    // us-east-1, eu-west-1 and us-west-2, 2026-10-09).
    let path_start = t.pos;
    let tok = match t.next_token()? {
        None => return Err(UNEXPECTED_TERM.to_string()),
        Some(tok) if tok == ";" => return Err(UNEXPECTED_TERM.to_string()),
        Some(tok) => tok,
    };
    if is_any(&tok, OPERATOR_WORDS) {
        return Err(UNEXPECTED_OPERATOR.to_string());
    }
    if is_any(&tok, CLAUSE_KEYWORDS) {
        return Err(UNEXPECTED_KEYWORD.to_string());
    }
    let tok_upper = tok.to_uppercase();

    match tok_upper.as_str() {
        "EXISTS" => {
            expect_char(t, "(")?;
            let path = parse_function_path(t)?;
            expect_char(t, ")")?;
            Ok(WhereCondition::Exists(path))
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
                // Bounds and members that are all values keep the path forms
                // the key pushdown reads; a path or size among them makes it a
                // comparison of operands, read item by item.
                "BETWEEN" => {
                    t.next_token()?; // consume BETWEEN
                    path_between(t, depth, path)
                }
                "IN" => {
                    t.next_token()?; // consume IN
                    let members = parse_in_list(t, depth, parse_term)?;
                    Ok(path_in(t, path, members))
                }
                // `x NOT IN [...]` and `x NOT BETWEEN lo AND hi` read as
                // `NOT x IN [...]` and `NOT x BETWEEN lo AND hi` (DynamoDB runs
                // both: captured eu-west-2 and us-east-1, 2026-10-10); the
                // caller turns the wrapper into the same tree the leading NOT
                // builds. Any other word after the NOT ends the expression
                // there: `x NOT NOT BETWEEN ...` is an unexpected token
                // (captured), and so, inferred from it, is `x NOT = 1`. `x NOT
                // LIKE` is left as it was, since LIKE isn't one DynamoDB was
                // asked about.
                "NOT" => {
                    let after = t.peek_token_at(1)?.unwrap_or_default();
                    if is_keyword(&after, "IN") {
                        t.next_token()?; // consume NOT
                        t.next_token()?; // consume IN
                        let members = parse_in_list(t, depth, parse_term)?;
                        Ok(WhereCondition::Not(Box::new(path_in(t, path, members))))
                    } else if is_keyword(&after, "BETWEEN") {
                        t.next_token()?; // consume NOT
                        t.next_token()?; // consume BETWEEN
                        Ok(WhereCondition::Not(Box::new(path_between(t, depth, path)?)))
                    } else if is_keyword(&after, "LIKE") {
                        Err("Unknown operator: NOT".to_string())
                    } else {
                        Err("Unexpected token after expression".to_string())
                    }
                }
                "IS" => {
                    t.next_token()?; // consume IS
                    let negated = t.peek_token()?.is_some_and(|tok| is_keyword(&tok, "NOT"));
                    if negated {
                        t.next_token()?;
                    }
                    match read_type_name(t)? {
                        None => Err("Expected type name".to_string()),
                        Some(TypeName::Missing) if negated => {
                            Ok(WhereCondition::IsNotMissing(path))
                        }
                        Some(TypeName::Missing) => Ok(WhereCondition::IsMissing(path)),
                        Some(TypeName::Test(test)) if negated => {
                            Ok(WhereCondition::IsNotType(path, test))
                        }
                        Some(TypeName::Test(test)) => Ok(WhereCondition::IsType(path, test)),
                        Some(TypeName::Unsupported(name)) => {
                            let at = t.position(t.span_at(path_start));
                            t.note_term(format!(
                                "Unsupported data type for IS operator: {name}. Supported \
                                 data types are [MISSING, NULL, BOOLEAN, BLOB, STRING, \
                                 NUMERIC, STRUCT, TUPLE, LIST] at {at}"
                            ));
                            // Never evaluated: the statement is refused once it
                            // has parsed.
                            Ok(WhereCondition::IsMissing(path))
                        }
                    }
                }
                op if is_arithmetic_operator(op) => arithmetic(t, depth, Operand::Path(path)),
                _ => {
                    // Standard comparison: path op operand
                    let op_tok = t.next_token()?.ok_or("Expected comparison operator")?;
                    let op = comparison_operator(&op_tok)?;
                    let right = parse_term(t, depth)?;
                    Ok(comparison(t, Operand::Path(path), op, &op_tok, right))
                }
            }
        }
    }
}

/// `path BETWEEN lo AND hi`, BETWEEN already read: bounds that are both
/// values keep the form the key pushdown reads; a path or size among them
/// makes it a comparison of operands, read item by item. The path itself as a
/// bound is refused.
fn path_between(t: &mut Tokenizer, depth: usize, path: String) -> Result<WhereCondition, String> {
    let low = parse_term(t, depth)?;
    expect_between_and(t)?;
    let high = parse_term(t, depth)?;
    if names_path(&[&low, &high], &path) {
        let message = not_distinct("BETWEEN", &path);
        return Ok(WhereCondition::Refused(
            t.refused(vec![Operand::Path(path), low, high], message),
        ));
    }
    Ok(match (low, high) {
        (Operand::Value(low), Operand::Value(high)) => WhereCondition::Between(path, low, high),
        (low, high) => WhereCondition::CompareBetween(Operand::Path(path), low, high),
    })
}

/// `path IN [...]`: members that are all values keep the form the key
/// pushdown reads; a path or size among them makes it a comparison of
/// operands, read item by item. The path itself among the members is refused.
fn path_in(t: &mut Tokenizer, path: String, members: Vec<Operand>) -> WhereCondition {
    let refusal = if names_path(&members.iter().collect::<Vec<_>>(), &path) {
        Some(not_distinct("IN", &path))
    } else {
        in_limit_refusal(members.len())
    };
    if let Some(message) = refusal {
        let operands = std::iter::once(Operand::Path(path))
            .chain(members)
            .collect();
        return WhereCondition::Refused(t.refused(operands, message));
    }
    match all_values(&members) {
        Some(values) => WhereCondition::In(path, values),
        None => WhereCondition::CompareIn(Operand::Path(path), members),
    }
}

/// Whether any of `operands` is `path` itself. A size of it, or a value, is
/// a different operand (captured eu-west-2 and us-east-1, 2026-10-10).
fn names_path(operands: &[&Operand], path: &str) -> bool {
    operands
        .iter()
        .any(|operand| matches!(operand, Operand::Path(other) if other == path))
}

/// DynamoDB's refusal of an operator or function handed the path it tests
/// among its other operands, wherever it stands among them: `x = x`,
/// `x IN [1, x]`, `x BETWEEN 1 AND x`. The path is named by its parts, so
/// `m.a` is `[m, a]` and `l[0]` is `[l, [0]]` (captured eu-west-2 and
/// us-east-1, 2026-10-10).
fn not_distinct(operator: &str, path: &str) -> String {
    format!(
        "The first operand must be distinct from the remaining operands for this operator or \
         function; operator: {operator}, first operand: [{}]",
        path_parts(path)
    )
}

/// A path's parts as DynamoDB lists them: `a.b[0]` is `a, b, [0]`. A path
/// the splitter can't read is listed whole.
fn path_parts(path: &str) -> String {
    match super::split_path_segments(path) {
        Some(segments) => segments
            .iter()
            .map(|segment| match segment {
                super::PathSegment::Key(key) => key.to_string(),
                super::PathSegment::Index(index) => format!("[{index}]"),
            })
            .collect::<Vec<_>>()
            .join(", "),
        None => path.to_string(),
    }
}

/// How many tokens ahead a path starting at the next token ends: a name, then
/// any `.name` and `[index]` steps.
fn path_end(t: &Tokenizer) -> Result<usize, String> {
    let mut end = 1;
    loop {
        match t.peek_token_at(end)?.as_deref() {
            Some(".") => end += 2,
            Some("[") => end += 3,
            _ => return Ok(end),
        }
    }
}

/// Parse a dotted path starting from an already-consumed first token.
/// Greedily consumes `.segment` continuations.
fn parse_dotted_path_from_token(first_tok: &str, t: &mut Tokenizer) -> Result<String, String> {
    let mut path = path_segment(first_tok);
    while let Some(ref next) = t.peek_token()? {
        if next == "." {
            t.next_token()?; // consume dot
            let seg = t.next_token()?.ok_or("Expected attribute name after '.'")?;
            check_dot_component(&seg)?;
            path.push('.');
            path.push_str(&path_segment(&seg));
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

/// One step of a path as written: a name, quoted or not, as the path text
/// holds it. A quoted name is one attribute whatever it holds, so one with a
/// `.` or a `[` in it stays quoted in the path (see `attribute_path`).
fn path_segment(tok: &str) -> String {
    let name = unquote(tok);
    if tok.starts_with('"') {
        super::attribute_path(&name).into_owned()
    } else {
        name
    }
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

/// `depth` counts the collection levels above this value, so a literal nested
/// past `MAX_EXPR_DEPTH` is a rejection rather than a stack overflow.
///
/// Nothing read inside the value counts toward a WHERE clause's operators.
fn parse_value(t: &mut Tokenizer, depth: usize) -> Result<PartiqlValue, String> {
    t.reading_literal += 1;
    let value = read_value(t, depth);
    t.reading_literal -= 1;
    value
}

fn read_value(t: &mut Tokenizer, depth: usize) -> Result<PartiqlValue, String> {
    if depth > MAX_EXPR_DEPTH {
        return Err(TOO_DEEP.to_string());
    }
    t.note_depth();
    let tok = t.next_token()?.ok_or("Expected value")?;

    if tok == "?" {
        let idx = t.next_param_index();
        return Ok(PartiqlValue::Parameter(idx));
    }

    // String literal: 'value'
    // An operator or a clause keyword where a value should start is refused
    // as DynamoDB refuses it (captured eu-west-2 and us-east-1, 2026-10-09:
    // `pk = LIKE`, `pk = IN`, `pk = WHERE`).
    if !is_keyword(&tok, "NOT") && is_any(&tok, OPERATOR_WORDS) {
        return Err(UNEXPECTED_OPERATOR.to_string());
    }
    if is_any(&tok, CLAUSE_KEYWORDS) {
        return Err(UNEXPECTED_KEYWORD.to_string());
    }

    if tok.starts_with('\'') && tok.ends_with('\'') && tok.len() >= 2 {
        let s = tok[1..tok.len() - 1].to_string();
        return Ok(PartiqlValue::Literal(AttributeValue::S(s)));
    }

    // An empty bag, which the tokenizer reads as `<`, `<>`, `>`.
    if tok == "<"
        && t.peek_token()?.as_deref() == Some("<>")
        && t.peek_token_at(1)?.as_deref() == Some(">")
    {
        t.next_token()?;
        t.next_token()?;
        t.note_empty_bag();
        return Ok(PartiqlValue::Literal(AttributeValue::SS(Vec::new())));
    }

    // Set literal: << val1, val2 >>
    if tok == "<" && t.peek_token()?.as_deref() == Some("<") {
        t.next_token()?; // consume second <
        return Ok(PartiqlValue::Literal(parse_bag(t, depth)?));
    }

    if tok.starts_with('`') {
        return Ok(PartiqlValue::Literal(ion_literal(t, &tok)?));
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
            t.value_path.push(format!("[{}]", items.len()));
            let item = parse_value(t, depth + 1);
            t.value_path.pop();
            items.push(item?);
            expect_separator(t, "]", "Expected RIGHT_BRACKET")?;
        }
        // We can only produce a Literal list if all elements are literals
        let mut avs = Vec::new();
        for item in items {
            match item {
                PartiqlValue::Literal(av) => avs.push(av),
                PartiqlValue::Parameter(_) => {
                    // Can't build a static list with parameters in it at parse time.
                    // For now, return an error: a more complete solution would
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
            t.note_key(&key);
            let colon = t.next_token()?.ok_or("Expected ':'")?;
            if colon != ":" {
                return Err(format!("Expected ':' but got '{colon}'"));
            }
            t.value_path.push(format!(".{key}"));
            let val = parse_value(t, depth + 1);
            t.value_path.pop();
            match val? {
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
            expect_separator(t, "}", "Expected RIGHT_CURLY")?;
        }
        return Ok(PartiqlValue::Literal(AttributeValue::M(map)));
    }

    // Negative number: `-` followed by a numeric token
    if (tok == "-" || tok == "+")
        && let Some(ref next) = t.peek_token()?
        && is_number_token(next)
    {
        let num = t.next_token()?.unwrap();
        let text = format!("{tok}{num}");
        t.note_number(&text);
        return Ok(PartiqlValue::Literal(AttributeValue::N(text)));
    }

    // Numeric literal
    if is_number_token(&tok) {
        t.note_number(&tok);
        return Ok(PartiqlValue::Literal(AttributeValue::N(tok)));
    }

    // Boolean / null
    match tok.to_uppercase().as_str() {
        "TRUE" => return Ok(PartiqlValue::Literal(AttributeValue::BOOL(true))),
        "FALSE" => return Ok(PartiqlValue::Literal(AttributeValue::BOOL(false))),
        "NULL" => return Ok(PartiqlValue::Literal(AttributeValue::NULL(true))),
        _ => {}
    }

    // Bare identifier: treat as a string (attribute name reference in SET expressions)
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
                check_dot_component(&seg)?;
                path.push('.');
                path.push_str(&path_segment(&seg));
            } else {
                break;
            }
        }
        return Ok(PartiqlValue::Literal(AttributeValue::S(path)));
    }

    // A point or a sign with no number after it can't start a term (captured
    // eu-west-2, 2026-10-09: `.`, `.e5`, `-.` and `..5`).
    if matches!(tok.as_str(), "." | "-" | "+") {
        return Err("Unexpected term".to_string());
    }

    Err(format!("Unexpected value token: {tok}"))
}

/// One number literal, as DynamoDB's PartiQL reads it. The message comes back
/// without the path, for the caller to place.
///
/// The usual range and precision checks apply, with one difference at the very
/// top of the exponent range. DynamoDB reads the literal as Java's
/// `BigDecimal` and reports underflow, not overflow, once its precision less its
/// scale reaches `i32::MAX`: `1e2147483645` is an overflow and `1e2147483646`
/// an underflow, and `1e2147483647` is an underflow where PutItem calls the
/// same number an overflow (captured eu-west-2, 2026-10-09).
fn check_literal_number(n: &str) -> Result<(), String> {
    if let Some((precision, scale)) = literal_precision_and_scale(n)
        && precision - scale >= i64::from(i32::MAX)
    {
        return Err(
            "Number underflow. Attempting to store a number with magnitude smaller \
             than supported range"
                .to_string(),
        );
    }
    crate::types::validate_dynamo_number(n).map_err(|e| match e {
        crate::errors::DynoxideError::ValidationException(message) => message,
        other => other.to_string(),
    })
}

/// A literal's precision and scale as `BigDecimal` counts them: precision is the
/// number of digits once leading zeros are dropped (at least one, so zero has
/// a precision of 1), and scale is the digits written after the point less the
/// exponent. `None` when the exponent does not read as a number, which the
/// tokeniser has already refused for anything written into a statement.
fn literal_precision_and_scale(n: &str) -> Option<(i64, i64)> {
    let unsigned = n.trim_start_matches(['+', '-']);
    let (coefficient, exponent) = match unsigned.find(['e', 'E']) {
        Some(at) => (&unsigned[..at], unsigned[at + 1..].parse::<i64>().ok()?),
        None => (unsigned, 0),
    };
    let fraction_digits = coefficient
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let digits: String = coefficient.chars().filter(char::is_ascii_digit).collect();
    let significant = digits.trim_start_matches('0').len().max(1);
    let scale = i64::try_from(fraction_digits).ok()? - exponent;
    Some((i64::try_from(significant).ok()?, scale))
}

/// Whether a token is a number: it starts with a digit, or with a point and a
/// digit (`.5`), which the tokeniser only produces where a value can start.
fn is_number_token(tok: &str) -> bool {
    let mut chars = tok.chars();
    match chars.next() {
        Some(c) if c.is_ascii_digit() => true,
        Some('.') => chars.next().is_some_and(|c| c.is_ascii_digit()),
        _ => false,
    }
}

/// What a bag literal's member is, as far as DynamoDB's bag rules go.
enum BagMember {
    Str(String),
    Num(String),
    Null,
    Parameter,
    Ion(&'static str),
    /// Anything else a bag can't hold: a boolean, a list, a map, a bag, a path.
    Other,
}

const BAG_REFUSAL: &str =
    "Unsupported data type in Bag. DynamoDB only supports either numbers or strings in bags";

/// Read a bag literal after its `<<`: a string set, a number set, or the
/// refusal DynamoDB gives the first member it can't hold.
///
/// A bag that starts with a string is refused at the first member that isn't
/// one, and a bag that starts with a number without a position; a parameter,
/// an Ion literal or a `null` after the first member gets a refusal of its own,
/// and a bag that starts with anything else is refused at its first member
/// (captured eu-west-2 and us-east-1, 2026-10-09). A refused bag reads as an
/// empty string set, which is never used: the statement is refused once it has
/// parsed.
fn parse_bag(t: &mut Tokenizer, depth: usize) -> Result<AttributeValue, String> {
    let mut members: Vec<(BagMember, Span)> = Vec::new();
    loop {
        let peek = t.peek_token()?.ok_or("Unexpected end of set literal")?;
        if peek == ">" {
            t.next_token()?;
            if t.peek_token()?.as_deref() == Some(">") {
                t.next_token()?;
            }
            break;
        }
        if peek == "," {
            t.next_token()?;
            continue;
        }
        let start = t.pos;
        let member = read_bag_member(t, depth)?;
        let span = member_span(t, start, &member);
        members.push((member, span));
        expect_separator(t, ">", "Expected RIGHT_DOUBLE_ANGLE_BRACKET")?;
    }
    let Some((first, first_span)) = members.first() else {
        t.note_empty_bag();
        return Ok(AttributeValue::SS(Vec::new()));
    };
    let strings = matches!(first, BagMember::Str(_));
    let path: String = t.value_path.concat();
    let own_refusal = |member: &BagMember, span: &Span| match member {
        BagMember::Parameter => Some(format!(
            "Unsupported data type: Parameter under key root{path} at {}",
            t.position(*span)
        )),
        BagMember::Ion(kind) => Some(format!(
            "Ion Literals are not supported: {kind} under key root{path} at {}",
            t.position(*span)
        )),
        _ => None,
    };
    let refusal = match first {
        BagMember::Str(_) => members[1..]
            .iter()
            .find(|(m, _)| !matches!(m, BagMember::Str(_)))
            .map(|(m, span)| {
                own_refusal(m, span)
                    .unwrap_or_else(|| format!("{BAG_REFUSAL} at {}", t.position(*span)))
            }),
        BagMember::Num(_) => members[1..]
            .iter()
            .find(|(m, _)| !matches!(m, BagMember::Num(_)))
            .map(|(m, span)| {
                own_refusal(m, span).unwrap_or_else(|| match m {
                    BagMember::Null => format!(
                        "One or more parameter values were invalid: Numeric sets may not \
                         contain null under root{path}"
                    ),
                    _ => BAG_REFUSAL.to_string(),
                })
            }),
        _ => Some(format!("{BAG_REFUSAL} at {}", t.position(*first_span))),
    };
    if let Some(message) = refusal {
        t.note_literal(message);
        return Ok(AttributeValue::SS(Vec::new()));
    }
    let set = if strings {
        AttributeValue::SS(
            members
                .into_iter()
                .filter_map(|(m, _)| match m {
                    BagMember::Str(s) => Some(s),
                    _ => None,
                })
                .collect(),
        )
    } else {
        AttributeValue::NS(
            members
                .into_iter()
                .filter_map(|(m, _)| match m {
                    BagMember::Num(n) => Some(n),
                    _ => None,
                })
                .collect(),
        )
    };
    t.note_set_literal(&set);
    Ok(set)
}

/// Read one member of a bag literal.
fn read_bag_member(t: &mut Tokenizer, depth: usize) -> Result<BagMember, String> {
    let tok = t.peek_token()?.ok_or("Unexpected end of set literal")?;
    if tok == "?" {
        t.next_token()?;
        t.next_param_index();
        return Ok(BagMember::Parameter);
    }
    if tok.starts_with('`') {
        t.next_token()?;
        return ion_type(&tok).map(BagMember::Ion).ok_or_else(String::new);
    }
    if tok.starts_with('\'') && tok.ends_with('\'') && tok.len() >= 2 {
        t.next_token()?;
        return Ok(BagMember::Str(tok[1..tok.len() - 1].to_string()));
    }
    if is_keyword(&tok, "NULL") {
        t.next_token()?;
        return Ok(BagMember::Null);
    }
    // A name is a path, which a bag can't hold. One that runs straight into
    // another term leaves the bag unclosed: `X'616263'` is not a binary
    // literal.
    if tok.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && !is_any(&tok, &["TRUE", "FALSE"])
    {
        t.next_token()?;
        parse_dotted_path_from_token(&tok, t)?;
        return Ok(BagMember::Other);
    }
    // A number, which parse_value notes if DynamoDB can't store it, or anything
    // else parse_value reads or refuses.
    Ok(match parse_value(t, depth + 1)? {
        PartiqlValue::Literal(AttributeValue::N(n)) => BagMember::Num(n),
        _ => BagMember::Other,
    })
}

/// Where a bag member stands, as DynamoDB points at it: its first token, a
/// number with its sign, or a nested bag's `<<`.
fn member_span(t: &Tokenizer, start: usize, member: &BagMember) -> Span {
    let first = t.span_at(start);
    let token = |i: usize| t.tokens.get(i).map(String::as_str);
    if matches!(member, BagMember::Num(_)) && matches!(token(start), Some("-" | "+")) {
        let number = t.span_at(start + 1);
        return Span {
            start: first.start,
            len: number.start + number.len - first.start,
        };
    }
    if token(start) == Some("<") {
        return Span {
            start: first.start,
            len: 2,
        };
    }
    first
}

/// After a member of a list, map or bag literal, the next token must separate
/// it from the next member or close the literal.
fn expect_separator(t: &mut Tokenizer, close: &str, missing: &str) -> Result<(), String> {
    match t.peek_token()?.as_deref() {
        Some(",") => Ok(()),
        Some(tok) if tok == close => Ok(()),
        _ => Err(missing.to_string()),
    }
}

/// An Ion literal, read as the token before `t.pos`, refused by its type where
/// it stands; the statement ends there if it doesn't read as Ion (captured
/// eu-west-2 and us-east-1, 2026-10-09). Read as `NULL`, which is never used.
fn ion_literal(t: &mut Tokenizer, tok: &str) -> Result<AttributeValue, String> {
    let kind = ion_type(tok).ok_or_else(String::new)?;
    let path: String = t.value_path.concat();
    let at = t.position(t.span_at(t.pos - 1));
    t.note_literal(format!(
        "Ion Literals are not supported: {kind} under key root{path} at {at}"
    ));
    Ok(AttributeValue::NULL(true))
}

/// The type DynamoDB names when it refuses an Ion literal written between
/// backticks, or `None` when the text doesn't read as Ion. BLOB, CLOB, INT,
/// DECIMAL, STRING, SYMBOL, STRUCT, LIST, TIMESTAMP, NULL and BOOL were
/// captured (eu-west-2 and us-east-1, 2026-10-09); FLOAT and SEXP follow Ion's
/// own type names.
fn ion_type(literal: &str) -> Option<&'static str> {
    let text = literal.strip_prefix('`')?.strip_suffix('`')?.trim();
    if let Some(inner) = text.strip_prefix("{{") {
        let inner = inner.strip_suffix("}}")?.trim();
        if inner.starts_with('"') || inner.starts_with("'''") {
            return Some("CLOB");
        }
        let base64 = inner.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '=') || c.is_ascii_whitespace()
        });
        return base64.then_some("BLOB");
    }
    let wrapped = |open: char, close: char| {
        text.len() >= 2 && text.starts_with(open) && text.ends_with(close)
    };
    if wrapped('{', '}') {
        return Some("STRUCT");
    }
    if wrapped('[', ']') {
        return Some("LIST");
    }
    if wrapped('(', ')') {
        return Some("SEXP");
    }
    if wrapped('"', '"') || (text.len() >= 6 && text.starts_with("'''") && text.ends_with("'''")) {
        return Some("STRING");
    }
    if wrapped('\'', '\'') {
        return Some("SYMBOL");
    }
    match text {
        "null" => return Some("NULL"),
        "true" | "false" => return Some("BOOL"),
        "nan" | "+inf" | "-inf" => return Some("FLOAT"),
        _ => {}
    }
    if let Some(kind) = text.strip_prefix("null.") {
        return kind
            .chars()
            .all(|c| c.is_ascii_alphabetic())
            .then_some("NULL");
    }
    if is_ion_timestamp(text) {
        return Some("TIMESTAMP");
    }
    if let Some(kind) = ion_number(text) {
        return Some(kind);
    }
    let symbol = text.starts_with(|c: char| c.is_ascii_alphabetic() || matches!(c, '_' | '$'))
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '$'));
    symbol.then_some("SYMBOL")
}

/// Whether Ion text is a timestamp: a four-digit year followed by `-` or `T`.
fn is_ion_timestamp(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() >= 5
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && matches!(bytes[4], b'-' | b'T')
        && text
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '-' | ':' | 'T' | 'Z' | '.' | '+'))
}

/// The Ion number type of `text`, if it is one: INT, DECIMAL or FLOAT.
fn ion_number(text: &str) -> Option<&'static str> {
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let digits = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit() || c == '_');
    let radix = [("0x", 16), ("0X", 16), ("0b", 2), ("0B", 2)];
    if let Some((rest, base)) = radix
        .iter()
        .find_map(|(prefix, base)| unsigned.strip_prefix(prefix).map(|rest| (rest, *base)))
    {
        let ok = !rest.is_empty() && rest.chars().all(|c| c == '_' || c.is_digit(base));
        return ok.then_some("INT");
    }
    let (mantissa, exponent, float) = if let Some(at) = unsigned.find(['e', 'E']) {
        (&unsigned[..at], Some(&unsigned[at + 1..]), true)
    } else if let Some(at) = unsigned.find(['d', 'D']) {
        (&unsigned[..at], Some(&unsigned[at + 1..]), false)
    } else {
        (unsigned, None, false)
    };
    if let Some(exponent) = exponent
        && !digits(exponent.strip_prefix(['+', '-']).unwrap_or(exponent))
    {
        return None;
    }
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (mantissa, None),
    };
    if !digits(whole) || fraction.is_some_and(|f| !f.is_empty() && !digits(f)) {
        return None;
    }
    Some(if float {
        "FLOAT"
    } else if fraction.is_some() || exponent.is_some() {
        "DECIMAL"
    } else {
        "INT"
    })
}

/// Parse a `{ 'key': 'value', ... }` item literal into a DynamoDB attribute map.
fn parse_item_literal(
    t: &mut Tokenizer,
    depth: usize,
) -> Result<HashMap<String, AttributeValue>, String> {
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
        t.note_key(&key);

        let colon = t.next_token()?.ok_or("Expected ':'")?;
        if colon != ":" {
            return Err(format!("Expected ':' but got '{colon}'"));
        }

        // Parse value, named from the value it is nested in
        t.value_path.push(format!(".{key}"));
        let val = parse_item_value(t, depth);
        t.value_path.pop();
        item.insert(key, val?);
        expect_separator(t, "}", "Expected RIGHT_CURLY")?;
    }

    Ok(item)
}

/// Parse a value inside an item literal (supports nested maps, lists, set literals, etc.).
fn parse_item_value(t: &mut Tokenizer, depth: usize) -> Result<AttributeValue, String> {
    if depth > MAX_EXPR_DEPTH {
        return Err(TOO_DEEP.to_string());
    }
    t.note_depth();
    let tok = t.peek_token()?.ok_or("Expected value")?;

    if tok == "{" {
        // Nested map
        let inner = parse_item_literal(t, depth + 1)?;
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
            t.value_path.push(format!("[{}]", items.len()));
            let item = parse_item_value(t, depth + 1);
            t.value_path.pop();
            items.push(item?);
            expect_separator(t, "]", "Expected RIGHT_BRACKET")?;
        }
        return Ok(AttributeValue::L(items));
    }

    // An empty bag, which the tokenizer reads as `<`, `<>`, `>`.
    if tok == "<"
        && t.peek_token_at(1)?.as_deref() == Some("<>")
        && t.peek_token_at(2)?.as_deref() == Some(">")
    {
        for _ in 0..3 {
            t.next_token()?;
        }
        t.note_empty_bag();
        return Ok(AttributeValue::SS(Vec::new()));
    }

    // Set literal: << val1, val2 >>
    if tok == "<" && t.peek_token_at(1)?.as_deref() == Some("<") {
        t.next_token()?; // consume first <
        t.next_token()?; // consume second <
        return parse_bag(t, depth);
    }

    // Scalar value
    let tok = t.next_token()?.ok_or("Expected value")?;

    if tok.starts_with('`') {
        return ion_literal(t, &tok);
    }

    // A function call is a value DynamoDB can't store. It names the function
    // when the call is an attribute's whole value and calls it `Call` deeper
    // in, with the path and where it stands (captured eu-west-2, us-east-1,
    // eu-west-1 and us-west-2, 2026-10-09).
    if tok.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && t.peek_token()?.as_deref() == Some("(")
    {
        let index = t.pos - 1;
        let name = if depth == 0 {
            tok.to_lowercase()
        } else {
            "Call".to_string()
        };
        let path: String = t.value_path.concat();
        let at = t.position(t.span_at(index));
        t.note_literal(format!(
            "Unsupported data type: {name} under key root{path} at {at}"
        ));
        let mut open = 0usize;
        loop {
            match t.next_token()?.as_deref() {
                Some("(") => open += 1,
                Some(")") if open == 1 => break,
                Some(")") => open -= 1,
                Some(_) => {}
                None => return Err("Expected ')'".to_string()),
            }
        }
        return Ok(AttributeValue::NULL(true));
    }

    // String
    if tok.starts_with('\'') && tok.ends_with('\'') && tok.len() >= 2 {
        return Ok(AttributeValue::S(tok[1..tok.len() - 1].to_string()));
    }

    // Negative number: `-` followed by a numeric token
    if (tok == "-" || tok == "+")
        && let Some(ref next) = t.peek_token()?
        && is_number_token(next)
    {
        let num = t.next_token()?.unwrap();
        let text = format!("{tok}{num}");
        t.note_number(&text);
        return Ok(AttributeValue::N(text));
    }

    // Number
    if is_number_token(&tok) {
        t.note_number(&tok);
        return Ok(AttributeValue::N(tok));
    }

    match tok.to_uppercase().as_str() {
        "TRUE" => Ok(AttributeValue::BOOL(true)),
        "FALSE" => Ok(AttributeValue::BOOL(false)),
        "NULL" => Ok(AttributeValue::NULL(true)),
        // A name that runs straight into a string leaves the literal around it
        // unclosed, which the caller reports: `X'616263'` is not a binary
        // literal (captured eu-west-2 and us-east-1, 2026-10-09). Read as
        // `NULL`, which is never used.
        _ if tok.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && t.peek_token()?.is_some_and(|next| next.starts_with('\'')) =>
        {
            Ok(AttributeValue::NULL(true))
        }
        _ => Err(format!("Unexpected value in item literal: {tok}")),
    }
}

/// Parse a `{ 'key': value, ... }` item literal where values may be `?` parameter placeholders.
/// Returns `PartiqlValue` wrappers so parameters can be resolved at execution time.
fn parse_item_literal_partiql(
    t: &mut Tokenizer,
    depth: usize,
) -> Result<HashMap<String, PartiqlValue>, String> {
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
        t.note_key(&key);

        let colon = t.next_token()?.ok_or("Expected ':'")?;
        if colon != ":" {
            return Err(format!("Expected ':' but got '{colon}'"));
        }

        // Parse value (may be a parameter placeholder). A top-level attribute's
        // value is its own `root`, so only a nested entry adds its key to the
        // path: `{'m': {'x': 1e126}}` fails `under root.x`.
        let nested = depth > 0;
        if nested {
            t.value_path.push(format!(".{key}"));
        }
        let val = parse_item_value_partiql(t, depth);
        if nested {
            t.value_path.pop();
        }
        item.insert(key, val?);
        expect_separator(t, "}", "Expected RIGHT_CURLY")?;
    }

    Ok(item)
}

/// Parse a value inside an item literal, supporting `?` parameter placeholders
/// and nested maps/lists (which are stored as `PartiqlValue::Literal`).
fn parse_item_value_partiql(t: &mut Tokenizer, depth: usize) -> Result<PartiqlValue, String> {
    if depth > MAX_EXPR_DEPTH {
        return Err(TOO_DEEP.to_string());
    }
    t.note_depth();
    let tok = t.peek_token()?.ok_or("Expected value")?;

    if tok == "?" {
        t.next_token()?; // consume ?
        let idx = t.next_param_index();
        return Ok(PartiqlValue::Parameter(idx));
    }

    // For lists, use parse_value which supports `?` inside list elements
    if tok == "[" {
        return parse_value(t, depth + 1);
    }

    // For nested maps, use recursive partiql parsing to support `?`
    if tok == "{" {
        // Parse nested map with partiql-aware parser
        let inner = parse_item_literal_partiql(t, depth + 1)?;
        // Check if all values are literals: if so, collapse to a single Literal
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
    let av = parse_item_value(t, depth)?;
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
    /// The clause whose values are being read, so a number DynamoDB would
    /// refuse is reported from the clause it reports first.
    clause: LiteralClause,
    /// Where the value being read sits inside the value it belongs to, as the
    /// segments after `root`: `.x` for a map entry, `[1]` for a list element.
    value_path: Vec<String>,
    /// Everything DynamoDB refuses as it reads the statement's terms, in the
    /// order written, with the clause each was written in: a literal it can't
    /// store (a number out of range, a value nested too deep, a bag it can't
    /// hold), a function it doesn't recognise, a type `IS` can't test.
    refusals: Vec<Refusal>,
    /// Whether the WHERE clause holds a call or an operator DynamoDB refuses
    /// for itself. The refusal stays on the node it belongs to, for the
    /// statement's operand check to report in the order written.
    operand_fault: bool,
    /// The first refusal of a `set_add` or `set_delete` call in an UPDATE's
    /// SET clause: reported after the WHERE clause's operands are checked.
    set_refusal: Option<String>,
    /// How deep the reader for a statement with no verb has nested.
    depth: usize,
    /// How many operators the WHERE clause has held so far.
    operators: usize,
    /// How many operators the SET right-hand sides have held so far.
    set_operators: usize,
    /// Whether a WHERE clause's conditions are being read, so its comparison
    /// operators and function calls count against `MAX_OPERATORS` as they
    /// are consumed.
    reading_where: bool,
    /// How many literal values are being read, nested. A literal's tokens
    /// count no operators: a bag's `<<` and `>>` reach the parser as `<` and
    /// `>`, and are brackets, not comparisons.
    reading_literal: usize,
}

/// One refusal DynamoDB makes as it reads a statement's terms.
struct Refusal {
    clause: LiteralClause,
    message: String,
    /// A literal's refusal, which a transaction reports as the statement's own
    /// rather than as a parse failure.
    literal: bool,
}

/// How deep a literal value may nest: a value 32 levels inside the value it is
/// part of is refused.
const LITERAL_NESTING_LIMIT: usize = 32;

/// The clause a literal value was written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiteralClause {
    Where,
    Set,
    Value,
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
            clause: LiteralClause::Where,
            value_path: Vec::new(),
            refusals: Vec::new(),
            operand_fault: false,
            set_refusal: None,
            depth: 0,
            operators: 0,
            set_operators: 0,
            reading_where: false,
            reading_literal: 0,
        })
    }

    /// Note a call, at the token at `index`, to a function DynamoDB doesn't
    /// recognise. It names the function in lower case, as written otherwise,
    /// and where it stands: `Unrecognized function: exists at 1:61:6`.
    fn note_unrecognized_function(&mut self, index: usize) {
        if self.clause_refused() {
            return;
        }
        let name = self.tokens[index].to_lowercase();
        let at = self.position(self.span_at(index));
        self.note_term(format!("Unrecognized function: {name} at {at}"));
    }

    /// Note a refusal of a term as it is read: a function, a subquery or an
    /// `IS` type DynamoDB doesn't support. It is reported in the order written
    /// against the literals DynamoDB refuses, WHERE clause first, and ahead of
    /// any operand refusal (captured eu-west-2 and us-east-1, 2026-10-09).
    fn note_term(&mut self, message: String) {
        if self.clause_refused() {
            return;
        }
        self.refusals.push(Refusal {
            clause: self.clause,
            message,
            literal: false,
        });
    }

    /// Whether the clause being read already holds a refusal. Only the first
    /// in a clause is reported, so a later one isn't built or kept: a
    /// statement full of refused literals under a long path would otherwise
    /// store a message per literal.
    fn clause_refused(&self) -> bool {
        self.refusals.iter().any(|r| r.clause == self.clause)
    }

    /// Whether the statement already holds any refusal, in any clause, so
    /// that it will be refused whatever else is read.
    fn holds_refusal(&self) -> bool {
        !self.refusals.is_empty()
    }

    /// Count an operator in the WHERE clause, refusing the one past
    /// [`MAX_OPERATORS`] in DynamoDB's words once the statement has parsed.
    fn count_operator(&mut self) {
        self.operators += 1;
        if self.operators == MAX_OPERATORS + 1 {
            self.note_term(format!(
                "The expression contains too many operators; operator count: {}",
                self.operators
            ));
        }
    }

    /// Count an operator in a SET right-hand side, refusing the one past
    /// [`MAX_OPERATORS`] in the words a WHERE clause's are refused in. The
    /// update expression is counted on its own, as DynamoDB holds an update
    /// expression and a condition expression to the limit separately.
    fn count_set_operator(&mut self) {
        self.set_operators += 1;
        if self.set_operators == MAX_OPERATORS + 1 {
            self.note_term(format!(
                "The expression contains too many operators; operator count: {}",
                self.set_operators
            ));
        }
    }

    /// Note a literal DynamoDB refuses as it reads it, in the same order.
    fn note_literal(&mut self, message: String) {
        if self.clause_refused() {
            return;
        }
        self.refusals.push(Refusal {
            clause: self.clause,
            message,
            literal: true,
        });
    }

    /// Note an empty bag, `<<>>`, which DynamoDB refuses wherever it is
    /// written, once the statement has parsed (captured eu-west-2, us-east-1,
    /// eu-west-1 and us-west-2, 2026-10-09).
    fn note_empty_bag(&mut self) {
        self.note_literal("Empty bags are not supported".to_string());
    }

    /// Note a set literal once it is read. A number set with a repeated value
    /// is refused naming its members as written and its path, `under root`
    /// being the value it was written as part of; a string set in the older
    /// validation layer's words, with no path (captured eu-west-2, us-east-1,
    /// eu-west-1 and us-west-2, 2026-10-09).
    fn note_set_literal(&mut self, value: &AttributeValue) {
        if self.clause_refused() {
            return;
        }
        let message = match value {
            AttributeValue::NS(members) => {
                let mut seen = std::collections::HashSet::new();
                let repeated = members
                    .iter()
                    .any(|n| !seen.insert(crate::types::normalize_dynamo_number(n)));
                repeated.then(|| {
                    let path: String = self.value_path.concat();
                    format!(
                        "One or more parameter values were invalid: Input collection [{}] \
                         contains duplicates! under root{path}",
                        members.join(", ")
                    )
                })
            }
            AttributeValue::SS(_) => crate::validation::duplicate_set_message(
                value,
                crate::validation::ValueLayer::Older,
            ),
            _ => None,
        };
        if let Some(message) = message {
            self.note_literal(message);
        }
    }

    /// A call or an operator DynamoDB refuses for itself, with the operands
    /// it was written with.
    fn refused(&mut self, operands: Vec<Operand>, message: String) -> Box<Refused> {
        self.operand_fault = true;
        Box::new(Refused { operands, message })
    }

    /// Note a `set_add` or `set_delete` call DynamoDB refuses; the first is
    /// kept.
    fn note_set_refusal(&mut self, message: String) {
        self.set_refusal.get_or_insert(message);
    }

    /// Note a number literal as it is read. One DynamoDB would refuse is
    /// recorded with its path, `root` being the value it was written as part
    /// of, and reported once the statement has parsed.
    fn note_number(&mut self, text: &str) {
        if self.clause_refused() {
            return;
        }
        if let Err(message) = check_literal_number(text) {
            let path: String = self.value_path.concat();
            self.note_literal(format!("{message} under root{path}"));
        }
    }

    /// Note a map key as it is read. DynamoDB refuses an empty one anywhere in
    /// a literal, an INSERT's own attribute names included, with no path, in
    /// the order written against the other literal checks (captured eu-west-2
    /// and us-east-1, 2026-10-09).
    fn note_key(&mut self, key: &str) {
        if key.is_empty() {
            self.note_literal("Path component cannot be an empty string".to_string());
        }
    }

    /// Note a value about to be read. One 32 levels inside the value it
    /// belongs to is as deep as DynamoDB reads a literal: it refuses it there,
    /// naming the path to it, whatever lies further in (captured eu-west-2 and
    /// us-east-1, 2026-10-09: 31 levels are accepted, and 32 and 63 both fail
    /// at the 32nd).
    fn note_depth(&mut self) {
        if self.value_path.len() == LITERAL_NESTING_LIMIT && !self.clause_refused() {
            let path: String = self.value_path.concat();
            self.note_literal(format!(
                "Nesting Levels have exceeded supported limits under root{path}"
            ));
        }
    }

    /// What DynamoDB reports first for a statement that parsed: the first
    /// refusal from the clause it reads first (an UPDATE's WHERE clause comes
    /// before its SET clause) and the first written within a clause (captured
    /// eu-west-2 and us-east-1, 2026-10-09). An operand's refusal is the
    /// statement check's to report, after these.
    fn first_refusal(&self) -> Option<ParseFailure> {
        [
            LiteralClause::Where,
            LiteralClause::Set,
            LiteralClause::Value,
        ]
        .into_iter()
        .find_map(|clause| self.refusals.iter().find(|r| r.clause == clause))
        .map(|r| {
            if r.literal {
                ParseFailure::Literal(r.message.clone())
            } else {
                ParseFailure::Statement(ParseError::Validation(r.message.clone()))
            }
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
        // A comparison operator, a call to a WHERE function, NOT, IN, BETWEEN
        // and IS are operators DynamoDB counts, as AND and OR are, wherever
        // the condition parser reads them: the NOT in `IS NOT` or `NOT IN`
        // counts on its own, and BETWEEN's AND doesn't count (captured
        // eu-west-2 and us-east-1, 2026-10-10). A literal's own tokens are
        // not: ` AND s = <<'x'>>` costs two, as ` AND x = 1` does (captured
        // eu-west-2, 2026-10-10).
        if self.reading_where && self.reading_literal == 0 {
            let calls = self.tokens.get(self.pos).is_some_and(|next| next == "(");
            if is_comparison_operator(&tok)
                || is_any(&tok, &["NOT", "IN", "BETWEEN", "IS"])
                || (calls && WHERE_FUNCTIONS.iter().any(|f| tok.eq_ignore_ascii_case(f)))
            {
                self.count_operator();
            }
        }
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

        // Comments read as whitespace, as DynamoDB reads them: `--` runs to
        // the end of the line and `/* ... */` to its close. An unterminated
        // block comment is refused with nothing after the envelope, and so is
        // `#`, which is not a comment marker here (captured eu-west-2,
        // 2026-10-09). Strings and quoted names are read whole below, so a
        // comment marker inside one stays data.
        if chars[i] == '-' && chars.get(i + 1) == Some(&'-') {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
            match (i + 2..len.saturating_sub(1)).find(|&j| chars[j] == '*' && chars[j + 1] == '/') {
                Some(close) => {
                    i = close + 2;
                    continue;
                }
                None => return Err(String::new()),
            }
        }
        if chars[i] == '#' {
            return Err(String::new());
        }

        let start = i;

        // A number can start at its point, as `.5` and `.25e1` do (captured
        // eu-west-2, 2026-10-09). A `.` straight after a name, a quoted name,
        // `]` or `)` continues a path instead, as in `a.b` or `l[0].x`.
        let leading_point = chars[i] == '.'
            && chars.get(i + 1).is_some_and(|c| c.is_ascii_digit())
            && !(i > 0
                && (chars[i - 1].is_ascii_alphanumeric()
                    || matches!(chars[i - 1], '_' | '"' | ']' | ')')));

        // Single-char tokens
        match chars[i] {
            '.' if leading_point => {}
            '{' | '}' | '[' | ']' | '(' | ')' | ',' | ':' | '*' | '/' | '?' | '+' | '-' | '.'
            | ';' => {
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

        // An Ion literal, between backticks, read whole so it can be refused by
        // type where it stands. One that never closes ends the statement with
        // nothing after the envelope (captured eu-west-2 and us-east-1,
        // 2026-10-09).
        if chars[i] == '`' {
            let Some(close) = (i + 1..len).find(|&j| chars[j] == '`') else {
                return Err(String::new());
            };
            i = close + 1;
            let tok: String = chars[start..i].iter().collect();
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
                // DynamoDB returns the envelope with nothing after it here.
                // The detail is deliberately kept anyway: the exception type and
                // the accept/reject outcome match, and the wording is the half
                // that drifts. AWS's own validation prose differed across two of
                // four regions in the 2026-06 capture, so a caller matching it
                // exactly is already broken; one branching on the type is not.
                // Extra detail is additive and cannot break either.
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
                // Detail kept, for the reason given on the string-literal case.
                return Err("Unterminated quoted identifier".to_string());
            }
            s.push('"');
            i += 1;
            emit(&mut tokens, &mut spans, s, start, i);
            continue;
        }

        // Number, with an optional exponent: `1e5`, `1.5E-3`. An `e` that is
        // not followed by digits (`1e`, `1ex`) is left for the next token.
        if chars[i].is_ascii_digit() || leading_point {
            // One point per number: `1.2.3` reads as `1.2` with `.3` left
            // over, which DynamoDB refuses as a token after the expression
            // (captured eu-west-2, 2026-10-09, with `.5.5`).
            let mut s = String::new();
            let mut seen_point = false;
            while i < len && (chars[i].is_ascii_digit() || (chars[i] == '.' && !seen_point)) {
                seen_point |= chars[i] == '.';
                s.push(chars[i]);
                i += 1;
            }
            if i < len && matches!(chars[i], 'e' | 'E') {
                let sign = usize::from(i + 1 < len && matches!(chars[i + 1], '+' | '-'));
                let first_digit = i + 1 + sign;
                // An exponent marker with no digits and nothing name-like
                // after it, as in `5e` at the end of a statement, is refused
                // with nothing after the envelope (captured eu-west-2,
                // 2026-10-09). One followed by a name (`1ex`) is still left
                // for the next token.
                if first_digit >= len
                    || !(chars[first_digit].is_ascii_alphanumeric() || chars[first_digit] == '_')
                {
                    return Err(String::new());
                }
                if chars[first_digit].is_ascii_digit() {
                    let mut end = first_digit;
                    while end < len && chars[end].is_ascii_digit() {
                        end += 1;
                    }
                    // DynamoDB reads the literal as Java's BigDecimal does,
                    // and refuses one BigDecimal can't hold as a statement
                    // that is not well formed, with nothing after the colon.
                    // That is an exponent outside an i32 (`1e99999999999`,
                    // captured eu-west-2, 2026-10-08) or a scale outside one,
                    // the scale being the digits written after the point
                    // minus the exponent (`1e-2147483648`, `1.50e-2147483646`,
                    // captured 2026-10-09). A literal that can be held goes on
                    // to the usual number checks wherever the value is used.
                    let exponent: String = chars[i + 1..end].iter().collect();
                    let Ok(exponent) = exponent.parse::<i32>() else {
                        return Err(String::new());
                    };
                    let fraction_digits =
                        s.split_once('.').map_or(0, |(_, fraction)| fraction.len());
                    let scale = i64::try_from(fraction_digits)
                        .unwrap_or(i64::MAX)
                        .saturating_sub(i64::from(exponent));
                    if i32::try_from(scale).is_err() {
                        return Err(String::new());
                    }
                    s.extend(&chars[i..end]);
                    i = end;
                }
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

        // Unknown character: report an error rather than silently skipping.
        // DynamoDB says nothing after "can't be processed: " for a character
        // outside ASCII, the no-break space included (captured eu-west-2 and
        // us-east-1, 2026-10-09); an unexpected ASCII character wasn't captured.
        if !chars[i].is_ascii() {
            return Err(String::new());
        }
        return Err(format!("Unexpected character: '{}'", chars[i]));
    }

    Ok((tokens, spans))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A long run of NOTs is read in a loop, so it nests nothing: the WHERE
    /// clause's operator limit refuses it, however long the run.
    #[test]
    fn a_long_run_of_nots_is_refused_by_the_operator_limit() {
        let statement = format!(r#"SELECT * FROM "t" WHERE {}pk='1'"#, "NOT ".repeat(5_000));
        match parse(&statement) {
            Err(ParseError::Validation(message)) => assert_eq!(
                message,
                "The expression contains too many operators; operator count: 301"
            ),
            other => panic!("expected the operator limit, got {other:?}"),
        }
    }

    /// Runs of NOT between brackets mustn't stack into one chain deeper than
    /// the bracket limit: each run is read in a loop, and only its parity
    /// matters, so the tree that reaches De Morgan stays shallow. Through the
    /// uncapped parser this shape once built a chain about 19,500 deep, which
    /// a recursive walk couldn't survive on a 2 MB stack.
    #[test]
    fn runs_of_not_between_brackets_stay_shallow() {
        let run = "NOT ".repeat(MAX_OPERATORS + 1);
        let statement = format!(
            r#"SELECT * FROM "t" WHERE {}{}a=1{}"#,
            format!("{run}(").repeat(MAX_EXPR_DEPTH),
            run,
            ")".repeat(MAX_EXPR_DEPTH)
        );
        let outcome = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || parse(&statement).map(|_| ()).map_err(|e| e.to_string()))
            .unwrap()
            .join()
            .expect("the parser ran out of stack");
        let err = outcome.expect_err("past the operator limit");
        assert!(err.contains("too many operators"), "got {err}");
    }

    /// A run of `-` on the right of SET is read in a loop, so however long it
    /// is it costs no stack: on a 2 MB stack, 100,000 of them before a path
    /// is the one-operand refusal of `-`, not an overflow. Read by recursion,
    /// a run of this length overflows that stack.
    #[test]
    fn a_long_run_of_minuses_in_set_is_refused_without_overflowing() {
        let statement = format!(
            r#"UPDATE "t" SET z = {}n WHERE pk = 'a'"#,
            "- ".repeat(100_000)
        );
        let outcome = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || parse(&statement).map(|_| ()).map_err(|e| e.to_string()))
            .unwrap()
            .join()
            .expect("the parser ran out of stack");
        let err = outcome.expect_err("a lone minus is refused");
        assert!(
            err.contains("operator or function: -, number of operands: 1"),
            "got {err}"
        );
    }

    /// Calls to `if_not_exists` nest to the expression depth limit on a 2 MB
    /// stack: 64 parse, and a 65th is refused as too deep, not an overflow.
    #[test]
    fn nested_if_not_exists_calls_parse_to_the_depth_limit() {
        let nested = |calls: usize| {
            format!(
                r#"UPDATE "t" SET z = {}1{} WHERE pk = 'a'"#,
                "if_not_exists(q, ".repeat(calls),
                ")".repeat(calls)
            )
        };
        let outcome = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let parsed = |calls| parse(&nested(calls)).map(|_| ()).map_err(|e| e.to_string());
                (parsed(MAX_EXPR_DEPTH), parsed(MAX_EXPR_DEPTH + 1))
            })
            .unwrap()
            .join()
            .expect("the parser ran out of stack");
        assert_eq!(outcome.0, Ok(()));
        let err = outcome.1.expect_err("one more call is too deep");
        assert!(err.contains(TOO_DEEP), "got {err}");
    }

    /// The `+` and `-` of a SET sum count towards the operator limit, as a
    /// WHERE clause's operators do: 300 run and the 301st is refused.
    #[test]
    fn a_set_sum_is_held_to_the_operator_limit() {
        let sum = |operators: usize| {
            format!(
                r#"UPDATE "t" SET z = 1{} WHERE pk = 'a'"#,
                " + 1".repeat(operators)
            )
        };
        assert!(parse(&sum(MAX_OPERATORS)).is_ok());
        match parse(&sum(MAX_OPERATORS + 1)) {
            Err(ParseError::Validation(message)) => assert_eq!(
                message,
                "The expression contains too many operators; operator count: 301"
            ),
            other => panic!("expected the operator limit, got {other:?}"),
        }
    }

    /// A `begins_with` or `contains` on operands other than a path and a
    /// value holds its literals' bytes against the clause's byte budget, as
    /// the other conditions do.
    #[test]
    fn a_search_on_operands_counts_against_the_byte_budget() {
        // `s` is one byte of path; the literal is the rest.
        let search = |bytes: usize| {
            format!(
                r#"SELECT * FROM "t" WHERE begins_with('{}', s)"#,
                "x".repeat(bytes)
            )
        };
        assert!(parse(&search(MAX_CLAUSE_BYTES - 1)).is_ok());
        let err = parse(&search(MAX_CLAUSE_BYTES)).unwrap_err();
        assert!(err.to_string().contains("too complex"), "got {err}");
    }

    /// The operators a WHERE clause is charged, read the way a statement's
    /// clause is.
    fn operators_in(clause: &str) -> usize {
        let mut t = Tokenizer::new(clause).unwrap();
        let _ = parse_conditions_with_or(&mut t);
        t.operators
    }

    /// A bag literal's `<<` and `>>` are brackets, not comparisons, so a bag
    /// counts nothing toward the operator limit wherever it's written:
    /// ` AND s = <<'x'>>` costs its AND and its `=` (captured eu-west-2,
    /// 2026-10-10), and a bag handed to a condition function costs nothing
    /// beyond the call. The function's operand is read once, so its bag isn't
    /// charged twice either.
    ///
    /// Only the first two rows are captured. The function row follows from
    /// the captured function-call count, and the IN row from the captured IN
    /// count: the IN costs one and its bag members nothing.
    #[test]
    fn a_bag_literal_counts_no_operators() {
        assert_eq!(operators_in("pk = 'a' AND s = <<'x'>>"), 3);
        assert_eq!(operators_in("pk = 'a' AND s = <<'x', 'y'>>"), 3);
        assert_eq!(operators_in("pk = 'a' AND attribute_exists(<<'x'>>)"), 3);
        assert_eq!(operators_in("pk = 'a' AND s IN [<<'x'>>, <<'y'>>]"), 3);
    }

    /// A NOT run keeps its parity: an odd run negates, an even one doesn't.
    #[test]
    fn a_run_of_nots_keeps_its_parity() {
        for (nots, negated) in [(1, true), (2, false), (3, true), (4, false)] {
            let statement = format!(r#"SELECT * FROM "t" WHERE {}a=1"#, "NOT ".repeat(nots));
            let Statement::Select {
                where_clause: Some(wc),
                ..
            } = parse(&statement).unwrap()
            else {
                panic!("expected a SELECT with WHERE");
            };
            let is_negated = match &*wc.groups[0][0] {
                WhereCondition::Comparison(_) => false,
                WhereCondition::NotComparison(_) => true,
                other => panic!("{nots} NOTs: {other:?}"),
            };
            assert_eq!(is_negated, negated, "{nots} NOTs");
        }
    }

    /// A negated `IN` is one group as wide as the list under De Morgan, and
    /// each negated conjunction after it clones that whole group once per
    /// alternative; capping groups alone left that work unbounded. The
    /// statement limit keeps such a clause out of the parser now, so this
    /// guards the cap behind it.
    #[test]
    fn a_clause_too_wide_to_flatten_keeps_its_tree_without_grinding() {
        let values = (0..MAX_IN_OPERANDS)
            .map(|i| format!("'v{i}'"))
            .collect::<Vec<_>>()
            .join(",");
        let mut clause = format!("NOT a IN ({values})");
        for i in 0..8 {
            clause.push_str(&format!(" AND NOT (b='p{i}' AND b='q{i}')"));
        }
        let start = std::time::Instant::now();
        let parsed = parse(&format!(r#"SELECT * FROM "t" WHERE {clause}"#)).unwrap();
        let Statement::Select {
            where_clause: Some(wc),
            ..
        } = parsed
        else {
            panic!("SELECT")
        };
        assert!(!wc.fully_expanded);
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }

    /// A negated `IN` past 100 operands is refused for its operands in
    /// DynamoDB's words, under a long path or a short one (captured eu-west-2,
    /// 2026-10-10).
    #[test]
    fn a_negated_in_past_100_operands_is_refused_for_them() {
        for path in ["a".repeat(4_000), "a".to_string()] {
            let values = vec!["1"; 1_900].join(",");
            let statement =
                format!(r#"SELECT * FROM "t" WHERE NOT {path} IN [{values}] AND (x = 1 OR x = 2)"#);
            assert!(statement.chars().count() <= 8_192, "{}", statement.len());
            let err = parse(&statement).unwrap_err().to_string();
            assert!(
                err.contains(
                    "The IN operator is provided with too many operands; number of operands: 1900"
                ),
                "got {err}"
            );
        }
    }

    /// A negated `IN` of 100 operands is an ordinary clause, under a long
    /// path too: De Morgan's copies of the path are within the budget.
    #[test]
    fn a_negated_in_of_100_operands_is_evaluated() {
        for path in ["a".repeat(4_000), "a".to_string()] {
            let values = vec!["1"; MAX_IN_OPERANDS].join(",");
            let statement =
                format!(r#"SELECT * FROM "t" WHERE NOT {path} IN [{values}] AND (x = 1 OR x = 2)"#);
            assert!(parse(&statement).is_ok(), "{} path", path.len());
        }
    }

    /// The parser itself takes statements of any length, so a negated `IN`
    /// whose path, copied once per operand, would hold more than the budget
    /// is refused before De Morgan copies it.
    #[test]
    fn a_negated_in_whose_copies_would_pass_the_budget_is_too_complex() {
        let path = "a".repeat(MAX_CLAUSE_BYTES / MAX_IN_OPERANDS + 1);
        let values = vec!["1"; MAX_IN_OPERANDS].join(",");
        let err = parse(&format!(
            r#"SELECT * FROM "t" WHERE NOT {path} IN [{values}]"#
        ))
        .unwrap_err();
        assert!(err.to_string().contains("too complex"), "got {err}");
    }

    /// A wide alternation is refused for its operators, as DynamoDB refuses
    /// it, rather than flattened first.
    #[test]
    fn a_wide_alternation_is_refused_without_flattening_it() {
        let clause = (0..40_000)
            .map(|i| format!("a='v{i}'"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let start = std::time::Instant::now();
        let err = parse(&format!(r#"SELECT * FROM "t" WHERE {clause}"#)).unwrap_err();
        assert!(err.to_string().contains("too many operators"), "got {err}");
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }

    /// A condition leaf for the flattening tests.
    fn leaf(name: &str) -> ConditionTree {
        ConditionTree::Leaf(Arc::new(WhereCondition::Exists(name.to_string())))
    }

    /// Flattening refuses a product, an alternation or a conjunction past its
    /// caps before it builds the result, and accepts one exactly at them.
    #[test]
    fn flattening_refuses_past_its_caps_before_building() {
        let pair = || ConditionTree::Or(vec![leaf("a"), leaf("b")]);
        // 2^8 = 256 groups is the cap; 2^9 is past it.
        assert_eq!(
            to_dnf(&ConditionTree::And(vec![pair(); 8])).unwrap().len(),
            256
        );
        assert_eq!(
            to_dnf(&ConditionTree::And(vec![pair(); 9])).unwrap_err(),
            TOO_COMPLEX
        );
        let alternation = |n| ConditionTree::Or((0..n).map(|_| leaf("a")).collect());
        assert_eq!(
            to_dnf(&alternation(MAX_OR_GROUPS)).unwrap().len(),
            MAX_OR_GROUPS
        );
        assert_eq!(
            to_dnf(&alternation(MAX_OR_GROUPS + 1)).unwrap_err(),
            TOO_COMPLEX
        );
        let conjunction = |n| ConditionTree::And((0..n).map(|_| leaf("a")).collect());
        assert_eq!(
            slots(&to_dnf(&conjunction(MAX_TOTAL_CONDITIONS)).unwrap()),
            MAX_TOTAL_CONDITIONS
        );
        assert_eq!(
            to_dnf(&conjunction(MAX_TOTAL_CONDITIONS + 1)).unwrap_err(),
            TOO_COMPLEX
        );
        // Four pairs hold 16 groups of 4 slots; joining a 300-wide conjunction
        // would make 64 + 300 x 16 = 4,864 slots, past the cap at that step.
        let wide = ConditionTree::And((0..300).map(|_| leaf("w")).collect());
        let mut terms = vec![pair(); 4];
        terms.push(wide);
        terms.push(pair());
        assert_eq!(to_dnf(&ConditionTree::And(terms)).unwrap_err(), TOO_COMPLEX);
    }

    /// An alternation's arms are each held to the per-product slot budget, and
    /// side by side they are held to four times it: four arms of 4,096
    /// conditions are accepted, one leaf more is refused.
    #[test]
    fn an_alternation_is_held_to_four_times_the_slot_budget() {
        let conjunction = |n| ConditionTree::And((0..n).map(|_| leaf("a")).collect());
        let mut arms = vec![conjunction(MAX_TOTAL_CONDITIONS); 4];
        assert_eq!(
            slots(&to_dnf(&ConditionTree::Or(arms.clone())).unwrap()),
            4 * MAX_TOTAL_CONDITIONS
        );
        arms.push(leaf("b"));
        assert_eq!(to_dnf(&ConditionTree::Or(arms)).unwrap_err(), TOO_COMPLEX);
    }

    /// Flattening shares conditions: a group repeats the same condition, not a
    /// copy of it.
    #[test]
    fn flattening_shares_each_condition_across_groups() {
        let shared = leaf("s");
        let pair = ConditionTree::Or(vec![leaf("a"), leaf("b")]);
        let groups = to_dnf(&ConditionTree::And(vec![pair, shared])).unwrap();
        assert_eq!(groups.len(), 2);
        assert!(Arc::ptr_eq(&groups[0][1], &groups[1][1]));
    }

    /// The byte budget counts each condition once, however many groups repeat
    /// it, and refuses a clause one byte past it (the parser itself has no
    /// length limit, so the boundary can be reached directly).
    #[test]
    fn the_byte_budget_counts_each_condition_once() {
        let repeated = format!(
            r#"SELECT * FROM "t" WHERE pk = 'k' AND {} AND s = '{}'"#,
            ["(a = 1 OR a = 2)"; 8].join(" AND "),
            "x".repeat(4_070)
        );
        let Statement::Select {
            where_clause: Some(wc),
            ..
        } = parse(&repeated).unwrap()
        else {
            panic!("expected a SELECT with WHERE");
        };
        let per_copy: usize = wc.groups.iter().flatten().map(|c| condition_bytes(c)).sum();
        let held = wc.tree.bytes();
        assert!(per_copy > 1024 * 1024, "{per_copy}");
        assert!(held < 8_192, "{held}");

        // `s` is one byte of path; the literal is the rest.
        let at_budget = format!(
            r#"SELECT * FROM "t" WHERE s = '{}'"#,
            "x".repeat(MAX_CLAUSE_BYTES - 1)
        );
        assert!(parse(&at_budget).is_ok());
        let over = format!(
            r#"SELECT * FROM "t" WHERE s = '{}'"#,
            "x".repeat(MAX_CLAUSE_BYTES)
        );
        let err = parse(&over).unwrap_err();
        assert!(err.to_string().contains("too complex"), "got {err}");
    }

    /// Only the first refusal in a clause is ever reported, so later ones in
    /// the same clause aren't built or kept, however many a statement holds
    /// or however long the path to them.
    #[test]
    fn only_the_first_refusal_in_a_clause_is_kept() {
        let mut t = Tokenizer::new("SELECT * FROM \"t\"").unwrap();
        t.clause = LiteralClause::Value;
        t.value_path = vec![format!(".{}", "k".repeat(10_000)); 31];
        for _ in 0..1_000 {
            t.note_number("1e999999999");
            t.note_key("");
            t.note_depth();
            t.note_empty_bag();
        }
        assert_eq!(t.refusals.len(), 1);
        assert!(
            t.refusals[0].message.contains("under root.kkk"),
            "{}",
            &t.refusals[0].message[..80]
        );

        t.clause = LiteralClause::Where;
        t.note_key("");
        assert_eq!(t.refusals.len(), 2);
        assert_eq!(
            t.refusals[1].message,
            "Path component cannot be an empty string"
        );
    }

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
                match &*wc.groups[0][0] {
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
            } => match &*wc.groups[0][0] {
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
            } => match &*wc.groups[0][0] {
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
                match &*wc.groups[0][0] {
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
                assert!(matches!(&*wc.groups[0][0], WhereCondition::Between(..)));
                assert!(matches!(&*wc.groups[0][1], WhereCondition::Comparison(..)));
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
                match &*wc.groups[0][0] {
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
                match &*wc.groups[0][0] {
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
                match &*wc.groups[0][0] {
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
                match &*wc.groups[0][0] {
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
            parse("UPDATE \"T\" SET name = 'Bob' REMOVE age REMOVE email WHERE pk = 'k1'").unwrap();
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
                assert_eq!(
                    set_clauses[0].value,
                    SetValue::Sum(vec![
                        SumTerm {
                            sign: SetSign::Plus,
                            named: SetSign::Plus,
                            term: SetTerm::Path("count".to_string()),
                        },
                        SumTerm {
                            sign: SetSign::Plus,
                            named: SetSign::Plus,
                            term: SetTerm::Value(PartiqlValue::Literal(AttributeValue::N(
                                "1".to_string()
                            ))),
                        },
                    ])
                );
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
        // A quoted "COUNT" identifier and a 'count(' string literal are fine.
        assert_eq!(
            count_projection_rejection("SELECT \"COUNT\" FROM \"T\""),
            None
        );
        assert_eq!(
            count_projection_rejection("SELECT 'count(' FROM \"T\""),
            None
        );
        // Any call is refused, at its own name's length.
        assert_eq!(
            count_projection_rejection("SELECT recount(x) FROM \"T\""),
            Some("Unexpected path component at 1:8:7".to_string())
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
            } => match &*wc.groups[0][0] {
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

    #[test]
    fn a_number_with_an_exponent_is_one_token() {
        for input in ["1e5", "1E5", "1e+5", "1e-5", "1.5e3", "15E-1"] {
            let (tokens, _) = tokenize(input).unwrap();
            assert_eq!(tokens, vec![input.to_string()], "{input}");
        }
    }

    #[test]
    fn an_e_without_exponent_digits_is_not_part_of_the_number() {
        for (input, expected) in [
            ("1ex", vec!["1", "ex"]),
            ("1 e5", vec!["1", "e5"]),
            ("e5", vec!["e5"]),
        ] {
            let (tokens, _) = tokenize(input).unwrap();
            assert_eq!(tokens, expected, "{input}");
        }
        // With nothing name-like after it, the marker is refused with nothing
        // after the envelope, as DynamoDB refuses `5e` (captured eu-west-2,
        // 2026-10-09).
        for input in ["1e", "1e+", "1e )"] {
            assert_eq!(tokenize(input).unwrap_err(), String::new(), "{input}");
        }
    }

    #[test]
    fn an_exponent_too_large_to_read_is_a_syntax_error_with_no_detail() {
        for input in ["1e99999999999", "1e-99999999999", "2e2147483648"] {
            assert_eq!(
                parse(&format!("SELECT * FROM \"T\" WHERE pk = {input}")).unwrap_err(),
                ParseError::Syntax(String::new()),
                "{input}"
            );
        }
        // At the edge of what can be read, the literal reads, and is refused as
        // a number DynamoDB can't store rather than as a syntax error.
        assert!(matches!(
            parse("SELECT * FROM \"T\" WHERE pk = 1e2147483647"),
            Err(ParseError::Validation(message)) if message.starts_with("Number underflow")
        ));
    }

    #[test]
    fn a_token_left_after_a_statement_is_a_syntax_error() {
        for input in [
            "SELECT * FROM \"T\" WHERE pk = 1 foo",
            "DELETE FROM \"T\" WHERE pk = 1 foo",
            "UPDATE \"T\" SET a = 1 WHERE pk = 1 foo",
            "INSERT INTO \"T\" VALUE {'pk': 1} foo",
        ] {
            assert_eq!(
                parse(input).unwrap_err(),
                ParseError::Syntax("Unexpected token after expression".to_string()),
                "{input}"
            );
        }
    }

    #[test]
    fn an_order_by_clause_on_a_select_is_accepted() {
        for input in [
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY sk",
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY sk DESC",
            "SELECT * FROM \"T\" WHERE pk = 1 order by sk asc",
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY sk DESC, other ASC",
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY a.b[0]",
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY \"sk\"",
            "SELECT * FROM \"T\" ORDER BY sk",
        ] {
            assert!(parse(input).is_ok(), "{input}");
        }
        assert!(parse("DELETE FROM \"T\" WHERE pk = 1 ORDER BY sk").is_err());
    }

    fn order_by_of(input: &str) -> Vec<(String, bool)> {
        match parse(input).unwrap() {
            Statement::Select { order_by, .. } => order_by
                .into_iter()
                .map(|term| (term.path, term.descending))
                .collect(),
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    #[test]
    fn order_by_terms_are_carried_on_the_select() {
        // The executor turns a keyed read round on `ORDER BY sk DESC`, so the
        // terms have to survive the parse with their direction.
        assert_eq!(
            order_by_of("SELECT * FROM \"T\" WHERE pk = 1 ORDER BY sk DESC, other"),
            vec![("sk".to_string(), true), ("other".to_string(), false)]
        );
        assert_eq!(
            order_by_of("SELECT * FROM \"T\" WHERE pk = 1 order by \"sk\" asc"),
            vec![("sk".to_string(), false)]
        );
        assert!(order_by_of("SELECT * FROM \"T\" WHERE pk = 1").is_empty());
    }

    fn where_value_of(input: &str) -> PartiqlValue {
        match parse(input).unwrap() {
            Statement::Select {
                where_clause: Some(wc),
                ..
            } => match &*wc.groups[0][0] {
                WhereCondition::Comparison(c) => c.value.clone(),
                other => panic!("expected a comparison, got {other:?}"),
            },
            other => panic!("expected a SELECT with a WHERE, got {other:?}"),
        }
    }

    #[test]
    fn a_point_starts_a_number_only_where_a_value_can_start() {
        for (input, expected) in [
            ("SELECT * FROM \"T\" WHERE pk = .5e1", ".5e1"),
            ("SELECT * FROM \"T\" WHERE pk=.5", ".5"),
            ("SELECT * FROM \"T\" WHERE pk = -.5e1", "-.5e1"),
        ] {
            assert_eq!(
                where_value_of(input),
                PartiqlValue::Literal(AttributeValue::N(expected.to_string())),
                "{input}"
            );
        }
        // Straight after a name, a quoted name or a closing bracket the point
        // still continues a path.
        match parse("SELECT a.b, \"c\".d, l[0].e FROM \"T\"").unwrap() {
            Statement::Select { projections, .. } => {
                assert_eq!(projections, vec!["a.b", "c.d", "l[0].e"]);
            }
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    #[test]
    fn an_order_by_clause_must_be_whole_and_last() {
        // A WHERE written after ORDER BY used to ride through the exemption
        // for the ORDER BY tail and run as an unfiltered read.
        for input in [
            "SELECT * FROM \"T\" ORDER BY sk WHERE pk = 'x'",
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY sk DESC junk",
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY sk ASC DESC",
        ] {
            assert_eq!(
                parse(input).unwrap_err(),
                ParseError::Syntax("Unexpected token after expression".to_string()),
                "{input}"
            );
        }
        for input in [
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY",
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY sk,",
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER sk",
            "SELECT * FROM \"T\" WHERE pk = 1 ORDER BY DESC",
        ] {
            assert!(
                matches!(parse(input), Err(ParseError::Syntax(_))),
                "{input}"
            );
        }
    }

    #[test]
    fn comments_are_read_as_whitespace() {
        // DynamoDB accepts `--` to the end of the line and `/* ... */`
        // anywhere whitespace can go (captured eu-west-2, 2026-10-09).
        let plain = format!("{:?}", parse("SELECT * FROM \"T\" WHERE pk = 1").unwrap());
        for input in [
            "SELECT * FROM \"T\" WHERE pk = 1 -- note",
            "SELECT * FROM \"T\" WHERE pk = 1 --",
            "SELECT * FROM \"T\" -- c\nWHERE pk = 1",
            "-- lead\nSELECT * FROM \"T\" WHERE pk = 1",
            "SELECT * FROM \"T\" WHERE pk = 1 /* c */",
            "SELECT * FROM \"T\" /* c */ WHERE pk = 1",
            "SELECT/**/* FROM \"T\" WHERE pk = 1",
        ] {
            let parsed = parse(input).unwrap_or_else(|e| panic!("{input:?}: {e:?}"));
            assert_eq!(format!("{parsed:?}"), plain, "{input:?}");
        }
        // Inside a string or a quoted name the characters are data.
        let (tokens, _) = tokenize("'a -- b' \"c /* d */\"").unwrap();
        assert_eq!(tokens, vec!["'a -- b'", "\"c /* d */\""]);
    }

    #[test]
    fn a_hash_or_an_unterminated_block_comment_is_a_syntax_error_with_no_detail() {
        for input in [
            "SELECT * FROM \"T\" WHERE pk = 1 # c",
            "SELECT * FROM \"T\" WHERE pk = 1 /* unterminated",
            "SELECT * FROM \"T\" WHERE pk = 1 /*/",
        ] {
            assert_eq!(
                parse(input).unwrap_err(),
                ParseError::Syntax(String::new()),
                "{input:?}"
            );
        }
    }

    #[test]
    fn an_exponent_whose_scale_does_not_fit_an_i32_is_a_syntax_error_with_no_detail() {
        // DynamoDB reads a number as Java's BigDecimal does, which refuses a
        // scale (digits after the point, as written, minus the exponent)
        // outside an i32 as well as an exponent outside one (captured
        // eu-west-2, 2026-10-09).
        for input in [
            "1e-2147483648",
            "1.5e-2147483647",
            "0.1e-2147483647",
            "1.50e-2147483646",
            "0e-2147483648",
        ] {
            assert_eq!(
                parse(&format!("SELECT * FROM \"T\" WHERE pk = {input}")).unwrap_err(),
                ParseError::Syntax(String::new()),
                "{input}"
            );
        }
        // A scale of exactly i32::MAX still reads.
        for input in ["1.5e-2147483646", "10e-2147483647"] {
            let (tokens, _) = tokenize(input).unwrap();
            assert_eq!(tokens, vec![input.to_string()], "{input}");
        }
    }

    #[test]
    fn an_alias_after_the_table_name_is_refused() {
        // DynamoDB refuses an alias on SELECT, DELETE and UPDATE with its own
        // message and no envelope, and an alias on INSERT as a leftover token
        // (captured eu-west-2, 2026-10-09).
        for input in [
            "SELECT * FROM \"T\" t WHERE pk = 1",
            "SELECT * FROM \"T\" AS t WHERE pk = 1",
            "SELECT * FROM \"T\" t",
            "SELECT * FROM \"T\" foo",
            "SELECT * FROM \"T\" as t",
            "SELECT t.pk FROM \"T\" t WHERE t.pk = 1",
            "DELETE FROM \"T\" t WHERE pk = 5",
            "UPDATE \"T\" t SET a = 3 WHERE pk = 1",
        ] {
            assert_eq!(
                parse(input).unwrap_err(),
                ParseError::Validation("Aliasing is not supported".to_string()),
                "{input}"
            );
        }
        assert_eq!(
            parse("INSERT INTO \"T\" t VALUE {'pk': 8}").unwrap_err(),
            ParseError::Syntax("Unexpected token after expression".to_string())
        );
        // The keywords that follow a table name are not aliases.
        for input in [
            "SELECT * FROM \"T\" WHERE pk = 1",
            "SELECT * FROM \"T\" ORDER BY pk",
            "DELETE FROM \"T\" WHERE pk = 5",
            "UPDATE \"T\" SET a = 3 WHERE pk = 1",
            "UPDATE \"T\" REMOVE a WHERE pk = 1",
        ] {
            assert!(parse(input).is_ok(), "{input}");
        }
    }

    /// The parser itself holds nested `size(` calls to its depth limit, so a
    /// caller that skips the request's length limit can't overflow the stack.
    #[test]
    fn the_parser_refuses_nested_size_calls_without_the_length_limit() {
        let statement = format!(
            r#"SELECT * FROM "Tbl" WHERE {}a{} = 1"#,
            "size(".repeat(10_000),
            ")".repeat(10_000)
        );
        assert!(parse(&statement).is_err());
    }

    /// A call or an operator DynamoDB refuses for itself no longer stops the
    /// statement parsing: the refusal stays on the node it belongs to, with
    /// the operands it was written with, for the statement check to report
    /// in the order written.
    #[test]
    fn a_refused_call_is_kept_where_it_was_written() {
        let parsed = parse_statement(
            r#"SELECT * FROM "t" WHERE n BETWEEN size(true) AND attribute_exists(n, n)"#,
        )
        .ok()
        .expect("parses");
        let Statement::Select {
            where_clause: Some(wc),
            ..
        } = parsed.statement
        else {
            panic!("SELECT with a WHERE clause")
        };
        let WrittenClause::Leaf(condition) = &wc.written else {
            panic!("one condition: {:?}", wc.written)
        };
        let WhereCondition::CompareBetween(Operand::Path(path), low, high) = condition.as_ref()
        else {
            panic!("BETWEEN on a path: {condition:?}")
        };
        assert_eq!(path, "n");
        assert_eq!(
            low,
            &Operand::Size(Box::new(Operand::Value(PartiqlValue::Literal(
                AttributeValue::BOOL(true)
            ))))
        );
        let Operand::Condition(call) = high else {
            panic!("a call as the upper bound: {high:?}")
        };
        let WhereCondition::Refused(refused) = call.as_ref() else {
            panic!("a refused call: {call:?}")
        };
        assert_eq!(
            refused.operands,
            vec![Operand::Path("n".into()), Operand::Path("n".into())]
        );
        assert_eq!(
            refused.message,
            "Incorrect number of operands for operator or function; operator or function: \
             attribute_exists, number of operands: 2"
        );
    }

    #[test]
    fn a_statement_whose_only_fault_is_an_operand_parses() {
        for clause in [
            "n = n",
            "attribute_exists(n, n)",
            "size(s, s) > 0",
            "s IN [s]",
            "size(s) + 1 > 0",
            "size(s) AND n = 1",
        ] {
            assert!(
                parse_statement(&format!(r#"SELECT * FROM "t" WHERE {clause}"#)).is_ok(),
                "{clause}"
            );
        }
    }
}
