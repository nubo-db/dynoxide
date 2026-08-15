//! PartiQL statement executor.
//!
//! Maps parsed PartiQL statements to internal DynamoDB operations.

use crate::actions::index_capacity::WriteCapacity;
use crate::errors::{DynoxideError, Result};
use crate::expressions::condition::compare_values;
use crate::expressions::key_condition::{ResolvedSortKeyCondition, sk_conditions_to_sql};
use crate::partiql::parser::{
    CompOp, PartiqlValue, ReturningVariant, SetValue, Statement, WhereClause, WhereCondition,
};
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item};
use std::collections::HashMap;

/// Execute a parsed PartiQL statement.
///
/// Returns `Some(items)` for SELECT (may be empty) and for a DELETE or UPDATE
/// carrying a `RETURNING` clause (the deleted item or the requested projection);
/// `None` for a write with no `RETURNING` clause. An optional `limit` bounds how
/// many rows a SELECT evaluates (reads), not how many it returns, matching
/// DynamoDB's `Limit` and the Query/Scan semantics.
pub async fn execute<S: StorageBackend>(
    storage: &S,
    stmt: &Statement,
    parameters: &[AttributeValue],
    limit: Option<usize>,
) -> Result<Option<Vec<Item>>> {
    Ok(execute_measured(storage, stmt, parameters, limit).await?.0)
}

/// Like [`execute`], but also returns the total item byte size the statement
/// touched. SELECT reports the summed size of the rows returned;
/// INSERT/UPDATE/DELETE report the affected item's size (0 when the statement
/// was a no-op, e.g. a missing DELETE target).
///
/// This size alone no longer describes what the statement costs. A write is
/// charged on the larger of the images either side of it plus a per-index arm,
/// which [`execute_page`] carries on `StatementPage::capacity`. Prefer that for
/// capacity; this remains for callers that only want the byte count.
pub async fn execute_measured<S: StorageBackend>(
    storage: &S,
    stmt: &Statement,
    parameters: &[AttributeValue],
    limit: Option<usize>,
) -> Result<(Option<Vec<Item>>, usize)> {
    let page = execute_page(storage, stmt, parameters, limit, None, false).await?;
    Ok((page.items, page.size))
}

/// One page of a statement's result.
///
/// Only a `SELECT` paginates. Every other statement returns the whole result
/// and no continuation token.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct StatementPage {
    /// The rows this page carries, with the same `Some`/`None` meaning as
    /// [`execute`].
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
    /// Base table items an LSI reach-back read to serve attributes the index
    /// does not project. Charged to the table arm, which is why an index read
    /// can report a non-zero one.
    pub base_reads: usize,
}

/// The index a `SELECT` was served from.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ReadIndex {
    pub name: String,
    /// LSIs and GSIs land on different arms of `ConsumedCapacity`.
    pub is_lsi: bool,
}

/// Like [`execute_measured`], but resumable.
///
/// `next_token` continues a previous page; the returned token, when present,
/// continues this one. A statement without a `Limit` returns every matching row
/// and no token.
/// The WHERE clause a statement carries, if any. `INSERT` never has one.
fn statement_where_clause(stmt: &Statement) -> Option<&WhereClause> {
    match stmt {
        Statement::Select { where_clause, .. }
        | Statement::Update { where_clause, .. }
        | Statement::Delete { where_clause, .. } => where_clause.as_ref(),
        Statement::Insert { .. } => None,
    }
}

/// Reject an ordering comparison whose operand is a type that has no ordering.
///
/// DynamoDB orders `S`, `N` and `B` and nothing else, and it rejects the
/// statement outright rather than declining to match: the check fires before the
/// table is resolved, so a statement naming a table that does not exist still
/// reports this. `=` and `<>` are unaffected, being defined for every type.
/// Captured eu-west-2 2026-08-15.
fn validate_ordering_operands(
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

    let Some(wc) = where_clause else {
        return Ok(());
    };
    for group in &wc.groups {
        for condition in group {
            match condition {
                WhereCondition::Comparison(c) => {
                    let op = match c.op {
                        CompOp::Lt => "<",
                        CompOp::Le => "<=",
                        CompOp::Gt => ">",
                        CompOp::Ge => ">=",
                        CompOp::Eq | CompOp::Ne => continue,
                    };
                    let value = resolve_value(&c.value, parameters)?;
                    if !orderable(&value) {
                        return Err(reject(op, &value));
                    }
                }
                WhereCondition::Between(_, low, high) => {
                    for operand in [low, high] {
                        let value = resolve_value(operand, parameters)?;
                        if !orderable(&value) {
                            return Err(reject("BETWEEN", &value));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

pub async fn execute_page<S: StorageBackend>(
    storage: &S,
    stmt: &Statement,
    parameters: &[AttributeValue],
    limit: Option<usize>,
    next_token: Option<&str>,
    consistent_read: bool,
) -> Result<StatementPage> {
    if next_token.is_some() && !matches!(stmt, Statement::Select { .. }) {
        return Err(DynoxideError::ValidationException(
            "NextToken is only valid on a SELECT statement".to_string(),
        ));
    }
    validate_ordering_operands(statement_where_clause(stmt), parameters)?;

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
    match stmt {
        Statement::Select {
            table_name,
            index_name,
            projections,
            where_clause,
            ..
        } => {
            let outcome = execute_select(
                storage,
                table_name,
                index_name.as_deref(),
                projections,
                where_clause.as_ref(),
                parameters,
                limit,
                next_token,
                consistent_read,
            )
            .await?;
            let size = outcome.items.iter().map(crate::types::item_size).sum();
            Ok(StatementPage {
                items: Some(outcome.items),
                size,
                capacity: None,
                next_token: outcome.next_token,
                read_index: outcome.index.map(|i| ReadIndex {
                    name: i.def.index_name,
                    is_lsi: i.is_lsi,
                }),
                base_reads: outcome.base_reads,
            })
        }
        Statement::Insert {
            table_name,
            item,
            if_not_exists,
        } => {
            let capacity =
                execute_insert(storage, table_name, item, parameters, *if_not_exists).await?;
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
            if let Some(variant) = returning {
                if *variant != ReturningVariant::AllOld {
                    return Err(DynoxideError::ValidationException(format!(
                        "Invalid returning clause: RETURNING {} *. Only RETURNING ALL OLD * is allowed in DELETE statements.",
                        variant.as_sql()
                    )));
                }
            }
            let (old_item, capacity) =
                execute_delete(storage, table_name, where_clause.as_ref(), parameters).await?;
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
    let parts: Vec<&str> = path.split('.').collect();
    // Use the leaf segment as the key
    let key = parts.last().unwrap();
    result.insert(key.to_string(), val);
}

#[allow(clippy::too_many_arguments)]
async fn execute_select<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    index_name: Option<&str>,
    projections: &[String],
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    limit: Option<usize>,
    next_token: Option<&str>,
    consistent_read: bool,
) -> Result<SelectOutcome> {
    // The table is resolved before the index: a qualified SELECT against a
    // table that does not exist reports the missing table, not the missing
    // index. Captured eu-west-2 2026-08-15.
    let meta = require_table(storage, table_name).await?;
    let table_key_schema = crate::actions::helpers::parse_key_schema(&meta)?;

    let index = index_name
        .map(|name| resolve_index(&meta, name))
        .transpose()?;

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
                .filter(|attr| !idx.projects(attr, &table_key_schema))
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

        if keys_index(where_clause, idx.pk_attr()) {
            let missing: Vec<String> = where_attributes(where_clause)
                .into_iter()
                .filter(|attr| !idx.projects(attr, &table_key_schema))
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

    let fingerprint = statement_fingerprint(where_clause, parameters, index_name);
    let cursor = next_token
        .map(|token| decode_next_token(token, table_name, fingerprint))
        .transpose()?;

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

    let window = evaluate_window(
        storage,
        table_name,
        index.as_ref(),
        where_clause,
        parameters,
        read_pk,
        read_sk,
        &table_key_schema,
        cursor.as_ref(),
        limit,
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

    // An LSI shares its partition with the table, so DynamoDB serves a
    // projection naming an attribute the index does not carry by reading the
    // base item. A GSI cannot, and rejects the statement above instead. The
    // index entry already holds the base key, so the fetch needs nothing the
    // row does not carry. Captured eu-west-2 2026-08-15 (case Q27).
    let reach_back = index.as_ref().is_some_and(|idx| {
        idx.is_lsi
            && projections
                .iter()
                .any(|p| !idx.projects(root_attribute(p), &table_key_schema))
    });

    let mut base_reads = 0usize;
    let mut rows = Vec::with_capacity(window.matched.len());
    for item in window.matched {
        if reach_back {
            let pk = item
                .get(&table_key_schema.partition_key)
                .and_then(|v| v.to_key_string())
                .unwrap_or_default();
            let sk = match table_key_schema.sort_key.as_deref() {
                Some(name) => item
                    .get(name)
                    .and_then(|v| v.to_key_string())
                    .unwrap_or_default(),
                None => String::new(),
            };
            if let Some(json) = storage.get_item(table_name, &pk, &sk).await? {
                if let Ok(full) = serde_json::from_str::<Item>(&json) {
                    base_reads += 1;
                    rows.push(full);
                    continue;
                }
            }
        }
        rows.push(item);
    }

    // Projections run last, so one that drops the key cannot break the
    // continuation: the cursor comes from the row as it was read.
    let items = rows
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
        next_token: token,
        index,
        base_reads,
    })
}

/// What a `SELECT` produced: its rows, where to resume, and the index it was
/// served from. The index rides along because capacity is attributed to it
/// rather than to the table, and the caller has no other way to know.
struct SelectOutcome {
    items: Vec<Item>,
    next_token: Option<String>,
    index: Option<ResolvedIndex>,
    /// How many base table items an LSI reach-back read. Each one is charged to
    /// the table arm, leaving the index arm to cover the index read alone.
    base_reads: usize,
}

/// One read of the table: the rows that matched, plus where the read stopped.
struct Window {
    matched: Vec<Item>,
    /// Where the last row read sat, matching or not. The cursor a continuation
    /// resumes from, so no row is evaluated twice.
    last_evaluated: Option<Cursor>,
    /// How many rows were read, matching or not.
    evaluated: usize,
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
) -> Result<Window> {
    let pk_condition = where_clause.and_then(|wc| find_pk_condition(wc, read_pk));

    let rows: Vec<(String, String, String)> = if let Some(pk_cond) = pk_condition {
        let pk_val = resolve_value(&pk_cond.value, parameters)?;
        let pk_str = pk_val
            .to_key_string()
            .ok_or_else(|| DynoxideError::ValidationException("Invalid key value".to_string()))?;

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
    let matched = rows
        .into_iter()
        .filter_map(|(_, _, json)| serde_json::from_str::<Item>(&json).ok())
        .filter(|item| matches_where(item, where_clause, parameters))
        .collect();

    Ok(Window {
        matched,
        last_evaluated,
        evaluated,
    })
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
/// `address.city` and `tags[0]` are both carried by `address` and `tags`.
fn root_attribute(path: &str) -> &str {
    let end = path.find(['.', '[']).unwrap_or(path.len());
    &path[..end]
}

/// Every attribute a WHERE clause reads.
fn where_attributes(where_clause: Option<&WhereClause>) -> Vec<String> {
    let Some(wc) = where_clause else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for group in &wc.groups {
        for condition in group {
            let path = match condition {
                WhereCondition::Comparison(c) => c.path.as_str(),
                WhereCondition::Exists(p)
                | WhereCondition::NotExists(p)
                | WhereCondition::BeginsWith(p, _)
                | WhereCondition::NotBeginsWith(p, _)
                | WhereCondition::Between(p, _, _)
                | WhereCondition::In(p, _)
                | WhereCondition::Contains(p, _)
                | WhereCondition::NotContains(p, _)
                | WhereCondition::IsMissing(p)
                | WhereCondition::IsNotMissing(p) => p.as_str(),
            };
            let root = root_attribute(path).to_string();
            if !out.contains(&root) {
                out.push(root);
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
    Err(DynoxideError::ValidationException(
        "The table does not have the specified index".to_string(),
    ))
}

/// A digest of the parts of a SELECT that determine its row walk: the WHERE
/// clause and the parameters. Projections are left out deliberately, because
/// they run after the read and do not move the cursor. Tokens are ephemeral
/// and in-process, so the hash does not need to be stable across builds.
fn statement_fingerprint(
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    index_name: Option<&str>,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{where_clause:?}").hash(&mut hasher);
    // The index is part of the walk, not just of the filter: a token minted
    // against one index would otherwise resume against another at a position
    // that means nothing there.
    index_name.hash(&mut hasher);
    serde_json::to_string(parameters)
        .unwrap_or_default()
        .hash(&mut hasher);
    hasher.finish()
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
    group: &[WhereCondition],
    sk_name: &str,
    parameters: &[AttributeValue],
) -> Option<Vec<ResolvedSortKeyCondition>> {
    let mut resolved = Vec::new();
    for cond in group {
        match cond {
            WhereCondition::Comparison(c) if c.path == sk_name => {
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
            WhereCondition::Between(path, lo, hi) if path == sk_name => {
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
            WhereCondition::BeginsWith(path, prefix) if path == sk_name => {
                let prefix = resolve_value(prefix, parameters).ok()?;
                prefix.to_key_string()?;
                resolved.push(ResolvedSortKeyCondition::BeginsWith(
                    sk_name.to_string(),
                    prefix,
                ));
            }
            // Any other condition shape on the sort key (IN, contains, negated
            // begins_with, the existence checks) has no key-condition
            // equivalent, so nothing at all is pushed down for the sort key.
            WhereCondition::NotBeginsWith(path, _)
            | WhereCondition::In(path, _)
            | WhereCondition::Contains(path, _)
            | WhereCondition::Exists(path)
            | WhereCondition::NotExists(path)
            | WhereCondition::IsMissing(path)
            | WhereCondition::IsNotMissing(path)
                if path == sk_name =>
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
/// `IN` on the index partition key counts, even though the read cannot push it
/// down as a single key and still scans; AWS rejects an unprojected filter
/// alongside it. An index key reached through OR does not count, and AWS
/// accepts an unprojected filter there. Captured eu-west-2 2026-08-15.
fn keys_index(where_clause: Option<&WhereClause>, pk_name: &str) -> bool {
    let Some(wc) = where_clause else {
        return false;
    };
    if wc.groups.len() != 1 {
        return false;
    }
    wc.groups[0].iter().any(|c| match c {
        WhereCondition::Comparison(cond) => cond.path == pk_name && cond.op == CompOp::Eq,
        WhereCondition::In(path, _) => path == pk_name,
        _ => false,
    })
}

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

/// The `(table, pk, sk)` a statement targets, for duplicate detection across a
/// batch. `None` when the statement does not resolve to a single item, which
/// covers a `SELECT` spanning a partition and anything whose key cannot be read
/// off the statement.
///
/// A `SELECT` naming every key attribute does resolve, and AWS rejects a batch
/// carrying two of them against one item just as it does for writes.
///
/// The result is a tuple rather than a joined string. Joining on a delimiter is
/// not injective once a key value contains it: `pk='a#b', sk='c'` and
/// `pk='a', sk='b#c'` render alike, and AWS accepts that pair as two distinct
/// items.
///
/// Errors are swallowed deliberately. A statement whose key cannot be resolved,
/// or whose table cannot be read, is left to fail on its own terms during
/// execution rather than surfacing here as a duplicate-detection failure.
pub async fn statement_target<S: StorageBackend>(
    storage: &S,
    stmt: &Statement,
    parameters: &[AttributeValue],
) -> Option<(String, String, String)> {
    let table_name = crate::partiql::parser::table_name(stmt)?;
    let meta = require_table(storage, table_name).await.ok()?;
    let key_schema = crate::actions::helpers::parse_key_schema(&meta).ok()?;

    let key_of = |source: &dyn Fn(&str) -> Option<AttributeValue>| -> Option<(String, String)> {
        let pk = source(&key_schema.partition_key)?.to_key_string()?;
        let sk = match key_schema.sort_key {
            Some(ref name) => source(name)?.to_key_string()?,
            None => String::new(),
        };
        Some((pk, sk))
    };

    let from_where = |where_clause: &Option<WhereClause>| -> Option<(String, String)> {
        let wc = where_clause.as_ref()?;
        key_of(&|name: &str| {
            find_comparison_in_groups(&wc.groups, name)
                .and_then(|cond| resolve_value(&cond.value, parameters).ok())
        })
    };

    let (pk, sk) = match stmt {
        Statement::Insert { item, .. } => key_of(&|name: &str| {
            item.get(name)
                .and_then(|v| resolve_value(v, parameters).ok())
        })?,
        Statement::Update { where_clause, .. }
        | Statement::Delete { where_clause, .. }
        // A SELECT resolves only when its WHERE pins every key attribute; one
        // spanning a partition yields nothing and takes no part in the check.
        | Statement::Select { where_clause, .. } => from_where(where_clause)?,
    };

    Some((table_name.to_string(), pk, sk))
}

/// Returns what the insert consumed. An `if_not_exists` duplicate makes it a
/// no-op, which carries no images and no index units.
async fn execute_insert<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    item_template: &HashMap<String, PartiqlValue>,
    parameters: &[AttributeValue],
    if_not_exists: bool,
) -> Result<WriteCapacity> {
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

    let meta = require_table(storage, table_name).await?;
    let key_schema = crate::actions::helpers::parse_key_schema(&meta)?;

    // Validate keys present
    crate::actions::helpers::validate_item_keys(&item, &key_schema, &meta)?;
    crate::validation::validate_item_attribute_values(&item)?;

    // Deduplicate sets
    crate::validation::normalize_item_sets(&mut item);

    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = crate::actions::helpers::extract_key_strings(&item, &key_schema)?;

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

    let item_json = serde_json::to_string(&item)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let item_size = crate::types::item_size(&item);

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
    };

    let gsi_units = crate::actions::gsi::maintain_gsis_after_write(
        storage,
        &meta,
        &target,
        old_item.as_ref(),
        &item,
    )
    .await?;

    let lsi_units = crate::actions::lsi::maintain_lsis_after_write(
        storage,
        &meta,
        &target,
        old_item.as_ref(),
        &item,
    )
    .await?;

    // Stream record
    crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), Some(&item)).await?;

    // An INSERT rejects an existing key above, so there is never an old image
    // here; `old_item` only ever comes back empty.
    Ok(WriteCapacity::new(
        table_name,
        old_item.as_ref().map(crate::types::item_size),
        Some(item_size),
        gsi_units,
        lsi_units,
    ))
}

/// Applies an UPDATE and returns the `RETURNING` projection (or `None` when the
/// statement carried no `RETURNING` clause) and what the write consumed. An
/// update that resolves to an empty item is skipped and carries no images.
async fn execute_update<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    set_clauses: &[crate::partiql::parser::SetClause],
    remove_paths: &[String],
    where_clause: Option<&WhereClause>,
    parameters: &[AttributeValue],
    returning: Option<ReturningVariant>,
) -> Result<(Option<Item>, WriteCapacity)> {
    let meta = require_table(storage, table_name).await?;
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
                "Where clause does not contain a mandatory equality on all key attributes"
                    .to_string(),
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
    let existing_json = storage.get_item(table_name, &pk_str, &sk_str).await?;
    let mut item: Item = existing_json
        .as_ref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    let old_item = item.clone();

    // PartiQL UPDATE is not an upsert: the target item must already exist, so a
    // missing item fails ConditionalCheckFailedException and creates nothing.
    // Non-key WHERE predicates act as a further condition on the existing item;
    // if that predicate is false the update fails the same way. Neither writes.
    if existing_json.is_none() || !matches_where(&old_item, where_clause, parameters) {
        return Err(DynoxideError::ConditionalCheckFailedException(
            "The conditional request failed".to_string(),
            None,
        ));
    }

    let before_item = item.clone();

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
        return Ok((
            None,
            WriteCapacity::new(table_name, None, None, HashMap::new(), HashMap::new()),
        ));
    }

    // Validate attribute values after SET clauses applied
    crate::validation::validate_item_attribute_values(&item)?;
    crate::validation::normalize_item_sets(&mut item);

    // Reject an index key this update set to an invalid value (see helpers).
    crate::actions::helpers::validate_updated_index_keys(&before_item, &item, &meta)?;

    let item_json = serde_json::to_string(&item)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let item_size = crate::types::item_size(&item);

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
    };

    let gsi_units =
        crate::actions::gsi::maintain_gsis_after_write(storage, &meta, &target, old_ref, &item)
            .await?;

    let lsi_units =
        crate::actions::lsi::maintain_lsis_after_write(storage, &meta, &target, old_ref, &item)
            .await?;

    // Stream record
    crate::streams::record_stream_event(storage, &meta, old_ref, Some(&item)).await?;

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
            old_ref.map(crate::types::item_size),
            Some(item_size),
            gsi_units,
            lsi_units,
        ),
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
) -> Result<(Option<Item>, WriteCapacity)> {
    let meta = require_table(storage, table_name).await?;
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
                "Where clause does not contain a mandatory equality on all key attributes"
                    .to_string(),
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
                None,
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
    };

    let gsi_units =
        crate::actions::gsi::maintain_gsis_after_delete(storage, &meta, &target, old_item.as_ref())
            .await?;

    let lsi_units =
        crate::actions::lsi::maintain_lsis_after_delete(storage, &meta, &target, old_item.as_ref())
            .await?;

    // Stream record
    if old_item.is_some() {
        crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), None).await?;
    }

    // A delete is charged on the item it removed, so a no-op delete against a
    // missing target carries no image and falls back to the one-unit minimum.
    let capacity =
        WriteCapacity::from_items(table_name, old_item.as_ref(), None, gsi_units, lsi_units);
    Ok((old_item, capacity))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn require_table<S: StorageBackend>(
    storage: &S,
    table_name: &str,
) -> Result<crate::storage::TableMetadata> {
    crate::actions::helpers::require_table(storage, table_name).await
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

/// The `ValidationException` DynamoDB raises for a document path that cannot be
/// applied by an update (e.g. indexing a scalar, or a missing intermediate).
fn invalid_update_path() -> DynoxideError {
    DynoxideError::ValidationException(
        "The document path provided in the update expression is invalid for update".to_string(),
    )
}

/// Set a value at a document path, navigating both map keys and list indices,
/// so `SET tags[0] = :v` writes the real list element rather than a literal
/// `tags[0]` key. A list index at or beyond the end appends, matching DynamoDB.
/// Intermediate map keys are created if absent, preserving the prior behaviour
/// for dotted map paths.
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
    let entry = item
        .entry(key)
        .or_insert_with(|| AttributeValue::M(HashMap::new()));
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
            AttributeValue::M(map) => {
                let next = map
                    .entry((*k).to_string())
                    .or_insert_with(|| AttributeValue::M(HashMap::new()));
                set_into_value(next, rest, val)
            }
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
/// rather than a literal `tags[0]` key.
fn remove_nested_value(item: &mut Item, path: &str) {
    let Some(segments) = split_path_segments(path) else {
        return;
    };
    let Some((first, rest)) = segments.split_first() else {
        return;
    };
    let PathSegment::Key(key) = first else {
        return; // the top-level item cannot be indexed
    };
    if rest.is_empty() {
        item.remove(*key);
        return;
    }
    if let Some(current) = item.get_mut(*key) {
        remove_from_value(current, rest);
    }
}

/// Recursive helper for [`remove_nested_value`]. A path that does not exist or
/// whose type does not match is a no-op, mirroring DynamoDB's tolerant REMOVE.
fn remove_from_value(current: &mut AttributeValue, segments: &[PathSegment]) {
    let (seg, rest) = segments.split_first().expect("segments is non-empty");
    if rest.is_empty() {
        match seg {
            PathSegment::Key(k) => {
                if let AttributeValue::M(map) = current {
                    map.remove(*k);
                }
            }
            PathSegment::Index(i) => {
                if let AttributeValue::L(list) = current {
                    if *i < list.len() {
                        list.remove(*i);
                    }
                }
            }
        }
        return;
    }
    match seg {
        PathSegment::Key(k) => {
            if let AttributeValue::M(map) = current {
                if let Some(next) = map.get_mut(*k) {
                    remove_from_value(next, rest);
                }
            }
        }
        PathSegment::Index(i) => {
            if let AttributeValue::L(list) = current {
                if let Some(next) = list.get_mut(*i) {
                    remove_from_value(next, rest);
                }
            }
        }
    }
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
            WhereCondition::NotBeginsWith(path, prefix_val) => {
                // Logical negation of begins_with: the row matches unless the
                // value is a string that starts with the prefix. A missing or
                // non-string attribute does not begin with the prefix, so it is
                // kept.
                if let Some(item_val) = resolve_nested_path(item, path) {
                    let prefix = match resolve_value(prefix_val, parameters) {
                        Ok(v) => v,
                        Err(_) => return false,
                    };
                    if let (AttributeValue::S(s), AttributeValue::S(p)) = (item_val, &prefix) {
                        if s.starts_with(p.as_str()) {
                            return false;
                        }
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
        }
    }

    true
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

/// Format a BigDecimal number, stripping unnecessary trailing zeros.
fn format_bigdecimal(n: &bigdecimal::BigDecimal) -> String {
    let normalized = n.normalized();
    if normalized.as_bigint_and_exponent().1 < 0 {
        normalized.with_scale(0).to_string()
    } else {
        normalized.to_string()
    }
}
