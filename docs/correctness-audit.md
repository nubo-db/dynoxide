# Dynoxide Correctness Audit

**Date:** 2026-03-16
**Scope:** Full implementation correctness vs DynamoDB behaviour
**Context:** Discovered during Phase 5 review that PartiQL INSERT `?` placeholders were broken. Expanded audit to cover all operations.

## CRITICAL — Silent Wrong Results (10)

### Core Operations (5)

| # | Issue | Location | Status |
|---|-------|----------|--------|
| C1 | Number comparison uses f64 — loses precision beyond ~15 digits | `expressions/condition.rs:516-521` | Open |
| C2 | Number arithmetic (SET +/-) uses f64 — silent data corruption | `expressions/update.rs:181-196` | Open |
| C3 | Number arithmetic (ADD) uses f64 — silent data corruption | `expressions/update.rs:250-256` | Open |
| C4 | `begins_with` sort key uses SQL LIKE without escaping `%` and `_` | `expressions/key_condition.rs:147-153` | Open |
| C5 | Query/Scan COUNT returns scanned_count not filtered count | `actions/query.rs:300`, `actions/scan.rs:262` | Open |

### PartiQL (5)

| # | Issue | Location | Status |
|---|-------|----------|--------|
| C6 | INSERT silently overwrites existing items (should fail) | `partiql/executor.rs:125` | Open |
| C7 | `?` placeholders missing in INSERT VALUE literals | `partiql/parser.rs:434` | Open |
| C8 | Negative numbers tokenised as two tokens (`-` + `42`) | `partiql/parser.rs:598` | Open |
| C9 | Escaped single quotes (`''`) handler is dead code | `partiql/parser.rs:631-639` | Open |
| C10 | INSERT in ExecuteTransaction silently overwrites | `actions/execute_transaction.rs` | Open |

## IMPORTANT — Wrong Behaviour (21)

### Core Operations (10)

| # | Issue | Location | Status |
|---|-------|----------|--------|
| I1 | PutItem doesn't validate ReturnValues parameter | `actions/put_item.rs` | Fixed |
| I2 | DeleteItem doesn't validate ReturnValues parameter | `actions/delete_item.rs` | Fixed |
| I3 | UpdateItem allows REMOVE/ADD/DELETE on key attributes | `actions/update_item.rs` | Fixed |
| I4 | PutItem/DeleteItem/UpdateItem condition + write not in SQLite transaction | `actions/put_item.rs`, `actions/delete_item.rs`, `actions/update_item.rs` | Fixed |
| I5 | `size()` function works on N/BOOL/NULL (should return no match) | `expressions/condition.rs` | Fixed |
| I6 | 1MB limit counts filtered items only (should count all scanned) | `actions/query.rs`, `actions/scan.rs` | Fixed |
| I7 | GSI query LastEvaluatedKey missing table primary key | `actions/query.rs` | Fixed |
| I8 | GSI scan LastEvaluatedKey missing table primary key | `actions/scan.rs` | Fixed |
| I9 | BatchWriteItem no duplicate key detection | `actions/batch_write_item.rs` | Fixed |
| I10 | TransactWriteItems 4MB check underestimates update size | `actions/transact_write_items.rs` | Fixed |
| I11 | UpdateItem accepts request with no UpdateExpression | `actions/update_item.rs` | Not a bug — DynamoDB allows UpdateItem without UpdateExpression (acts as conditional no-op) |

### PartiQL (11)

| # | Issue | Location | Status |
|---|-------|----------|--------|
| I12 | UPDATE REMOVE clause not supported | `partiql/parser.rs` | Fixed |
| I13 | SET with nested paths creates wrong top-level keys | `partiql/executor.rs` | Fixed |
| I14 | SET does not support expressions (`count + 1`, `list_append`) | `partiql/parser.rs`, `partiql/executor.rs` | Fixed |
| I15 | DELETE with missing sort key silently does nothing | `partiql/executor.rs` | Fixed |
| I16 | No LIMIT or pagination (NextToken) for SELECT | `actions/execute_statement.rs`, `partiql/executor.rs` | Fixed |
| I17 | No COUNT(*) support | `partiql/parser.rs`, `partiql/executor.rs` | Fixed |
| I18 | No set literal syntax (`<< >>`) | `partiql/parser.rs` | Fixed |
| I19 | OR conditions and parenthesised grouping not supported | `partiql/parser.rs`, `partiql/executor.rs` | Fixed |
| I20 | Nested paths in WHERE functions only capture first token | `partiql/parser.rs` | Fixed |
| I21 | `?` not supported in list/map values in SET | `partiql/parser.rs` | Fixed |
| I22 | INSERT does not support IF NOT EXISTS | `partiql/parser.rs`, `partiql/executor.rs` | Fixed |

## MINOR (10)

| # | Issue | Location | Status |
|---|-------|----------|--------|
| M1 | Projection flattens nested structure into top-level keys | `partiql/executor.rs:116` | Fixed |
| M2 | Double-quoted identifier escaping (`""`) not handled | `partiql/parser.rs:650` | Fixed |
| M3 | Unknown tokeniser characters silently skipped | `partiql/parser.rs:688` | Fixed |
| M4 | BatchGetItem UnprocessedKeys drops projection/consistent settings | `actions/batch_get_item.rs:127` | Fixed |
| M5 | TransactWriteItems missing ItemCollectionMetrics in response | `actions/transact_write_items.rs:99` | Fixed — field added, always None (full metrics computation deferred) |
| M6 | TransactWriteItems client_request_token ignored | `actions/transact_write_items.rs:13` | Known limitation — idempotency tokens require persistent token-to-result storage, out of scope for local emulator |
| M7 | PutItem storage double-reads item with ConditionExpression | `actions/put_item.rs` + `storage.rs` | Known limitation — minor performance inefficiency, not worth the API surface change |
| M8 | GSI sk column replacement is fragile string hack | `storage.rs:679` | Documented — comment added explaining assumptions; full refactor deferred |
| M9 | No ConsistentRead field on ExecuteStatement | `actions/execute_statement.rs` | Fixed |
| M10 | Missing DuplicateItemException error type | `errors.rs` | Fixed |
