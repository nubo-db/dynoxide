# Dynoxide DynamoDB Compatibility Gaps

Prioritised list of gaps between Dynoxide and the DynamoDB API surface.

> **Original audit date:** 2026-03-14 · **Commit:** `45bd46c`
> **Updated:** 2026-03-24 · All 12 original gaps resolved. Conformance suite (526 tests, 100%) validates all fixes. See also [correctness-audit.md](correctness-audit.md) for additional fixes beyond the original gap analysis.

---

## P1 - Likely to Block Adoption

All five P1 gaps have been resolved.

### 1. No Local Secondary Indexes (LSI) - RESOLVED

**Impact:** Any table that defines LSIs at creation time will fail. Users with existing DynamoDB tables using LSIs cannot test against Dynoxide.

**Scope:** CreateTable needs to accept `LocalSecondaryIndexes`, create per-LSI SQLite tables (similar to GSIs but sharing the partition key), and route Query operations to LSI tables when `IndexName` points to an LSI.

**Workaround:** None - restructure queries to use GSIs or base table.

**Resolution:** Phase 4 (`c4ddb85`). LSI creation, storage, Query/Scan routing, and projection support fully implemented.

### 2. No Empty String / Empty Set Rejection - RESOLVED

**Impact:** DynamoDB rejects empty S values and empty SS/NS/BS sets. Tests that rely on this validation will pass against Dynoxide but fail against real DynamoDB.

**Scope:** Add validation in PutItem, UpdateItem, and BatchWriteItem to reject items containing empty strings or empty sets.

**Resolution:** Phase 1 (`d549bb6`). Item validation rejects empty strings and empty sets on all write paths.

### 3. No Number Precision Validation - RESOLVED

**Impact:** DynamoDB supports up to 38 digits of precision with range ±1E+126. Numbers outside this range are rejected. Dynoxide stores numbers as strings without range checking, so invalid numbers will be silently accepted.

**Scope:** Add validation in write paths to reject numbers outside DynamoDB's supported range.

**Resolution:** Phase 1 (`d549bb6`). Number precision and range validation enforced on write paths.

### 4. No Set Deduplication - RESOLVED

**Impact:** DynamoDB deduplicates SS/NS/BS sets on write. Dynoxide stores sets as-is. Tests comparing set contents may see different ordering or duplicates.

**Scope:** Deduplicate sets during PutItem, UpdateItem, and ADD operations.

**Resolution:** Phase 1 (`d549bb6`). Sets are deduplicated on all write paths.

### 5. ReturnItemCollectionMetrics Never Populated - RESOLVED

**Impact:** Applications that inspect `ItemCollectionMetrics` in responses will receive empty/null values. Low impact since most applications don't use this, but SDK tests or compliance checks may fail.

**Scope:** Track per-partition-key collection sizes and return them when `ReturnItemCollectionMetrics: SIZE` is set.

**Resolution:** Phase 5 (`1bf146b`). BatchWriteItem returns `ItemCollectionMetrics` when requested. TransactWriteItems includes the field in responses (`507a88a`). Full per-partition-key size computation is deferred - the field is present but currently returns None.

---

## P2 - Will Matter for Some Users

All seven P2 gaps have been resolved.

### 6. No Parallel Scan (Segment/TotalSegments) - RESOLVED

**Impact:** Applications using parallel scan for performance will silently get full table scans for each segment, returning duplicate data across workers. Incorrect results, not just slow performance.

**Scope:** Implement hash-based segment assignment. For each item, compute `hash(pk) % TotalSegments` and only return items matching the requested `Segment`.

**Resolution:** Phase 2 (`116254a`). Hash-based segment assignment implemented with SQLite-level filtering. Optimised in `2a62877`.

### 7. ExecuteTransaction (PartiQL) Not Implemented - RESOLVED

**Impact:** Users of PartiQL's transactional execution cannot use Dynoxide.

**Workaround:** Use `TransactWriteItems`/`TransactGetItems` instead.

**Resolution:** Phase 5 (`1bf146b`). Full ExecuteTransaction support with transactional semantics.

### 8. PartiQL WHERE Clause Gaps - RESOLVED

**Impact:** PartiQL queries using `BETWEEN`, `IN`, `CONTAINS`, or `IS MISSING` in WHERE clauses will fail.

**Scope:** Extend the PartiQL parser and executor to handle these operators (the condition expression engine already supports them - the logic could be shared).

**Resolution:** Phase 3 (`97a03d1`). BETWEEN, IN, CONTAINS, IS MISSING, and IS NOT MISSING all implemented in the PartiQL parser and executor. Additionally, OR conditions and parenthesised grouping were added (`aecc8ed`).

### 9. ConsumedCapacity Per-GSI Breakdown Missing - RESOLVED

**Impact:** `ReturnConsumedCapacity: INDEXES` returns table-level capacity but `GlobalSecondaryIndexes` is always `None`. Applications that monitor per-index capacity will see incomplete data.

**Scope:** Track and return per-GSI capacity units when INDEXES mode is requested.

**Resolution:** Phase 5 (`1bf146b`). Per-GSI capacity breakdown returned in INDEXES mode.

### 10. PartiQL Nested Path Projections - RESOLVED

**Impact:** PartiQL `SELECT nested.path FROM ...` fails. Only flat attribute names are supported in projections.

**Scope:** Extend the PartiQL parser to handle dot-notation paths in SELECT clauses.

**Resolution:** Phase 3 (`97a03d1`). Nested path projections and correct structure preservation implemented. Projection flattening bug also fixed (`507a88a`).

### 11. CreateTable Missing Parameters - RESOLVED

**Impact:** `SSESpecification`, `TableClass`, `Tags` (inline), and `DeletionProtectionEnabled` are not accepted in CreateTable. Users with CloudFormation or Terraform templates that include these will get deserialisation errors.

**Scope:** Accept these parameters - SSE and TableClass can be stored but not enforced; Tags should call tag_resource internally; DeletionProtectionEnabled should prevent DeleteTable.

**Resolution:** Phase 3 (`97a03d1`). SSESpecification, TableClass, Tags, and DeletionProtectionEnabled all accepted. DeletionProtectionEnabled prevents DeleteTable; Tags calls tag_resource internally.

### 12. Unused Expression Attribute Names/Values Not Rejected - RESOLVED

**Impact:** DynamoDB rejects requests where `ExpressionAttributeNames` or `ExpressionAttributeValues` contain entries not referenced in the expression. Dynoxide only validates that referenced names/values exist in the map, not that all map entries are used. Tests relying on this validation will pass against Dynoxide but fail against DynamoDB.

**Scope:** After expression parsing, check that all provided names/values were consumed.

**Resolution:** Phase 2 (`f74aca6`). Unused expression attribute names and values are now rejected with ValidationException.

---

## P3 - Nice to Have / Not Expected Locally

Features that are meaningless or very rarely needed in a local emulator. These remain as-is - they are intentionally out of scope.

### 13. Backup/Restore Operations (8 operations)

CreateBackup, DeleteBackup, DescribeBackup, ListBackups, RestoreTableFromBackup, RestoreTableToPointInTime, DescribeContinuousBackups, UpdateContinuousBackups.

**Note:** Dynoxide provides `vacuum_into()` and `restore_from()` for snapshot management, which covers the practical use case.

**DDB Local:** Not supported.

### 14. Global Table Operations (6 operations)

Meaningless for a single-region local emulator.

**DDB Local:** Not supported.

### 15. Kinesis Streaming Destination (3 operations)

No Kinesis integration needed locally.

**DDB Local:** Not supported.

### 16. Import/Export Operations (6 operations)

Dynoxide has its own `import_items()` API and CLI import tool. The DynamoDB Import/Export API format is different.

**DDB Local:** Not supported.

### 17. Contributor Insights, Resource Policy, Capacity Management (16 operations)

Administrative/billing features with no local equivalent.

**DDB Local:** Not supported (always on-demand billing).

### 18. DescribeEndpoints

Could trivially return the local server address. Very low priority.

**DDB Local:** Not supported.

### 19. Single-Shard Stream Model

DescribeStream returns a single shard regardless of data volume. `ExclusiveStartShardId` and `Limit` on DescribeStream are accepted but ignored. Adequate for testing but doesn't model real shard splitting.

**DDB Local:** Also single-shard.

### 20. Missing Transaction Error Types

`TransactionConflictException`, `TransactionInProgressException` are not in the error enum. These are edge cases related to concurrent transaction conflicts that don't arise in a single-writer SQLite model.

**DDB Local:** Same limitation (single-process model).

### 21. Validation Ordering

DynamoDB validates parameters in a specific order. Dynoxide may return a different error type for requests with multiple validation issues. This only matters for tests that assert on specific error messages for intentionally malformed requests.

**DDB Local:** Fails 28 of 30 Tier 3 validation tests (worse than Dynoxide's 0 failures).

---

## Summary

| Priority | Count | Original Status | Current Status | Also missing in DDB Local |
|----------|-------|-----------------|----------------|---------------------------|
| **P1** | 5 | Open | All resolved | 3 failures (ItemCollectionMetrics) |
| **P2** | 7 | Open | All resolved | 9 failures (tags, PartiQL error code) |
| **P3** | 9 | Intentionally out of scope | Unchanged | ~7 also not supported |

All 12 actionable gaps (P1 + P2) identified in the original audit have been resolved across five implementation phases. In addition, the [correctness audit](correctness-audit.md) identified and resolved 31 further issues (10 critical, 10 important core, 11 important PartiQL) plus 10 minor issues, bringing overall DynamoDB compatibility to a significantly higher level.

### Known Remaining Limitations

These are intentional trade-offs documented in the correctness audit:

- **PutItem double-read (M7):** PutItem with ConditionExpression reads the item twice (once for condition check, once for old-image capture). A minor performance inefficiency not worth the API surface change.
- **GSI sk column replacement (M8):** String-based column name replacement in GSI queries. Documented with assumptions; full refactor deferred.
- **Number arithmetic precision:** Comparisons and arithmetic (SET +/-, ADD) use arbitrary-precision decimal via the `rust_decimal` crate (`083491c`), but the underlying precision model differs slightly from DynamoDB's proprietary implementation.
- **Single-shard streams:** DescribeStream models a single shard regardless of data volume.
- **Validation ordering:** 526/526 conformance tests pass; minor differences may exist for edge cases with multiple simultaneous validation errors not covered by the suite.
- **P3 features:** Backup/restore, global tables, Kinesis, import/export, and administrative operations are intentionally not implemented.
