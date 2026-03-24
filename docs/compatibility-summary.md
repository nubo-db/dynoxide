# Dynoxide DynamoDB Compatibility

Dynoxide is a lightweight, embeddable DynamoDB emulator backed by SQLite. It is designed for local development, testing, and CI pipelines — not as a production DynamoDB replacement.

**What "not applicable" means:** Dynoxide does not emulate capacity management, throttling, global replication, backup infrastructure, or Kinesis integration. These features are meaningless for a local emulator and are marked as "not applicable" rather than "not implemented."

**Consistency model:** SQLite provides strong consistency. `ConsistentRead` is accepted but has no effect — all reads are strongly consistent.

> **Updated:** 2026-03-24 · Reflects all fixes from compatibility phases 1–5, correctness audit, and conformance suite results.

---

## Conformance

Dynoxide's DynamoDB compatibility is independently verified by the
[dynamodb-conformance](https://github.com/nubo-db/dynamodb-conformance) test
suite — 526 tests across 3 tiers, validated against real DynamoDB ground truth.

| Target | Tier 1 (Core) | Tier 2 (Complete) | Tier 3 (Strict) | Total |
|---|---|---|---|---|
| DynamoDB | 100% | 100% | 100% | 100% (526/526) |
| **Dynoxide** | **100%** | **100%** | **100%** | **100% (526/526)** |
| DynamoDB Local | 98.9% (264/267) | 90.3% (84/93) | 81.9% (136/166) | 92.0% (484/526) |

Tier 1 covers core CRUD, queries, scans, batch operations, and GSIs. Tier 2
adds transactions, PartiQL, streams, TTL, tags, and UpdateTable. Tier 3 covers
validation ordering, error message fidelity, reserved words, and edge cases.

"100% conformance on 526 tests" means Dynoxide matches real DynamoDB behaviour
for every test in the suite. It does not mean "100% DynamoDB compatible" — there
are aspects of DynamoDB the suite does not yet cover.

**How Dynoxide compares to DynamoDB Local:** DynamoDB Local is itself a subset of
the full DynamoDB API — it fails 42 of the 526 conformance tests that real
DynamoDB passes. Where a feature is unsupported in both, it is marked as such.
Where Dynoxide supports something DynamoDB Local does not, this is noted
explicitly.

---

## Operation Coverage

### Core Operations — Fully Supported

| Category | Operations | Dynoxide | DDB Local |
|----------|-----------|----------|-----------|
| **Item CRUD** | PutItem, GetItem, UpdateItem, DeleteItem | Full | Partial — ItemCollectionMetrics returns null |
| **Query & Scan** | Query, Scan | Full | Full |
| **Batch** | BatchGetItem, BatchWriteItem | Full | Partial — ItemCollectionMetrics returns null |
| **Transactions** | TransactWriteItems, TransactGetItems | Full | Full |
| **Table Management** | CreateTable, DeleteTable, DescribeTable, UpdateTable, ListTables | Full | Full |
| **TTL** | UpdateTimeToLive, DescribeTimeToLive | Full | Full |
| **Tags** | TagResource, UntagResource, ListTagsOfResource | Full | Not supported |
| **Streams** | ListStreams, DescribeStream, GetShardIterator, GetRecords | Full | Full (single-shard) |
| **PartiQL** | ExecuteStatement, BatchExecuteStatement, ExecuteTransaction | Full | Partial — wrong error code for duplicate INSERT |

### Not Implemented

| Category | Operations | Impact |
|----------|-----------|--------|
| **Backup/Restore** | CreateBackup, DeleteBackup, RestoreTable*, etc. (8 ops) | Not applicable |
| **Global Tables** | CreateGlobalTable, DescribeGlobalTable, etc. (6 ops) | Not applicable |
| **Kinesis** | Enable/Disable/DescribeKinesisStreamingDestination | Not applicable |
| **Import/Export** | ImportTable, ExportTableToPointInTime, etc. (6 ops) | Not applicable |
| **Capacity** | DescribeReservedCapacity, DescribeLimits, etc. (5 ops) | Not applicable |
| **Other** | ContributorInsights, ResourcePolicy, TableReplicas, DescribeEndpoints (9 ops) | Not applicable |

**27 of 27 applicable DynamoDB operations are implemented.** The remaining 39 operations are cloud-infrastructure features with no meaningful local equivalent.

---

## Expression Support

| Expression Type | Status | Notes |
|-----------------|--------|-------|
| **ConditionExpression** | Full | All comparisons, functions, logical operators, BETWEEN, IN |
| **FilterExpression** | Full | Same grammar as ConditionExpression |
| **KeyConditionExpression** | Full | PK equality + SK comparisons/BETWEEN/begins_with |
| **ProjectionExpression** | Full | Top-level, nested paths, list indexing |
| **UpdateExpression** | Full | SET (with if_not_exists, list_append, arbitrary-precision arithmetic), REMOVE, ADD, DELETE |
| **ExpressionAttributeNames** | Full | `#name` substitution; unused entries rejected |
| **ExpressionAttributeValues** | Full | `:value` substitution; unused entries rejected |

### Condition/Filter Functions

`attribute_exists` · `attribute_not_exists` · `attribute_type` · `begins_with` · `contains` · `size` — all supported.

---

## Index Support

| Feature | Status |
|---------|--------|
| GSI on CreateTable | Supported |
| GSI add/remove via UpdateTable | Supported (with backfill) |
| GSI projection ALL / KEYS_ONLY / INCLUDE | Supported |
| Sparse GSI (items without GSI keys excluded) | Supported |
| Per-GSI ConsumedCapacity (INDEXES mode) | Supported |
| **LSI on CreateTable** | Supported |
| **LSI projection ALL / KEYS_ONLY / INCLUDE** | Supported |
| **LSI Query routing** | Supported |
| **LSI Scan routing** | Supported |

---

## PartiQL Support

Supports `SELECT`, `INSERT`, `UPDATE`, `DELETE` with full WHERE clause support:

- **Comparisons:** `=`, `<>`, `<`, `>`, `<=`, `>=`
- **Range/membership:** `BETWEEN`, `IN`
- **Functions:** `EXISTS`, `NOT EXISTS`, `BEGINS_WITH`, `CONTAINS`
- **Existence:** `IS MISSING`, `IS NOT MISSING`
- **Logical:** `AND`, `OR`, `NOT`, parenthesised grouping
- **Projections:** Nested dot-notation paths, `COUNT(*)`
- **Pagination:** `LIMIT`, `NextToken`
- **Literals:** Set literals (`<< >>`), negative numbers, escaped quotes
- **Mutations:** `INSERT` (with IF NOT EXISTS, rejects duplicates), `UPDATE` (SET with expressions, REMOVE), `DELETE` (requires sort key)
- **Transactions:** `ExecuteTransaction` with all-or-nothing semantics

Parameter placeholders (`?`) supported in all positions including nested list/map values.

**Not supported:** `RETURNING` clause.

---

## Data Validation

| Validation | Status |
|------------|--------|
| Empty string rejection | Enforced on all write paths |
| Empty set rejection | Enforced on all write paths |
| Number precision (38 digits, ±1E+126 range) | Enforced on all write paths |
| Set deduplication (SS/NS/BS) | Enforced on all write paths |
| 400KB item size limit | Enforced |
| Unused ExpressionAttributeNames/Values | Rejected with ValidationException |
| ReturnValues parameter validation | Enforced (PutItem, DeleteItem accept only NONE/ALL_OLD) |
| Key attribute protection | UpdateItem rejects REMOVE/ADD/DELETE on key attributes |
| BatchWriteItem duplicate key detection | Enforced |

---

## Where Dynoxide Exceeds DynamoDB Local

### Conformance advantages

DynamoDB Local fails 42 of 526 conformance tests. Grouped by category:

| Category | Failures | Details |
|---|---|---|
| Table name validation messages | 15 | DDB Local returns generic "Invalid table/index name" instead of specific AWS constraint messages |
| Tags (TagResource, UntagResource, ListTagsOfResource) | 8 | DDB Local returns `UnknownOperationException: Tagging is not currently supported` |
| Validation ordering — wrong exception type | 4 | DDB Local returns `ResourceNotFoundException` or `InternalFailure` instead of `ValidationException` |
| CreateTable error message fidelity | 4 | DDB Local uses its own wording for KeySchema and index validation errors |
| ItemCollectionMetrics | 3 | DDB Local returns `undefined` for `ReturnItemCollectionMetrics: SIZE` |
| Scan parallel validation messages | 3 | DDB Local uses different wording for Segment/TotalSegments validation |
| Batch operation error messages | 2 | DDB Local uses its own wording for empty RequestItems errors |
| Query validation messages | 2 | DDB Local conflates Select and ReturnConsumedCapacity validation |
| PartiQL error code | 1 | DDB Local returns `DuplicateItem` instead of `DuplicateItemException` |

### Capability advantages

| Capability | Notes |
|---|---|
| MCP server (33 tools, stdio + HTTP) | Exposes all DynamoDB operations as tools for coding agents |
| Embedded mode (direct Rust API) | `Database::memory()` — no HTTP, no serialisation overhead |
| Snapshots + auto-snapshot before destructive ops | Point-in-time save/restore for safe experimentation |
| OneTable data model integration | `--data-model` loads entity schemas for agent context |
| Anonymised import with rule-based anonymisation | Import DynamoDB exports with fake/mask/hash/redact rules |
| SQLCipher encryption at rest | `encryption` feature flag for encrypted databases |
| iOS/native embedding | No runtime dependencies — runs on platforms where Docker can't |
| Sub-millisecond startup, ~5 MB binary | vs ~2.5s and ~225 MB for DynamoDB Local |

---

## Known Remaining Limitations

- **Single-shard stream model** — DescribeStream returns a single shard; `ExclusiveStartShardId` and `Limit` accepted but ignored
- **Number arithmetic precision** — uses `rust_decimal` for arbitrary-precision arithmetic, which may have minor differences from DynamoDB's proprietary implementation at extreme edge cases
- **Transaction contention errors** — `TransactionConflictException` and `TransactionInProgressException` not emulated (concurrent transaction contention doesn't apply to single-process emulator)

### Legacy Pre-2015 API Parameters

The legacy filter and update API (pre-expression-based API from before 2015) has partial support:

| Parameter | Supported | Notes |
|-----------|-----------|-------|
| `AttributeUpdates` (UpdateItem) | Partial | PUT, ADD, DELETE actions supported; used when `UpdateExpression` is absent |
| `Expected` (PutItem, UpdateItem, DeleteItem) | Accepted, ignored | Use `ConditionExpression` instead |
| `ScanFilter` / `QueryFilter` | Accepted, ignored | Use `FilterExpression` instead |
| `KeyConditions` (Query) | Accepted, ignored | Use `KeyConditionExpression` instead |
| `AttributesToGet` (GetItem, Query, Scan) | Accepted, ignored | Use `ProjectionExpression` instead |
| `ConditionalOperator` | Accepted, ignored | Use `ConditionExpression` with `AND`/`OR` instead |

All legacy parameters are silently accepted during deserialisation (serde ignores unknown fields by default). `AttributeUpdates` is the only one actively processed. Users should prefer the expression-based API (`UpdateExpression`, `FilterExpression`, `KeyConditionExpression`, `ProjectionExpression`, `ConditionExpression`) for full functionality.

---

## Data Types

All 10 DynamoDB types fully supported: `S`, `N`, `B`, `BOOL`, `NULL`, `SS`, `NS`, `BS`, `L`, `M`.

---

## Error Codes

Dynoxide returns DynamoDB-compatible error codes with the `com.amazonaws.dynamodb.v20120810#` prefix:

`ResourceNotFoundException` · `ResourceInUseException` · `ValidationException` · `ConditionalCheckFailedException` (with optional Item) · `TransactionCanceledException` · `ItemCollectionSizeLimitExceededException` · `ProvisionedThroughputExceededException` · `LimitExceededException` · `DuplicateItemException` · `InternalServerError`

---

## Audit History

The original compatibility audit (2026-03-14, commit `45bd46c`) identified 12 actionable gaps (5 P1, 7 P2) and 9 intentionally out-of-scope items (P3). All 12 actionable gaps were resolved across five implementation phases. A subsequent correctness audit (2026-03-16) identified and resolved a further 41 issues: 10 critical silent-wrong-result bugs, 21 important behavioural issues, and 10 minor issues.

See [compatibility-matrix.md](compatibility-matrix.md) for the full detailed parameter-level audit, [gaps.md](gaps.md) for the gap resolution log, and [correctness-audit.md](correctness-audit.md) for the correctness findings.
