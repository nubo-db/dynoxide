# Dynoxide DynamoDB Compatibility

Dynoxide is an embeddable DynamoDB emulator backed by SQLite. It is designed for local development, testing, and CI pipelines - not as a production DynamoDB replacement.

**What "not applicable" means:** Dynoxide does not emulate capacity management, throttling, global replication, backup infrastructure, or Kinesis integration. These features are meaningless for a local emulator and are marked as "not applicable" rather than "not implemented."

**Consistency model:** SQLite provides strong consistency. `ConsistentRead` is accepted but has no effect - all reads are strongly consistent.

> Behaviour validated by the [conformance suite](https://github.com/paritysuite/dynamodb-conformance). Pass rates move as the suite grows, so this page links to the [live results](https://paritysuite.org) rather than pinning a snapshot.

---

## Conformance

Dynoxide's DynamoDB compatibility is independently verified by **Parity Suite**, the
[DynamoDB conformance suite](https://github.com/paritysuite/dynamodb-conformance) that
runs one test matrix against real DynamoDB and every major emulator across
three tiers:

- **Tier 1 (Core)** - CRUD, queries, scans, batch operations, GSIs, and UpdateTable
- **Tier 2 (Complete)** - transactions, PartiQL, LSIs, streams, TTL, and tags
- **Tier 3 (Strict)** - validation ordering, error-message fidelity, reserved
  words, legacy-API handling, and edge cases

Disclosure: Dynoxide and Parity Suite are maintained by the same person. The suite scores Dynoxide on the same public matrix it runs against every other engine, and the results and test code are open.

Pass rates move as the suite grows and each engine changes, so rather than pin a
snapshot that goes stale, the current standings are published live:

- **[Live standings](https://paritysuite.org)** - pass rates for every engine, broken down by tier
- **[Live capability matrix](https://paritysuite.org/capabilities)** - the feature-by-feature support matrix
- **[paritysuite/dynamodb-conformance](https://github.com/paritysuite/dynamodb-conformance#results)** - the suite, the raw results, and how each target is run

A high conformance score means Dynoxide matches real DynamoDB behaviour for the
tests in the suite. It does not mean "100% DynamoDB compatible" - there are
aspects of DynamoDB the suite does not yet cover, and the limitations below are
the ones worth knowing.

---

## Operation Coverage

### Core Operations - Fully Supported

| Category | Operations | Dynoxide | DDB Local |
|----------|-----------|----------|-----------|
| **Item CRUD** | PutItem, GetItem, UpdateItem, DeleteItem | Full | Partial - ItemCollectionMetrics returns null |
| **Query & Scan** | Query, Scan | Full | Full |
| **Batch** | BatchGetItem, BatchWriteItem | Full | Partial - ItemCollectionMetrics returns null |
| **Transactions** | TransactWriteItems, TransactGetItems | Full | Full |
| **Table Management** | CreateTable, DeleteTable, DescribeTable, UpdateTable, ListTables | Full | Full |
| **TTL** | UpdateTimeToLive, DescribeTimeToLive | Full | Full |
| **Tags** | TagResource, UntagResource, ListTagsOfResource | Full | Not supported |
| **Streams** | ListStreams, DescribeStream, GetShardIterator, GetRecords | Full | Full (single-shard) |
| **PartiQL** | ExecuteStatement, BatchExecuteStatement, ExecuteTransaction | Full | Partial - wrong error code for duplicate INSERT |
| **Vector search** | SearchVectors | Full (native and wasm) | Not supported |

### Not Implemented

| Category | Operations | Impact |
|----------|-----------|--------|
| **Backup/Restore** | CreateBackup, DeleteBackup, RestoreTable*, etc. (8 ops) | Not applicable |
| **Global Tables** | CreateGlobalTable, DescribeGlobalTable, etc. (6 ops) | Not applicable |
| **Kinesis** | Enable/Disable/DescribeKinesisStreamingDestination | Not applicable |
| **Import/Export** | ImportTable, ExportTableToPointInTime, etc. (6 ops) | Not applicable |
| **Capacity** | DescribeReservedCapacity, DescribeLimits, etc. (5 ops) | Not applicable |
| **Other** | ContributorInsights, ResourcePolicy, TableReplicas, DescribeEndpoints (9 ops) | Not applicable |

**28 of 28 applicable DynamoDB operations are implemented**, including `SearchVectors` and the vector index surface on `CreateTable`, `UpdateTable` and `DescribeTable`. The remaining 39 operations are cloud-infrastructure features with no meaningful local equivalent.

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

`attribute_exists` · `attribute_not_exists` · `attribute_type` · `begins_with` · `contains` · `size` - all supported.

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
| **Per-LSI ConsumedCapacity (INDEXES mode)** | Supported |
| **Vector index on CreateTable** | Supported |
| **Vector index add/remove via UpdateTable** | Supported (with backfill) |
| **Vector index projection ALL / KEYS_ONLY / INCLUDE** | Supported |
| **SearchSchema HASH scoping and INLINE_FILTER** | Supported |
| **`SearchVectors` (COSINE, EUCLIDEAN, DOT_PRODUCT)** | Supported |
| **Per-vector-index ConsumedCapacity (INDEXES mode)** | Supported |

A vector index goes `ACTIVE` immediately, where AWS reports it `CREATING` and
backfills in the background before it becomes searchable. The backfilled data
matches; only the lifecycle is compressed. The same is true of adding a GSI, and
it has the same consequence: the `Backfilling` flag and the errors that only
arise while an index is still filling are unreachable here, so code that waits
for them will wait forever against Dynoxide and should poll for `ACTIVE`
instead.

Vector search is exact brute-force KNN rather than an approximate index. At
emulator and embedded scale that reaches a correct answer faster than building
an ANN structure would, and it removes the recall question entirely: the top-k
is the true top-k. It also means a search costs time linear in the number of
indexed entries, so a local table holding millions of vectors is outside what
this is built for.

Two scoring details worth knowing. Vectors are stored and compared at `f32`,
so a value written at full `f64` precision reads back through the index rounded,
while the base table keeps exactly what was written. And where several entries
tie on score, Dynoxide breaks the tie deterministically; AWS does not commit to
an order there, so a tie-dependent assertion will hold here and may not against
the real thing.

`SearchVectors` reports `VectorSearchRequestBytes`, and that figure is a
divergence by construction. Real DynamoDB bills a search on the data it read and
does not reproduce its own number between identical calls; five identical
searches over one unchanged index reported 14214, 13903, 14214, 14214 and 14518.
Dynoxide reports a deterministic figure sized on the entries the search scanned,
using the same measure a write is captured against. The unit is therefore the
captured one, but the quantity is still Dynoxide's own and not a figure to
compare against AWS. Each entry's size is computed once when it is written and
stored with it, so asking a search for capacity costs nothing extra. The write
axis is the captured one: a write's
`VectorWriteRequestBytes` is `4 * dimensions` plus the vector attribute's name
plus the item size of the rest of the projected entry, held to a 1KB floor, and
matches eu-west-2 byte for byte across fixtures from 3 to 512 dimensions.

A vector index is not reachable through PartiQL, matching AWS: naming one in a
`"table"."index"` qualifier answers `Scan operation not supported on this index
type`. PartiQL reads of the base table return the vector attribute like any
other attribute.

Index write capacity is charged against the change to what an index stores, as
DynamoDB does, rather than against the item the write leaves behind. A write that
leaves an index's stored view untouched reports no arm for that index at all,
moving an index key costs two writes, and removing one costs a single delete.
Sizing is on the projected index entry, so an attribute the index does not
project costs it nothing.

Every write surface reports the breakdown: `PutItem`, `UpdateItem`, `DeleteItem`,
`BatchWriteItem`, `TransactWriteItems`, `ExecuteStatement`, `ExecuteTransaction`
and `BatchExecuteStatement`.

The transactional 2x factor applies to the base table arm alone. An index arm
inside a `TransactWriteItems` or `ExecuteTransaction` costs what the same write
costs outside a transaction, so a GSI key move charges the index the same either
way while the table arm doubles.

A transactional table arm is sized on the larger of the item's before and after
images, which covers a `ConditionCheck` too: it writes nothing and is still
charged on the image it read. A same-token replay is charged against those same
images at 4KB read granularity.

A PartiQL `SELECT` served from an index is charged against that index's arm with
the table arm at zero, matching `Query` and `Scan`.

One read-side gap remains, and it is not specific to indexes: a PartiQL read with
no key condition is charged on the rows it returns, where DynamoDB charges a flat
figure for the scan regardless of how many rows it evaluated. An unqualified base
table scan diverges by the same margin as an index one.

---

## PartiQL Support

Supports `SELECT`, `INSERT`, `UPDATE`, `DELETE` with full WHERE clause support:

- **Comparisons:** `=` and `<>` on every attribute type; `<`, `>`, `<=`, `>=` and `BETWEEN` on the three DynamoDB orders (`S`, `N`, `B`), rejecting any other operand with `Incorrect operand type for operator or function` before the table is resolved. Sets compare without regard to member order, lists in order, maps on their key set; the same comparison serves condition expressions, so the two surfaces cannot drift
- **Range/membership:** `BETWEEN`, `IN`
- **Functions:** `EXISTS`, `NOT EXISTS`, `BEGINS_WITH`, `CONTAINS`
- **Existence:** `IS MISSING`, `IS NOT MISSING`
- **Logical:** `AND`, `OR`, `NOT`, parenthesised grouping up to 64 levels deep. `AND` binds tighter than `OR`, and a `NOT` over a group is applied by De Morgan, so `NOT (a=1 OR b=2)` selects what `a<>1 AND b<>2` does. The clause is flattened to an OR of ANDs internally; a clause whose flattened form exceeds 256 alternatives is rejected as too complex
- **Projections:** Nested dot-notation paths
- **Aggregates:** Not supported, matching DynamoDB: a `COUNT(...)` projection is rejected with DynamoDB's `Unexpected path component` message carrying the token's position (captured against eu-west-2)
- **Pagination:** `LIMIT` and `NextToken` on `SELECT`. `LIMIT` bounds the rows evaluated, as it does on Query and Scan, so a filtered page can come back short or empty and still carry a token. The statement and parameters must stay identical across pages; a token replayed with either changed is rejected with DynamoDB's `NextToken does not match request` message, and one that cannot be decoded at all with `Invalid NextToken` (both captured against eu-west-2)
- **Literals:** Set literals (`<< >>`), negative numbers, escaped quotes
- **Mutations:** `INSERT` (with IF NOT EXISTS, rejects duplicates), `UPDATE` (SET with expressions, REMOVE, supports `RETURNING`), `DELETE` (requires sort key, supports `RETURNING ALL OLD *`)
- **Transactions:** `ExecuteTransaction` with all-or-nothing semantics
- **Index qualifier:** `SELECT * FROM "table"."index"` is served from the named GSI or LSI. The read follows the index, so items the index does not hold are absent and a `KEYS_ONLY` or `INCLUDE` projection returns only what it projects. An unknown index name, a path of more than two components, an empty path component and a strongly consistent read of a GSI are each rejected with DynamoDB's own wording. A GSI rejects a projection naming an attribute it does not carry; either kind rejects a filter on one when the read is keyed on the index partition key, and matches nothing when it is not. `INSERT` rejects a qualifier at parse, `UPDATE` and `DELETE` reject it in execution. Captured against eu-west-2

  An index-qualified `SELECT` inside `ExecuteTransaction` is rejected, as it is on DynamoDB. One limit remains: an LSI serves a projection naming an unprojected attribute from the base table on DynamoDB and returns nothing here

Parameter placeholders (`?`) supported in all positions including nested list/map values.

**`RETURNING`:** honoured on `ExecuteStatement` (`DELETE ALL OLD *`, and `UPDATE` in all four `ALL`/`MODIFIED` × `OLD`/`NEW` variants, with `MODIFIED` excluding the key) and on `BatchExecuteStatement`; rejected inside `ExecuteTransaction` with a `ValidationException`. `DELETE` accepts only `RETURNING ALL OLD *` and rejects the other variants, matching DynamoDB.

**Batch and transaction shape:** `BatchExecuteStatement` and `ExecuteTransaction` reject a request that mixes reads with writes, and one that names the same item twice, reads included. Both raise a top-level `ValidationException` before any statement runs, each with its own message. A member that does not parse is reported against itself and does not stop the rest of a batch. Captured against eu-west-2.

**`ReturnConsumedCapacity`:** accepted on all three PartiQL surfaces. `BatchExecuteStatement` aggregates per table across the batch and charges a failed statement the write it attempted, sized on the row already stored.

**Batch member options:** `BatchStatementRequest` carries `ConsistentRead` and `ReturnValuesOnConditionCheckFailure`. `ConsistentRead` is per member and sets the rate that member's read is charged at, so a batch mixing the two sums both rates; it does not change which rows come back. `ReturnValuesOnConditionCheckFailure` is accepted and inert, matching DynamoDB: a member whose condition fails returns the same response whether it is `ALL_OLD`, `NONE` or absent, and never the item. A batch `SELECT` must name the table's primary key and may not name an index; either shape is rejected against that member while the rest of the batch runs.

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

DynamoDB Local fails a share of the suite that real DynamoDB passes, clustered in table- and index-name validation messages, tag operations, and validation ordering and error codes. For the current per-engine counts, broken down by tier, see the [live results](https://paritysuite.org).

### Capability advantages

| Capability | Notes |
|---|---|
| MCP server (34 tools, stdio + HTTP) | Exposes all DynamoDB operations as tools for coding agents |
| Embedded mode (direct Rust API) | `Database::memory()` - no HTTP, no serialisation overhead |
| Snapshots + auto-snapshot before destructive ops | Point-in-time save/restore for safe experimentation |
| OneTable data model integration | `--data-model` loads entity schemas for agent context |
| Anonymised import with rule-based anonymisation | Import DynamoDB exports with fake/mask/hash/redact rules |
| SQLCipher encryption at rest | `encryption` feature flag for encrypted databases |
| iOS/native embedding | No runtime dependencies - runs on platforms where Docker can't |
| Sub-millisecond startup, ~5 MB binary | vs ~2.5s and ~225 MB for DynamoDB Local |

---

## Known Remaining Limitations

- **Single-shard stream model** - DescribeStream returns a single shard; `ExclusiveStartShardId` and `Limit` accepted but ignored
- **Number arithmetic precision** - uses `rust_decimal` for arbitrary-precision arithmetic, which may have minor differences from DynamoDB's proprietary implementation at extreme edge cases
- **Transaction contention errors** - `TransactionConflictException` and `TransactionInProgressException` not emulated (concurrent transaction contention doesn't apply to single-process emulator)

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
