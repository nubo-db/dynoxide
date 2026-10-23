# Dynoxide DynamoDB Compatibility

Dynoxide is an embeddable DynamoDB emulator backed by SQLite. It is designed for local development, testing, and CI pipelines - not as a production DynamoDB replacement.

**What "not applicable" means:** Dynoxide does not emulate capacity management, throttling, global replication, backup infrastructure, or Kinesis integration. These features are meaningless for a local emulator and are marked as "not applicable" rather than "not implemented."

**Consistency model:** SQLite provides strong consistency. `ConsistentRead` is accepted but has no effect - all reads are strongly consistent.

> Behaviour validated by the [conformance suite](https://github.com/paritysuite/dynamodb-conformance). Pass rates move as the suite grows, so this page links to the [live results](https://paritysuite.org) rather than pinning a snapshot.

---

## Behaviour changes by version

Conformance fixes ship as minor releases, so behaviour moves within a major line. This is the cumulative record: if something answers differently after an upgrade, look here first. `docs/versioning.md` explains the rule and how to pin if you would rather behaviour held still.

### 1.4.0

- **Results change without an error.**
  - Every clause of an update reads the item as it was, in `UpdateItem` and
    PartiQL alike: `REMOVE l[0], l[1]` takes out both original elements, a
    PartiQL `SET s = n + 1` beside `SET n = 7` reads the old `n`, and an
    `ADD`, `DELETE` or `REMOVE` at an index a list did not reach leaves an
    element another clause appended alone.
  - PartiQL `SET zz.y = 1` no longer creates the missing `zz`.
  - A DELETE that empties a set held in a list element takes the element out at
    its original index, and appends past the end of a list land in index order.
  - A parallel `Scan` with `TotalSegments` above 4096 returns each item once,
    and one that deletes each page as it goes sees its whole segment.
- **Requests that used to be accepted are now rejected**, matching DynamoDB:
  - an update through a missing or wrong-typed parent;
  - PartiQL `REMOVE a, b` (one path per `REMOVE`) and overlapping PartiQL
    paths;
  - `Select` of `COUNT`, `ALL_ATTRIBUTES` or `ALL_PROJECTED_ATTRIBUTES` beside
    `AttributesToGet`, on `Query` and `Scan`;
  - a `Scan` `Segment` above 999999;
  - a `Limit` of 0 on `Database::scan`, `Database::query` and the MCP tools.
- **Requests that used to be refused are now accepted**: a `Limit`, `Segment` or
  `TotalSegments` past what a 32-bit integer holds, or with a fraction, is read
  as DynamoDB reads it, saturated and truncated.
- **Messages move.** `Scan` answers its member checks, attribute values and
  empty names as DynamoDB's newer validation layer does, which most regions run
  as of October 2026 (`us-east-1` still runs the older one). `Query` puts its
  `Select` and projection messages in the `1 validation error detected:`
  envelope. `SET` arithmetic and `list_append` give DynamoDB's operand errors.

### 1.1.0

- **`BatchGetItem` with an empty `RequestItems` map answers the standard validation envelope** (`Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1`) in place of a bespoke parameter-required sentence, following AWS. `BatchWriteItem` has not moved and still answers the bespoke sentence, so the two siblings differ.

### 1.0.0

A large correction pass. Full detail in `CHANGELOG.md`; the shapes that matter:

- **A PartiQL write can now modify data it previously did not.** Predicates on set, list, map, binary and null attributes compare by value instead of never matching; a negated comparison keeps rows its attribute is missing from; parenthesised grouping and bare `NOT` now parse. `UPDATE` and `DELETE` share their `WHERE` matcher with `SELECT`, so all three reach the write paths.
- **Requests that used to be accepted are now rejected**, matching DynamoDB: item-size ceilings on `UpdateItem` and the PartiQL write surfaces, `ReturnConsumedCapacity` validation, two `BatchExecuteStatement` and `ExecuteTransaction` request shapes, index-qualified PartiQL `SELECT` in several forms, and ordering comparisons against unorderable operands.
- **Consumed capacity figures move** across index writes, transactional surfaces, PartiQL reads and LSI base-table reads.
- **Results change without an error**: a PartiQL `SELECT` is served from the index its `FROM` names, and an LSI serves a projection naming an attribute it does not carry.

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
| Multi-attribute GSI keys (up to four HASH and four RANGE attributes) | Supported |
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

### Four things about vector search that will never match AWS

Everything else on this page is either the same as DynamoDB or a bug waiting to
be fixed. These four are neither. They come from how the two systems are built,
so no amount of work here will close them, and a conformance test for them would
have nothing to assert.

Each one can make a test pass locally and fail against AWS.

**1. Dynoxide finds the genuinely nearest vectors. AWS finds roughly the nearest
ones.**

Dynoxide compares your query against every vector in the index and returns the
closest. AWS builds an approximate index that skips most of those comparisons so
it stays fast on very large tables, and skipping comparisons means it can miss a
close match that was really there.

For the same data and the same query, AWS can return a slightly different set of
results from dynoxide. Dynoxide will not miss anything. AWS occasionally will.

*What to do:* do not write a test that pins the exact list of results and expect
it to pass against AWS. Assert the thing you actually care about, such as "the
item I put in comes back" or "I got five results". Also worth knowing: because
dynoxide checks every vector, a search costs time in proportion to how many
there are, so a local table with millions of vectors will be slow in a way AWS
would not be.

**2. When two results score identically, dynoxide always orders them the same
way. AWS does not.**

Dynoxide breaks a tie by primary key, so the same query over the same data gives
the same order every time. AWS makes no such promise. Three identical calls
against real DynamoDB returned three different orderings.

*What to do:* never depend on the order of results that tie. If you need a
stable order, sort them yourself after the search. A test that asserts "these
two came back in this order" will pass here and is not reliable against AWS.

**3. The index creation lifecycle has the same shape here, and nothing like the
same length.**

The phases, the statuses and the refusals all match. The clock does not.
Dynoxide runs the whole thing in about half a minute. AWS takes minutes, and how
many depends on how much data it has to index: a 25-item table took roughly
seventeen minutes. There is no single duration to copy, because AWS does not
have one.

*What to do:* never write `sleep(30)` and assume the index is ready. Poll for
readiness by retrying the search and treating the refusal as "not yet". That
works against both.

**4. `VectorSearchRequestBytes` is dynoxide's own number, not AWS's.**

AWS bills a search on whatever data it happened to read, and it does not
reproduce its own figure between identical calls: five identical searches over
one unchanged index reported 14214, 13903, 14214, 14214 and 14518. Dynoxide
reports a stable figure sized on the entries it scanned, using the same unit AWS
uses.

*What to do:* use it to compare one search against another within dynoxide,
where it is meaningful. Do not compare it to a figure from AWS, and do not
assert an exact value in a test that also runs against AWS. The write-side
figure, `VectorWriteRequestBytes`, is captured from real DynamoDB and does match.

A vector index added to a live table walks a creation lifecycle, the same shape
AWS walks but nothing like the same length. A vector index created as part of
`CreateTable` reports `ACTIVE` at once, which is what AWS does too.

An index added through `UpdateTable` passes through four phases:

| Phase | `TableStatus` | `IndexStatus` | `Backfilling` | `SearchVectors` | `Delete` of the index |
|---|---|---|---|---|---|
| Allocating | `UPDATING` | `CREATING` | `false` | `Cannot search backfilling vector index: <name>` | refused |
| Backfilling | `ACTIVE` | `CREATING` | `true` | the same refusal | accepted |
| Active, not searchable | `ACTIVE` | `ACTIVE` | absent | the same refusal | accepted |
| Searchable | `ACTIVE` | `ACTIVE` | absent | served | accepted |

The base table is `ACTIVE` for all but the first phase, and that phase is the
short one, so a table waiter looks like the right gate for a search long before
it is one.

Cancelling a create is not available immediately. While the index is
allocating, an `UpdateTable` that deletes it answers `ResourceInUseException`
with `Attempt to change a resource which is still in use: Index creation is in
resource allocation phase. Retry deletion during backfilling phase or when the
index is active.` The answer names the phase to wait for.

The table cannot be dropped while the index is creating, meaning for both of
those first two phases. `DeleteTable` answers `ResourceInUseException` with
`Attempt to change a resource which is still in use: Cannot delete table while
indexes are being created, updated, or deleted.`

The one online index limit is per table rather than per call, so a second index
cannot start creating while the first still is, whether the two arrive in one
`UpdateTable` or in separate ones: both answer `Subscriber limit exceeded: Only
1 online index can be created or deleted simultaneously per table`. The action
that does get through is a delete of the creating index itself, which is how you
cancel one.

When the index reaches `ACTIVE` the `Backfilling` field is not set to `false`,
it disappears. A wait condition of "`IndexStatus` is `ACTIVE` and `Backfilling`
is `false`" therefore never comes true, here or on AWS.

The sharper trap is the third row: `ACTIVE` arrives before the index will
answer. On AWS it arrives early by minutes, an index added to a 25-item table
taking roughly a quarter of an hour after `ACTIVE` before it stopped refusing
searches. Dynoxide reproduces the gap so a readiness check that polls for
`ACTIVE` and searches immediately fails here the way it fails against the real
thing. Search in a retry loop and treat the refusal as "not yet" rather than
trusting the status.

**The durations are not AWS's.** Dynoxide's windows are tens of seconds, chosen
to be observable rather than to match anything; AWS's are minutes, and vary with
the table. The shape and the ordering are what carry over, never the magnitude.
The backfill itself is synchronous here, so the data behind the index is
complete before the window opens; the wait reproduces the lifecycle, not the
work.

The behaviour above, and where AWS's own documentation contradicts it, is
written up in [what the DynamoDB vector search docs get
wrong](https://martinhicks.dev/articles/dynamodb-vector-search-docs-get-wrong).

Adding a GSI is still compressed: the new index is `ACTIVE` and queryable the
moment `UpdateTable` returns, and `Backfilling` is never reported for it. The
errors that only arise while a GSI is still filling are unreachable here for
that reason.

Vector search is exact brute-force KNN rather than an approximate index, which
is difference 1 above. At emulator and embedded scale that reaches a correct
answer faster than building an ANN structure would, and it removes the recall
question entirely: the top-k is the true top-k. It also means a search costs
time linear in the number of indexed entries, so a local table holding millions
of vectors is outside what this is built for.

Two scoring details worth knowing. Vectors are stored and compared at `f32`,
so a value written at full `f64` precision reads back through the index rounded,
while the base table keeps exactly what was written. And where several entries
tie on score, Dynoxide breaks the tie deterministically, which is difference 2
above.

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

The base table charges the vector the same way. On a table with a vector index,
an attribute an index reads costs its name plus `4 * dimensions` in a write's
units, in a read's units on every read path and in `TableSizeBytes`, whatever
numbers it holds, while the 400 KB item limit sizes it as the list of numbers it
is. Both rules are DynamoDB's, captured in eu-west-2 and us-east-1 at 1, 3, 6,
8, 256 and 1,024 dimensions; reads moved to the billed size between 9 and 22
October 2026. A GSI's or LSI's share of the write, and a read served from one,
is sized by the number rule even when the index projects the vector, so their
`IndexSizeBytes` is too. A vector
index's `ItemCount` and `IndexSizeBytes` count its entries and total their write
figure without the floor.

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
- **Logical:** `AND`, `OR`, `NOT`, parenthesised grouping up to 64 levels deep. `AND` binds tighter than `OR`, and a `NOT` over a group is applied by De Morgan, so `NOT (a=1 OR b=2)` selects what `a<>1 AND b<>2` does. The clause is flattened to an OR of ANDs internally, under three limits of Dynoxide's own: at most 256 alternatives, at most 4,096 condition slots in any AND product, and at most 16,384 slots across an alternation. A clause past any of them is rejected as too complex. DynamoDB evaluates a condition as a tree and runs clauses far larger than these (40 pairs of `(a = 1 OR a = 2)` run on eu-west-2), so this is a known difference
- **Projections:** Nested dot-notation paths
- **Aggregates:** Not supported, matching DynamoDB: a `COUNT(...)` projection is rejected with DynamoDB's `Unexpected path component` message carrying the token's position (captured against eu-west-2)
- **Pagination:** `LIMIT` and `NextToken` on `SELECT`. `LIMIT` bounds the rows evaluated, as it does on Query and Scan, so a filtered page can come back short or empty and still carry a token. The statement must stay identical character for character across pages, and so must the parameters, though `Limit` may change; a token replayed with any difference is rejected with DynamoDB's `NextToken does not match request` message, and one that cannot be decoded at all with `Invalid NextToken` (both captured against eu-west-2). A statement with a list or map parameter is never resumed, even with the same parameter sent again (captured against eu-west-2 and us-east-1)
- **Literals:** Set literals (`<< >>`), negative numbers, escaped quotes
- **Mutations:** `INSERT` (with IF NOT EXISTS, rejects duplicates), `UPDATE` (SET with expressions, REMOVE, supports `RETURNING`), `DELETE` (requires sort key, supports `RETURNING ALL OLD *`)
- **Transactions:** `ExecuteTransaction` with all-or-nothing semantics. A cancelled transaction lists a reason for every statement that failed, not only the first. A key of the wrong type on any statement is reported alone, with `None` for every other statement. A `DELETE` with a condition, of an item that isn't there, does nothing when the transaction commits and is reported as a failed condition when it is cancelled. Captured against eu-west-2 and us-east-1
- **Index qualifier:** `SELECT * FROM "table"."index"` is served from the named GSI or LSI. The read follows the index, so items the index does not hold are absent and a `KEYS_ONLY` or `INCLUDE` projection returns only what it projects. An unknown index name, a path of more than two components, an empty path component and a strongly consistent read of a GSI are each rejected with DynamoDB's own wording. A GSI rejects a projection naming an attribute it does not carry; either kind rejects a filter on one when the read is keyed on the index partition key, and matches nothing when it is not. `INSERT` rejects a qualifier at parse, `UPDATE` and `DELETE` reject it in execution. Captured against eu-west-2

  An index-qualified `SELECT` inside `ExecuteTransaction` is rejected, as it is on DynamoDB. One limit remains: an LSI serves a projection naming an unprojected attribute from the base table on DynamoDB and returns nothing here

Parameter placeholders (`?`) supported in all positions including nested list/map values.

**`RETURNING`:** honoured on `ExecuteStatement` (`DELETE ALL OLD *`, and `UPDATE` in all four `ALL`/`MODIFIED` × `OLD`/`NEW` variants, with `MODIFIED` excluding the key) and on `BatchExecuteStatement`; rejected inside `ExecuteTransaction` with a `ValidationException`. `DELETE` accepts only `RETURNING ALL OLD *` and rejects the other variants, matching DynamoDB.

**Batch and transaction shape:** `BatchExecuteStatement` and `ExecuteTransaction` reject a request that mixes reads with writes, and one that names the same item twice, reads included. Both raise a top-level `ValidationException` before any statement runs, each with its own message. A member that does not parse is reported against itself and does not stop the rest of a batch. Captured against eu-west-2.

**`ReturnConsumedCapacity`:** accepted on all three PartiQL surfaces. `BatchExecuteStatement` aggregates per table across the batch and charges a failed statement the write it attempted, sized on the row already stored.

**Batch member options:** `BatchStatementRequest` carries `ConsistentRead` and `ReturnValuesOnConditionCheckFailure`. `ConsistentRead` is per member and sets the rate that member's read is charged at, so a batch mixing the two sums both rates; it does not change which rows come back. `ReturnValuesOnConditionCheckFailure` set to `ALL_OLD` puts the item a member's `UPDATE` or `DELETE` failed its condition on on that member's `Error`, as it does on an `ExecuteTransaction` statement's cancellation reason and on `ExecuteStatement`'s `ConditionalCheckFailedException`; `NONE`, a missing item and a duplicate `INSERT` carry none. Captured against eu-west-2 and us-east-1. A batch `SELECT` must name the table's primary key and may not name an index; either shape is rejected against that member while the rest of the batch runs.

---

## Data Validation

| Validation | Status |
|------------|--------|
| Empty string rejection | Enforced on all write paths |
| Empty set rejection | Enforced on all write paths |
| Number precision (38 digits, ±1E+126 range) | Enforced on all write paths |
| Set deduplication (SS/NS/BS) | Enforced on all write paths |
| 400KB item size limit | Enforced. A standalone `UpdateItem` is also held to a charge worked out from its request, as DynamoDB holds it; `TransactWriteItems` updates and PartiQL `UPDATE` are measured by the finished item |
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
| MCP server (35 tools, stdio + HTTP) | Exposes all DynamoDB operations as tools for coding agents |
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
- **Which of two PartiQL refusals is reported** - when one statement holds both an operand of the wrong type for its operator (such as `size(s) < true`) and something refused as the statement is read, Dynoxide reports the second and DynamoDB reports the operand's type
- **Map order in a refused request's echo** - when a request too long to accept is echoed back in its error (`BatchWriteItem` over 25 requests, a transaction over 100 actions), each map lists its entries in the order of the Java hash map DynamoDB reads it into, sized to the map. Two entries that land in the same bucket are listed in the order they were sent on DynamoDB, and in name order on Dynoxide, which no longer has the order they were sent in once the request is read

### Where AWS answers the same request differently

These three come from inside AWS, so Dynoxide picks one answer and keeps to it.
Captured in eu-west-2 and us-east-1 on 2026-10-09.

**1. Scan order across partitions.** AWS walks partitions in the order of an
internal hash of the partition key: the same six items came back with partition
`b` ahead of `a` in both regions. Dynoxide walks them in key order. Within a
partition both return sort key order.

*What to do:* never depend on which partition a Scan reaches first.

**2. `LastEvaluatedKey` on a page that ends at the last matching item.** When a
Query or Scan fills its `Limit` with the last item there is, AWS sometimes
returns a `LastEvaluatedKey` and sometimes does not. The same request answered
one way in eu-west-2 and the other in us-east-1, and two requests that differed
only in direction or `Select` answered differently in one region. Dynoxide
always returns one when a page fills its `Limit`, so the next request comes back
empty, which AWS also does.

*What to do:* page until `LastEvaluatedKey` is absent, and treat an empty page
as the end.

**3. How a refused key schema is printed.** A request-model error that quotes a
key schema, such as a table key with three attributes, usually lists each
element as `KeySchemaElement(attributeName=pk, keyType=HASH)`. Some AWS hosts
print `com.amazonaws.dynamodb.v20120810.KeySchemaElement@ad28e71d` instead; both
forms came back from eu-west-2 within the same hour. Dynoxide prints the first.

*What to do:* match the constraint text (`Member must have length less than or
equal to 2`), not the list.

### Legacy Pre-2015 API Parameters

The legacy parameters that came before the expression API are implemented:

| Parameter | Supported | Notes |
|-----------|-----------|-------|
| `AttributeUpdates` (UpdateItem) | Yes | `PUT`, `ADD` and `DELETE` actions |
| `Expected` (PutItem, UpdateItem, DeleteItem) | Yes | `Value` and `Exists`, or a `ComparisonOperator` with `AttributeValueList` |
| `QueryFilter` (Query) / `ScanFilter` (Scan) | Yes | Every comparison operator: `EQ`, `NE`, `LE`, `LT`, `GE`, `GT`, `NULL`, `NOT_NULL`, `CONTAINS`, `NOT_CONTAINS`, `BEGINS_WITH`, `IN`, `BETWEEN` |
| `KeyConditions` (Query) | Yes | `EQ` on the partition key; `EQ`, `LE`, `LT`, `GE`, `GT`, `BEGINS_WITH` or `BETWEEN` on the sort key |
| `AttributesToGet` (GetItem, BatchGetItem, Query, Scan) | Yes | Returns the named top-level attributes |
| `ConditionalOperator` | Yes | `AND` or `OR` across the conditions in `Expected`, `QueryFilter` or `ScanFilter` |

A request that mixes a legacy parameter with its expression counterpart, such as `Expected` with `ConditionExpression`, is refused, as DynamoDB refuses it. The expression API (`UpdateExpression`, `ConditionExpression`, `FilterExpression`, `KeyConditionExpression`, `ProjectionExpression`) covers more, such as nested paths and functions, and is the one to use for new code.

---

## Data Types

All 10 DynamoDB types fully supported: `S`, `N`, `B`, `BOOL`, `NULL`, `SS`, `NS`, `BS`, `L`, `M`.

---

## Error Codes

Dynoxide returns DynamoDB-compatible error codes with the `com.amazonaws.dynamodb.v20120810#` prefix:

`ResourceNotFoundException` · `ResourceInUseException` · `ValidationException` · `ConditionalCheckFailedException` (with optional Item) · `TransactionCanceledException` · `ItemCollectionSizeLimitExceededException` · `ProvisionedThroughputExceededException` · `LimitExceededException` · `DuplicateItemException` · `InternalServerError`
