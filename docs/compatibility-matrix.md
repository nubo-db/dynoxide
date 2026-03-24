# Dynoxide DynamoDB Compatibility Matrix

> **Original audit date:** 2026-03-14 · **Commit:** `45bd46c`
> **Updated:** 2026-03-24 · Reflects all fixes from compatibility phases 1–5, correctness audit, and conformance suite (526 tests, 100% pass rate).
> **Methodology:** Every "supported" claim cites a specific file and line number. Parameters are classified as **Implemented** (functionally used), **Accepted but ignored** (deserialised but not acted upon), or **Not implemented**.

---

## 1. API Operation Coverage

### Item Operations

| Operation | Status | Handler Location | Notes | DDB Local |
|-----------|--------|-----------------|-------|-----------|
| PutItem | Supported | `src/actions/put_item.rs:38` | Full expression support, GSI/LSI maintenance, streams, empty string/number precision/set dedup validation | Partial — ItemCollectionMetrics returns null |
| GetItem | Supported | `src/actions/get_item.rs:32` | Projection, ConsumedCapacity | Full |
| UpdateItem | Supported | `src/actions/update_item.rs:40` | Full update expression support, key attribute protection, empty string/number precision/set dedup validation | Partial — ItemCollectionMetrics returns null |
| DeleteItem | Supported | `src/actions/delete_item.rs:38` | Condition checks, GSI/LSI cleanup, streams | Partial — ItemCollectionMetrics returns null |
| BatchWriteItem | Supported | `src/actions/batch_write_item.rs:46` | 25-item limit enforced, 16MB limit, duplicate key detection, ItemCollectionMetrics | Partial — ItemCollectionMetrics returns null |
| BatchGetItem | Supported | `src/actions/batch_get_item.rs:39` | 100-key limit, 16MB limit, UnprocessedKeys preserves projection/consistent settings | Full |
| TransactWriteItems | Supported | `src/actions/transact_write_items.rs:114` | 100-item limit, idempotency tokens, all-or-nothing, ItemCollectionMetrics field | Full |
| TransactGetItems | Supported | `src/actions/transact_get_items.rs:49` | 100-item limit, per-table ConsumedCapacity | Full |

### Query & Scan

| Operation | Status | Handler Location | Notes | DDB Local |
|-----------|--------|-----------------|-------|-----------|
| Query | Supported | `src/actions/query.rs:60` | Full KeyCondition/Filter/Projection, GSI, LSI routing, pagination, correct 1MB limit (counts all scanned items), correct COUNT semantics | Full |
| Scan | Supported | `src/actions/scan.rs:56` | Parallel scan (Segment/TotalSegments) with hash-based filtering, LSI routing, correct 1MB limit | Full — but ignores Segment/TotalSegments |

DynamoDB Local ignores `Segment` and `TotalSegments` for parallel scan (returns full table for each segment). AWS documentation confirms this limitation.

### Table Management

| Operation | Status | Handler Location | Notes | DDB Local |
|-----------|--------|-----------------|-------|-----------|
| CreateTable | Supported | `src/actions/create_table.rs:42` | GSIs, LSIs, streams, provisioned throughput, SSESpecification, TableClass, Tags, DeletionProtectionEnabled | Full |
| DeleteTable | Supported | `src/actions/delete_table.rs:19` | GSI/LSI cleanup, respects DeletionProtectionEnabled | Full |
| DescribeTable | Supported | `src/actions/describe_table.rs:18` | Item count, table size, full metadata including LSI definitions | Full |
| UpdateTable | Supported | `src/actions/update_table.rs:60` | GSI create/delete with backfill, stream spec | Full |
| ListTables | Supported | `src/actions/list_tables.rs:24` | Pagination with ExclusiveStartTableName | Full |

### TTL

| Operation | Status | Handler Location | Notes | DDB Local |
|-----------|--------|-----------------|-------|-----------|
| UpdateTimeToLive | Supported | `src/actions/update_time_to_live.rs:27` | Enables/disables TTL per table | Full |
| DescribeTimeToLive | Supported | `src/actions/describe_time_to_live.rs:25` | Returns TTL config | Full |

### Streams

| Operation | Status | Handler Location | Notes | DDB Local |
|-----------|--------|-----------------|-------|-----------|
| ListStreams | Supported | `src/actions/list_streams.rs:37` | Filters by table, pagination | Full |
| DescribeStream | Partial | `src/actions/describe_stream.rs:62` | Single-shard model; `ExclusiveStartShardId` and `Limit` accepted but ignored | Full (also single-shard) |
| GetShardIterator | Supported | `src/actions/get_shard_iterator.rs:49` | TRIM_HORIZON, LATEST, AT/AFTER_SEQUENCE_NUMBER | Full |
| GetRecords | Supported | `src/actions/get_records.rs:66` | 1000-record cap, next iterator | Full |

### Tags

| Operation | Status | Handler Location | Notes | DDB Local |
|-----------|--------|-----------------|-------|-----------|
| TagResource | Supported | `src/actions/tag_resource.rs:18` | ARN-based lookup | Not supported |
| UntagResource | Supported | `src/actions/untag_resource.rs:17` | ARN-based lookup | Not supported |
| ListTagsOfResource | Supported | `src/actions/list_tags_of_resource.rs:19` | ARN-based lookup | Not supported |

DynamoDB Local returns `UnknownOperationException` for all tag operations.

### PartiQL

| Operation | Status | Handler Location | Notes | DDB Local |
|-----------|--------|-----------------|-------|-----------|
| ExecuteStatement | Supported | `src/actions/execute_statement.rs:21` | SELECT, INSERT, UPDATE, DELETE; full WHERE support including BETWEEN, IN, CONTAINS, IS MISSING, OR; nested paths; LIMIT; COUNT(*); set literals | Partial — wrong error code for duplicate INSERT |
| BatchExecuteStatement | Supported | `src/actions/batch_execute_statement.rs:43` | 25-statement limit | Full |
| ExecuteTransaction | Supported | `src/actions/execute_transaction.rs` | PartiQL transactional batch with all-or-nothing semantics | Full |

### Not Implemented — Not Applicable for Local Emulator

| Operation | Category | Notes |
|-----------|----------|-------|
| CreateBackup | Backup/Restore | Local-only; snapshots available via `vacuum_into()` |
| DeleteBackup | Backup/Restore | — |
| DescribeBackup | Backup/Restore | — |
| ListBackups | Backup/Restore | — |
| RestoreTableFromBackup | Backup/Restore | — |
| RestoreTableToPointInTime | Backup/Restore | — |
| DescribeContinuousBackups | Backup/Restore | — |
| UpdateContinuousBackups | Backup/Restore | — |
| CreateGlobalTable | Global Tables | Single-region emulator |
| DescribeGlobalTable | Global Tables | — |
| ListGlobalTables | Global Tables | — |
| UpdateGlobalTable | Global Tables | — |
| DescribeGlobalTableSettings | Global Tables | — |
| UpdateGlobalTableSettings | Global Tables | — |
| EnableKinesisStreamingDestination | Kinesis | No Kinesis integration |
| DisableKinesisStreamingDestination | Kinesis | — |
| DescribeKinesisStreamingDestination | Kinesis | — |
| ExportTableToPointInTime | Import/Export | — |
| DescribeExport | Import/Export | — |
| ListExports | Import/Export | — |
| ImportTable | Import/Export | Dynoxide has its own `import_items()` API |
| DescribeImport | Import/Export | — |
| ListImports | Import/Export | — |
| DescribeContributorInsights | Insights | — |
| ListContributorInsights | Insights | — |
| UpdateContributorInsights | Insights | — |
| GetResourcePolicy | Resource Policy | — |
| PutResourcePolicy | Resource Policy | — |
| DeleteResourcePolicy | Resource Policy | — |
| DescribeEndpoints | Endpoints | — |
| DescribeLimits | Limits | — |
| DescribeReservedCapacity | Capacity | No capacity model |
| DescribeReservedCapacityOfferings | Capacity | — |
| PurchaseReservedCapacityOfferings | Capacity | — |
| DescribeTableReplicaAutoScaling | Replicas | Single-region |
| UpdateTableReplicaAutoScaling | Replicas | — |

---

## 2. Per-Operation Parameter Audit

### PutItem (`src/actions/put_item.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TableName` | Implemented | put_item.rs:39,111 |
| `Item` | Implemented | put_item.rs:43,46,96,115,127 — validates empty strings, number precision, set deduplication |
| `ConditionExpression` | Implemented | put_item.rs:58,65 — condition check and write in single SQLite transaction |
| `ExpressionAttributeNames` | Implemented | put_item.rs:71 — unused entries rejected |
| `ExpressionAttributeValues` | Implemented | put_item.rs:72 — unused entries rejected |
| `ReturnValues` | Implemented | put_item.rs:131-135 — validates only NONE/ALL_OLD accepted |
| `ReturnConsumedCapacity` | Implemented | put_item.rs:145-148 |
| `ReturnValuesOnConditionCheckFailure` | Implemented | put_item.rs:76 |
| `ReturnItemCollectionMetrics` | Implemented | Returns ItemCollectionMetrics when SIZE requested |

### GetItem (`src/actions/get_item.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TableName` | Implemented | get_item.rs:33 |
| `Key` | Implemented | get_item.rs:37,40 |
| `ProjectionExpression` | Implemented | get_item.rs:48-62 |
| `ExpressionAttributeNames` | Implemented | get_item.rs:59 — unused entries rejected |
| `ConsistentRead` | Accepted but ignored | SQLite always consistent (intentional) |
| `ReturnConsumedCapacity` | Implemented | get_item.rs:70-74 |

### UpdateItem (`src/actions/update_item.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TableName` | Implemented | update_item.rs:41 |
| `Key` | Implemented | update_item.rs:45,48,59 |
| `UpdateExpression` | Implemented | update_item.rs:95,171,185 — rejects REMOVE/ADD/DELETE on key attributes; validates empty strings, number precision, set deduplication on results |
| `ConditionExpression` | Implemented | update_item.rs:65,66 — condition check and write in single SQLite transaction |
| `ExpressionAttributeNames` | Implemented | update_item.rs:71,103,177,194 — unused entries rejected |
| `ExpressionAttributeValues` | Implemented | update_item.rs:72,125,177,194 — unused entries rejected |
| `ReturnValues` | Implemented | update_item.rs:165-201 (NONE, ALL_OLD, ALL_NEW, UPDATED_OLD, UPDATED_NEW) |
| `ReturnConsumedCapacity` | Implemented | update_item.rs:203-207 |
| `ReturnValuesOnConditionCheckFailure` | Implemented | update_item.rs:76 |
| `ReturnItemCollectionMetrics` | Implemented | Returns ItemCollectionMetrics when SIZE requested |

### DeleteItem (`src/actions/delete_item.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TableName` | Implemented | delete_item.rs:39 |
| `Key` | Implemented | delete_item.rs:43,46 |
| `ConditionExpression` | Implemented | delete_item.rs:49,56 — condition check and write in single SQLite transaction |
| `ExpressionAttributeNames` | Implemented | delete_item.rs:61 — unused entries rejected |
| `ExpressionAttributeValues` | Implemented | delete_item.rs:62 — unused entries rejected |
| `ReturnValues` | Implemented | delete_item.rs:95-99 — validates only NONE/ALL_OLD accepted |
| `ReturnConsumedCapacity` | Implemented | delete_item.rs:112-116 |
| `ReturnValuesOnConditionCheckFailure` | Implemented | delete_item.rs:66 |
| `ReturnItemCollectionMetrics` | Implemented | Returns ItemCollectionMetrics when SIZE requested |

### Query (`src/actions/query.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TableName` | Implemented | query.rs:61 |
| `KeyConditionExpression` | Implemented | query.rs:76-80 |
| `FilterExpression` | Implemented | query.rs:179-184 |
| `ProjectionExpression` | Implemented | query.rs:187-192 |
| `ExpressionAttributeNames` | Implemented | query.rs:78,236,259 — unused entries rejected |
| `ExpressionAttributeValues` | Implemented | query.rs:92,237,259 — unused entries rejected |
| `IndexName` | Implemented | query.rs:65-73,155-176,207 — routes to GSI or LSI tables |
| `ScanIndexForward` | Implemented | query.rs:162,172 |
| `Limit` | Implemented | query.rs:151,278 |
| `ExclusiveStartKey` | Implemented | query.rs:134-142 |
| `Select` | Implemented | query.rs:195-199 — COUNT returns filtered count correctly |
| `ConsistentRead` | Accepted but ignored | SQLite always consistent (intentional) |
| `ReturnConsumedCapacity` | Implemented | query.rs:287-291 — includes per-GSI breakdown in INDEXES mode |

### Scan (`src/actions/scan.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TableName` | Implemented | scan.rs:57 |
| `FilterExpression` | Implemented | scan.rs:103-108 |
| `ProjectionExpression` | Implemented | scan.rs:111-116 |
| `ExpressionAttributeNames` | Implemented | scan.rs:159,182 — unused entries rejected |
| `ExpressionAttributeValues` | Implemented | scan.rs:160,182 — unused entries rejected |
| `IndexName` | Implemented | scan.rs:61-69,85-92,130 — routes to GSI or LSI tables |
| `Limit` | Implemented | scan.rs:89,96,200 |
| `ExclusiveStartKey` | Implemented | scan.rs:72-82 |
| `Select` | Implemented | scan.rs:119-123 |
| `Segment` | Implemented | Hash-based segment assignment |
| `TotalSegments` | Implemented | Items filtered by `hash(pk) % TotalSegments` |
| `ConsistentRead` | Accepted but ignored | SQLite always consistent (intentional) |
| `ReturnConsumedCapacity` | Implemented | scan.rs:219-223 |

### BatchWriteItem (`src/actions/batch_write_item.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `RequestItems` | Implemented | batch_write_item.rs:53,81 |
| `ReturnConsumedCapacity` | Implemented | batch_write_item.rs:146-174 |
| `ReturnItemCollectionMetrics` | Implemented | Returns ItemCollectionMetrics when SIZE requested |
| 25-item limit | Enforced | batch_write_item.rs:53-58 |
| 16MB aggregate limit | Enforced | batch_write_item.rs:61-79 |
| Duplicate key detection | Enforced | Rejects requests with duplicate table+key combinations |
| UnprocessedItems | Returned (always empty) | batch_write_item.rs response |

### BatchGetItem (`src/actions/batch_get_item.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `RequestItems` | Implemented | batch_get_item.rs:41,55 |
| `ReturnConsumedCapacity` | Implemented | batch_get_item.rs:127-145 |
| Per-table `Keys` | Implemented | batch_get_item.rs:75,82 |
| Per-table `ProjectionExpression` | Implemented | batch_get_item.rs:60-65 |
| Per-table `ExpressionAttributeNames` | Implemented | batch_get_item.rs:103 |
| Per-table `ConsistentRead` | Accepted but ignored | In struct, not used |
| 100-key limit | Enforced | batch_get_item.rs:41-46 |
| 16MB response limit | Enforced | batch_get_item.rs:48 |
| UnprocessedKeys | Returned when truncated | batch_get_item.rs:73-94 — preserves projection and ConsistentRead settings |

### TransactWriteItems (`src/actions/transact_write_items.rs` + `src/lib.rs:393-434`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TransactItems` | Implemented | transact_write_items.rs:118,149,160 |
| `ClientRequestToken` | Implemented | Idempotency enforced — `IdempotentParameterMismatchException` returned for mismatched requests with same token |
| `ReturnConsumedCapacity` | Implemented | transact_write_items.rs:155-177 |
| `ReturnItemCollectionMetrics` | Implemented | Field present in response (value currently None) |
| 100-item limit | Enforced | transact_write_items.rs:121-125 |
| 4MB aggregate size limit | Enforced | transact_write_items.rs:139-144 — correctly estimates update sizes |
| Duplicate item target detection | Enforced | transact_write_items.rs:128-136 |
| Per-action `ConditionExpression` | Implemented | Per action type structs |
| Per-action `ReturnValuesOnConditionCheckFailure` | Implemented | Per action type structs |

### TransactGetItems (`src/actions/transact_get_items.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TransactItems` | Implemented | transact_get_items.rs:54,62,107 |
| `ReturnConsumedCapacity` | Implemented | transact_get_items.rs:101-126 |
| Per-get `ProjectionExpression` | Implemented | transact_get_items.rs:76-91 |
| Per-get `ExpressionAttributeNames` | Implemented | transact_get_items.rs:88 |
| 100-item limit | Enforced | transact_get_items.rs:54-58 |

### CreateTable (`src/actions/create_table.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TableName` | Implemented | create_table.rs:44,59,89 |
| `KeySchema` | Implemented | create_table.rs:45,71 |
| `AttributeDefinitions` | Implemented | create_table.rs:46,73 |
| `GlobalSecondaryIndexes` | Implemented | create_table.rs:52-56,102-106 |
| `LocalSecondaryIndexes` | Implemented | Creates per-LSI SQLite tables sharing partition key |
| `BillingMode` | Accepted but ignored | In struct, not used |
| `ProvisionedThroughput` | Implemented | create_table.rs:81-86 (stored in metadata) |
| `StreamSpecification` | Implemented | create_table.rs:109-118 |
| `SSESpecification` | Accepted but ignored | Accepted in request, stored but not enforced |
| `TableClass` | Accepted but ignored | Accepted in request, stored but not enforced |
| `Tags` | Implemented | Calls tag_resource internally |
| `DeletionProtectionEnabled` | Implemented | Prevents DeleteTable when enabled |

### UpdateTable (`src/actions/update_table.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `TableName` | Implemented | update_table.rs:61,137,164,192 |
| `AttributeDefinitions` | Implemented | update_table.rs:79-82 |
| `GlobalSecondaryIndexUpdates` | Implemented | update_table.rs:88-150 (Create + Delete) |
| `StreamSpecification` | Implemented | update_table.rs:167-178 |
| `BillingMode` | Implemented | Accepted in request, stored in metadata |
| `ProvisionedThroughput` | Implemented | Accepted in request, stored in metadata |
| `SSESpecification` | Not in struct | — |
| `TableClass` | Not in struct | — |
| `DeletionProtectionEnabled` | Implemented | Prevents DeleteTable when enabled |

### ListTables (`src/actions/list_tables.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `ExclusiveStartTableName` | Implemented | list_tables.rs:30-37 |
| `Limit` | Implemented | list_tables.rs:25,41 (clamped 1-100) |

### ExecuteStatement (`src/actions/execute_statement.rs`)

| Parameter | Status | Location |
|-----------|--------|----------|
| `Statement` | Implemented | Full PartiQL parsing and execution |
| `Parameters` | Implemented | `?` placeholders in all positions including nested literals |
| `Limit` | Implemented | Controls max items returned |
| `NextToken` | Implemented | Pagination support |
| `ConsistentRead` | Accepted but ignored | SQLite always consistent |
| `ReturnConsumedCapacity` | Implemented | — |

---

## 3. Expression Language Coverage

### Condition / Filter Expressions (`src/expressions/condition.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| `=` | Implemented | condition.rs:301-308 |
| `<>` | Implemented | condition.rs:310-317 |
| `<` | Implemented | condition.rs:319-326 |
| `<=` | Implemented | condition.rs:328-335 |
| `>` | Implemented | condition.rs:337-344 |
| `>=` | Implemented | condition.rs:346-353 |
| `BETWEEN x AND y` | Implemented | condition.rs:355-364 |
| `IN (val1, val2, ...)` | Implemented | condition.rs:366-378 |
| `AND` | Implemented | condition.rs:208-215 |
| `OR` | Implemented | condition.rs:208-215 |
| `NOT` | Implemented | condition.rs:228-234 |
| Parenthetical grouping | Implemented | condition.rs:238-243 |
| `attribute_exists(path)` | Implemented | condition.rs:250-255, eval 134-137 |
| `attribute_not_exists(path)` | Implemented | condition.rs:257-262, eval 139-141 |
| `attribute_type(path, :type)` | Implemented | condition.rs:264-271, eval 144-151 |
| `begins_with(path, substr)` | Implemented | condition.rs:273-280, eval 154-161 |
| `contains(path, operand)` | Implemented | condition.rs:282-289, eval 164-182 |
| `size(path)` | Implemented | condition.rs:387-393, eval 480-500 — correctly rejects N/BOOL/NULL |
| `size()` as operand in comparisons | Implemented | Returns `AttributeValue::N`, usable in any comparison |

### Key Condition Expressions (`src/expressions/key_condition.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| Partition key `=` only | Implemented | key_condition.rs:261-263, enforced 59-70 |
| Sort key `=` | Implemented | key_condition.rs:261-263 |
| Sort key `<`, `<=`, `>`, `>=` | Implemented | key_condition.rs:265-279 |
| Sort key `BETWEEN` | Implemented | key_condition.rs:281-285 |
| Sort key `begins_with` | Implemented | key_condition.rs:243-252 — SQL LIKE wildcards properly escaped |

### Projection Expressions (`src/expressions/projection.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| Top-level attributes | Implemented | projection.rs:74-115 |
| Nested dot notation (`a.b.c`) | Implemented | projection.rs:86-97 |
| List indexing (`a[0]`, `a[0].b`) | Implemented | projection.rs:99-109 |
| Key attributes always included | Implemented | projection.rs:55-60 |
| ExpressionAttributeNames in projection | Implemented | projection.rs:125-126 |

### Update Expressions (`src/expressions/update.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| `SET path = value` | Implemented | update.rs:365-369 |
| `SET path = path + value` (addition) | Implemented | update.rs:376-379, eval 176-186 — uses arbitrary-precision decimal |
| `SET path = path - value` (subtraction) | Implemented | update.rs:381-384, eval 188-200 — uses arbitrary-precision decimal |
| `SET path = if_not_exists(path, default)` | Implemented | update.rs:395-402, eval 217-222 |
| `SET path = list_append(list, list)` | Implemented | update.rs:404-411, eval 224-233 |
| Multiple SET actions | Implemented | update.rs:356-362 |
| `REMOVE path` (top-level and nested) | Implemented | update.rs:434-443 |
| `ADD` numeric (create or increment) | Implemented | update.rs:251-262 — uses arbitrary-precision decimal |
| `ADD` string set union | Implemented | update.rs:266-272 |
| `ADD` number set union | Implemented | update.rs:277-284 |
| `ADD` binary set union | Implemented | update.rs:288-295 |
| `DELETE` string set subtraction | Implemented | update.rs:314-323 |
| `DELETE` number set subtraction | Implemented | update.rs:325-334 |
| `DELETE` binary set subtraction | Implemented | update.rs:336-345 |
| Clause uniqueness (SET/REMOVE/ADD/DELETE once each) | Implemented | update.rs:79-105 |
| Key attribute protection | Enforced | REMOVE/ADD/DELETE on key attributes rejected |

### Expression Attribute Names/Values (`src/expressions/mod.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| `#name` substitution | Implemented | mod.rs:19-37 |
| `:value` substitution | Implemented | mod.rs:40-54 |
| Path resolution (nested, indexed) | Implemented | mod.rs:57-91 |
| `set_path()` (creates intermediates) | Implemented | mod.rs:95-124 |
| `remove_path()` | Implemented | mod.rs:186-214 |
| Unused name/value validation | Implemented | Rejects requests where ExpressionAttributeNames or ExpressionAttributeValues contain entries not referenced in the expression |

---

## 4. PartiQL Coverage (`src/partiql/`)

| Feature | Status | Location |
|---------|--------|----------|
| `SELECT` | Implemented | parser.rs, executor.rs — including nested path projections, correct structure preservation |
| `SELECT COUNT(*)` | Implemented | Returns count of matching items |
| `INSERT` | Implemented | parser.rs, executor.rs — rejects duplicate items (DuplicateItemException), supports IF NOT EXISTS |
| `UPDATE` | Implemented | parser.rs, executor.rs — supports SET with expressions (`count + 1`, `list_append`), REMOVE clause |
| `DELETE` | Implemented | parser.rs, executor.rs — correctly requires sort key when table has composite key |
| WHERE `=`, `<>`, `<`, `>`, `<=`, `>=` | Implemented | parser.rs:258-266 |
| WHERE `BETWEEN` | Implemented | Full range comparisons |
| WHERE `IN` | Implemented | Value list membership |
| WHERE `CONTAINS` | Implemented | String/set containment |
| WHERE `IS MISSING` / `IS NOT MISSING` | Implemented | Attribute existence checks |
| WHERE `OR` | Implemented | Logical disjunction with correct precedence |
| WHERE parenthesised grouping | Implemented | Arbitrary nesting of conditions |
| WHERE `EXISTS` | Implemented | parser.rs, executor.rs |
| WHERE `NOT EXISTS` | Implemented | parser.rs, executor.rs |
| WHERE `BEGINS_WITH` | Implemented | parser.rs, executor.rs |
| Nested path access in projections | Implemented | Dot-notation paths in SELECT clauses with correct structure |
| Nested path access in WHERE | Implemented | Multi-segment path support in all WHERE operators |
| `LIMIT` | Implemented | Controls max items returned from SELECT |
| `NextToken` pagination | Implemented | ExecuteStatement supports continuation tokens |
| Set literal syntax (`<< >>`) | Implemented | Typed set literals in expressions |
| `RETURNING` clause | Not implemented | — |
| Parameter placeholders (`?`) | Implemented | All positions including nested list/map values |
| Item literal parsing (maps, lists, scalars) | Implemented | Including negative numbers (correct tokenisation) |
| Escaped single quotes (`''`) | Implemented | Correct string escaping |
| Double-quoted identifiers (`""`) | Implemented | Correct identifier escaping |
| Unknown characters in tokeniser | Handled | Returns error instead of silently skipping |

---

## 5. Data Type Handling (`src/types.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| S (String) | Implemented | types.rs:17 |
| N (Number as string) | Implemented | types.rs:19 |
| B (Binary, base64 serde) | Implemented | types.rs:21, 161-163, 205-210 |
| BOOL | Implemented | types.rs:23 |
| NULL | Implemented | types.rs:25 |
| SS (String Set) | Implemented | types.rs:27 |
| NS (Number Set) | Implemented | types.rs:29 |
| BS (Binary Set) | Implemented | types.rs:31, 168-170, 216-224 |
| L (List) | Implemented | types.rs:33 |
| M (Map) | Implemented | types.rs:35 |
| Key type validation (S, N, B only) | Implemented | validation.rs:73-89, types.rs:121-128 |
| Item size calculation | Implemented | types.rs:44-77 (follows DynamoDB rules) |
| Number size: (significant digits / 2) + 1 | Implemented | types.rs:48-52 |
| List/Map overhead in size calc | Implemented | types.rs:65-76 |
| 400KB item size limit | Implemented | types.rs:407 (`MAX_ITEM_SIZE`) |
| Number sort key normalisation | Implemented | types.rs:257-352 (scientific notation, negatives) |
| Set deduplication | Enforced | SS/NS/BS deduplicated on all write paths |
| Empty string rejection | Enforced | DynamoDB-compatible validation on write paths |
| Empty set rejection | Enforced | DynamoDB-compatible validation on write paths |
| Number precision (38 digits) | Enforced | Range and precision validated on write paths |
| Number comparison precision | Implemented | Uses arbitrary-precision decimal (rust_decimal) instead of f64 |
| Number arithmetic precision | Implemented | SET +/-, ADD use arbitrary-precision decimal |

---

## 6. Error Fidelity (`src/errors.rs`)

| Error Type | HTTP Status | Implemented | Location |
|------------|-------------|-------------|----------|
| `ResourceNotFoundException` | 400 | Yes | errors.rs:13, status 91-93 |
| `ResourceInUseException` | 400 | Yes | errors.rs:17, status 91-93 |
| `ValidationException` | 400 | Yes | errors.rs:21, status 91-93 |
| `ConditionalCheckFailedException` | 400 | Yes | errors.rs:26-29 (with optional Item for ALL_OLD) |
| `TransactionCanceledException` | 400 | Yes | errors.rs:33, status 91-93 |
| `ItemCollectionSizeLimitExceededException` | 400 | Yes | errors.rs:37, status 91-93 |
| `ProvisionedThroughputExceededException` | 400 | Yes (defined, not raised) | errors.rs:41, status 91-93 |
| `DuplicateItemException` | 400 | Yes | PartiQL INSERT on existing item |
| `InternalServerError` | 500 | Yes | errors.rs:45, status 93 |
| `ConversionError` → `ValidationException` | 400 | Yes | errors.rs:49, type 82-83 |
| `SqliteError` → `InternalServerError` | 500 | Yes | errors.rs:53, type 84-86 |
| `IdempotentParameterMismatchException` | 400 | Yes | errors.rs — returned when ClientRequestToken matches but request differs |
| `TransactionConflictException` | 400 | Not implemented | Not in error enum |
| `TransactionInProgressException` | 400 | Not implemented | Not in error enum |
| `RequestLimitExceeded` | 400 | Not implemented | Not in error enum |

**Error response format:** Correct DynamoDB `__type` prefix (`com.amazonaws.dynamodb.v20120810#`) with `message` field. Error response includes optional `Item` for `ConditionalCheckFailedException` with `ReturnValuesOnConditionCheckFailure: ALL_OLD`.

**Unknown operation handling:** Returns `ValidationException` with `UnknownOperationException: {operation}` (server.rs:327-329).

---

## 7. Pagination Behaviour

### Query (`src/actions/query.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| 1MB response size limit | Enforced | query.rs — counts all scanned items (not just filtered items) towards limit |
| `LastEvaluatedKey` generation | Implemented | query.rs — uses last scanned item, not last returned; GSI LEK includes table primary key |
| `Limit` controls scanned items | Implemented | query.rs (SQL LIMIT, not post-filter) |
| `ExclusiveStartKey` | Implemented | query.rs (extracts effective key from ESK) |
| `ScanIndexForward` | Implemented | query.rs → storage.rs |
| `ScannedCount` vs `Count` | Implemented | query.rs — Count reflects filtered results, ScannedCount reflects all scanned |
| GSI-aware LEK | Implemented | LEK includes both GSI key names and base table primary key |
| LSI-aware routing | Implemented | Queries routed to LSI tables when IndexName points to an LSI |

### Scan (`src/actions/scan.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| 1MB response size limit | Enforced | scan.rs — counts all scanned items towards limit |
| `LastEvaluatedKey` generation | Implemented | scan.rs |
| `Limit` controls scanned items | Implemented | scan.rs |
| `ExclusiveStartKey` | Implemented | scan.rs |
| `ScanIndexForward` | Not applicable | Scan always ascending |
| `Segment`/`TotalSegments` | Implemented | Hash-based parallel scan with SQLite-level filtering |
| LSI-aware routing | Implemented | Scans routed to LSI tables when IndexName points to an LSI |

---

## 8. GSI / LSI Behaviour

### GSI Support (`src/actions/gsi.rs`, `src/storage.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| GSI creation (CreateTable) | Implemented | create_table.rs:102-106 |
| GSI creation (UpdateTable) | Implemented | update_table.rs:129-142 (with backfill) |
| GSI deletion (UpdateTable) | Implemented | update_table.rs:145-148 |
| GSI query routing | Implemented | query.rs:65-73, storage.rs:561-574 |
| GSI scan routing | Implemented | scan.rs:61-69,85-92, storage.rs:609-615 |
| Projection ALL | Implemented | gsi.rs:56-57 |
| Projection KEYS_ONLY | Implemented | gsi.rs:58-79 |
| Projection INCLUDE | Implemented | gsi.rs:80-110 |
| Sparse index (items without GSI keys excluded) | Implemented | gsi.rs:133-155 |
| GSI maintenance on PutItem | Implemented | put_item.rs:109-118 |
| GSI maintenance on UpdateItem | Implemented | Via maintain_gsis_after_write() |
| GSI maintenance on DeleteItem | Implemented | delete_item.rs:85 |
| GSI maintenance on BatchWriteItem | Implemented | batch_write_item.rs:105-130 |
| GSI eventual consistency | N/A | SQLite is always consistent (intentional) |
| GSI storage schema | Separate tables | `"{TableName}::gsi::{IndexName}"` with (gsi_pk, gsi_sk, table_pk, table_sk) |
| Per-GSI ConsumedCapacity | Implemented | Returned in INDEXES mode |

### LSI Support

| Feature | Status | Notes |
|---------|--------|-------|
| LSI creation (CreateTable) | Implemented | Creates per-LSI SQLite tables sharing the partition key |
| LSI query routing | Implemented | Query routes to LSI table when IndexName is an LSI |
| LSI scan routing | Implemented | Scan routes to LSI table when IndexName is an LSI |
| LSI projection ALL | Implemented | Full item copied to LSI table |
| LSI projection KEYS_ONLY | Implemented | Only key attributes stored |
| LSI projection INCLUDE | Implemented | Key attributes plus specified non-key attributes |
| LSI maintenance on writes | Implemented | Maintained alongside GSIs on PutItem, UpdateItem, DeleteItem, BatchWriteItem |
| LSI storage schema | Separate tables | Similar to GSI tables but sharing the partition key with the base table |

---

## 9. ConsumedCapacity (`src/types.rs`)

| Feature | Status | Location |
|---------|--------|----------|
| `TOTAL` mode | Implemented | types.rs:450-455 |
| `INDEXES` mode | Implemented | Returns Table capacity and per-GSI breakdown |
| `NONE` mode | Implemented | types.rs:448 (returns None) |
| Write capacity: 1 WCU = 1KB | Implemented | types.rs:433-435 |
| Read capacity: 1 RCU = 4KB | Implemented | types.rs:438-440 |
| Per-GSI capacity breakdown | Implemented | Returned in INDEXES mode |

---

## 10. Behavioural Notes

- **Number sort key ordering**: Normalised via `normalize_number_for_sort()` (types.rs:257-352) with 40-digit mantissa, offset exponent, and complement encoding for negatives. Handles scientific notation.
- **Number comparison precision**: Uses arbitrary-precision decimal (`rust_decimal`) for correct comparisons beyond f64 range.
- **Number arithmetic precision**: SET +/- and ADD operations use arbitrary-precision decimal to avoid silent data corruption.
- **Type coercion in comparisons**: N-typed values compared numerically via arbitrary-precision decimal parsing.
- **Item size enforcement**: Checked on PutItem and UpdateItem via `MAX_ITEM_SIZE` (400KB).
- **Item validation**: Empty strings, empty sets, and out-of-range numbers rejected on all write paths.
- **Set deduplication**: SS/NS/BS sets deduplicated on write.
- **Stream events**: Recorded for PutItem, UpdateItem, DeleteItem, BatchWriteItem, TransactWriteItems with configurable view type (NEW_AND_OLD_IMAGES, NEW_IMAGE, OLD_IMAGE, KEYS_ONLY).
- **Idempotency tokens**: TransactWriteItems accepts `ClientRequestToken` but idempotency is not enforced (known limitation).
- **Condition + write atomicity**: PutItem, UpdateItem, DeleteItem wrap condition check and write in a single SQLite transaction to prevent race conditions.
- **Validation ordering**: Dynoxide validates in its own order, which may differ from DynamoDB for edge cases with multiple simultaneous validation errors.
- **PartiQL INSERT**: Correctly rejects inserts of items with duplicate primary keys (DuplicateItemException). Supports IF NOT EXISTS for conditional insertion.
- **`begins_with` sort key**: SQL LIKE wildcards (`%`, `_`) are properly escaped to prevent false matches.
- **DDB Local: ItemCollectionMetrics** — DynamoDB Local returns `undefined` for `ReturnItemCollectionMetrics: SIZE` on PutItem, UpdateItem, and DeleteItem. Dynoxide returns the correct metrics. (AWS documentation confirms this limitation.)
- **DDB Local: Tags** — DynamoDB Local does not implement TagResource, UntagResource, or ListTagsOfResource. Returns `UnknownOperationException`. Dynoxide fully supports tags.
- **DDB Local: Parallel scan** — DynamoDB Local ignores `Segment` and `TotalSegments` parameters, returning the full table for each segment. Dynoxide implements correct hash-based segment assignment. (AWS documentation confirms this limitation.)
- **DDB Local: Validation ordering** — DynamoDB Local fails 28 Tier 3 conformance tests due to incorrect exception types and error message wording. Dynoxide passes all 166 Tier 3 tests.
