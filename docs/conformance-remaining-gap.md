# Dynalite Conformance: Remaining Gap

**Date:** 2026-03-19
**Dynoxide commit:** `678e776`
**DynamoDB parity:** 433/497 (87.1%)
**Dynalite pass rate:** 816/1039 (78.5%)
**Internal tests:** 610+ passing
**Remaining gap:** 64 tests pass on real DynamoDB but fail on Dynoxide
**Progress this session:** +201 dynalite tests fixed (615 → 816), +83 DynamoDB parity (350 → 433)

### What was fixed
- Filter condition argument count and type validation (query/scan)
- SerializationException pre-checks for non-list field types
- UpdateTable billing mode, throughput, LimitExceededException
- Set type comparison (SS, NS, BS) and binary CONTAINS
- ExclusiveStartKey schema validation with correct error messages
- Parallel scan MD5 hash with Oracle packed BCD encoding
- GSI hash-only query ordering and pagination
- ConsumedCapacity calculation for batch operations
- AttributeValue number validation during deserialisation
- Query key rejection (hash/range in FilterExpression/QueryFilter)
- Non-scalar key access detection
- Duplicate set rejection in item validation
- Expression parser function name detection

### Remaining gap categories
- Expression validation (syntax errors, reserved keywords, function misuse): ~12
- Validation ordering mismatches (DynamoDB checks values before arg counts): ~15
- UpdateItem expression validation (conflicting/overlapping paths): ~8
- CreateTable validation (missing PT, >5 GSIs): ~4
- ESK predicate matching: ~5
- Other individual issues: ~23

## Gap by category

### Query (47 tests)

**GSI query and projection (7 tests):**
- GSI hash-only index queries not returning results in correct order
- GSI projected attributes not returned correctly (ALL_ATTRIBUTES vs index projection)
- Secondary index string ordering
- These require GSI storage layer changes (ordering by hash key only)

**ExclusiveStartKey validation (15 tests):**
- ESK schema validation for global/local indexes
- ESK with invalid values, multiple datatypes, empty/invalid numbers
- ESK segment mismatch for parallel scans
- ESK validation messages don't match DynamoDB's exact wording

**Legacy KeyConditions/QueryFilter validation (12 tests):**
- Incorrect argument count for comparison operators
- Invalid BEGINS_WITH/BETWEEN/CONTAINS types in KeyConditions
- Invalid values and multiple datatypes in KeyConditions/QueryFilter
- SerializationException for non-list AttributeValueList
- Hash/range key appearing in QueryFilter (should be rejected)

**Expression validation (8 tests):**
- BETWEEN args wrong order/type in KeyConditionExpression
- Missing ExpressionAttributeValues
- Expression + non-expression mixing validation
- Hash/range key in FilterExpression (should be rejected for Query)
- Syntax error message format

**Other (5 tests):**
- ALL_ATTRIBUTES validation when GSI doesn't have ALL projection
- Various edge cases

### Scan (32 tests)

**Legacy ScanFilter (11 tests):**
- Set type scanning (BS, NS, SS with EQ, NE)
- CONTAINS/NOT_CONTAINS on binary type
- These require the filter condition evaluator to handle set types

**Expression validation (10 tests):**
- Function validation messages (incorrect operands, operand types, names)
- Reserved keyword detection
- Redundant parentheses detection
- Non-distinct expression paths
- Missing attribute names/values

**Parallel scan (5 tests):**
- Segment validation messages (minor wording differences)
- Segment hash function produces different results than DynamoDB
- Items returned in different hash order

**ExclusiveStartKey (3 tests):**
- ESK validation for compound global index
- ESK segment mismatch
- ESK with invalid values

**Other (3 tests):**
- `size()` function calculation differences
- `attribute_type()` function with incorrect value

### UpdateItem (18 tests)

**UpdateExpression validation (11 tests):**
- Conflicting/overlapping paths detection
- Multiple sections validation
- Syntax error messages
- Undefined attribute names/values (pre-table-check ordering)
- Incorrect number of operands to functions
- Incorrect types to functions (if_not_exists, list_append)

**Nested attribute updates (3 tests):**
- Updating nested attributes on non-existent items
- Updating non-existent nested paths

**Type validation (4 tests):**
- ADD/DELETE with incorrect types
- Index update rejection
- Item size limit validation timing

### CreateTable (12 tests)

**SerializationException format (8 tests):**
- Specific SerializationException messages for struct fields passed as wrong types
- These require matching DynamoDB's Java class names in error messages

**Validation (4 tests):**
- Missing ProvisionedThroughput validation ordering
- Missing attribute definitions in GSI/LSI
- More than 5 empty GSIs validation message

### GetItem (10 tests)

- Nested attribute projection
- Table-being-created status check
- ProjectionExpression syntax/conflict validation
- Key attribute value validation (invalid numbers, multiple datatypes)
- Reserved keyword detection
- Non-scalar key access validation

### UpdateTable (7 tests)

- LimitExceededException for too many GSI updates
- PROVISIONED without ProvisionedThroughput
- PAY_PER_REQUEST with ProvisionedThroughput update
- High index capacity validation for non-existent indexes
- Same read/write values rejection
- Throughput triple-and-reduce functionality

### PutItem (7 tests)

- Invalid numbers/values/datatypes in Item
- Expected parameter validation (AttributeValueList length)
- Secondary index key type validation
- Large item with multi-number attribute (ResourceNotFoundException ordering)

### BatchGetItem (6 tests)

- ConsumedCapacity values for consistent/inconsistent reads
- Duplicate mixed-up keys detection
- Projection with AttributesToGet

### DeleteItem (5 tests)

- ExpressionAttributeValues validation (invalid numbers, keys, values, datatypes, types)

### Other (3 tests)

- DeleteTable: eventually delete (table state timing)
- BatchWriteItem: ConsumedCapacity for larger items
- ListTables: empty ExclusiveStartTableName validation

## Categorisation

| Category | Tests | Fix approach |
|----------|-------|--------------|
| **Validation message text** | ~60 | Match DynamoDB exact wording per Dynalite test expectations |
| **ExclusiveStartKey validation** | ~20 | Validate ESK schema/values with correct error messages |
| **SerializationException format** | ~12 | Map serde errors to DynamoDB Java class names |
| **Expression validation** | ~20 | Pre-table-check expression syntax/reference validation |
| **Legacy filter set types** | ~11 | Extend filter condition evaluator for BS/NS/SS |
| **GSI query ordering** | ~7 | Storage layer GSI query ordering for hash-only indexes |
| **Parallel scan hash** | ~3 | Hash function divergence (may be unfixable) |
| **Nested updates** | ~3 | Nested path handling in UpdateItem |
| **ConsumedCapacity** | ~7 | Correct RCU/WCU calculation per item |
| **Other** | ~3 | Individual investigation |

## Recommended approach for next session

1. **Start with validation messages** (60 tests) — these are the highest count and purely mechanical string matching
2. **ExclusiveStartKey validation** (20 tests) — systematic, well-defined area
3. **Expression validation** (20 tests) — error messages for invalid expressions
4. **SerializationException format** (12 tests) — map serde errors to DynamoDB format
5. **Legacy filter set types** (11 tests) — extend condition evaluator
6. **Everything else** — individual investigation

## How to run

```bash
# Build and start Dynoxide
cargo build --release
./target/release/dynoxide --port 4567 &

# Run Dynalite suite
cd tests/external/dynalite
REMOTE=1 DYNALITE_HOST=http://127.0.0.1:4567 npx mocha --require should --reporter spec -t 10s

# Run against real DynamoDB for comparison (requires AWS credentials)
eval $(aws configure export-credentials --profile nubo --format env)
REMOTE=1 AWS_REGION=eu-west-2 npx mocha --require should --reporter spec -t 30s

# Compare results
# AWS passing tests are in tests/external/results/dynalite-aws-*.txt
# Dynoxide results are in tests/external/results/dynalite-2*.txt
```
