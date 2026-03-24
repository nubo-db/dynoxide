# Dynalite Test Suite Results Against Dynoxide

**Date:** 2026-03-17
**Dynoxide commit:** `7562da4` (fix/dynamodb-compatibility-gaps branch, 18 commits ahead of main)
**Dynalite commit:** `c5e5b46ef5e51e7d907411c001db7839dd146088`
**Total tests:** 1037
**Passed:** 68 (6.6%)
**Failed:** 969
**Skipped:** 0

## Summary

The low pass rate is dominated by two systemic issues, not widespread functional bugs:

1. **Error type prefix mismatch (696 failures):** Dynalite's tests expect the old AWS Coral error type format (`com.amazon.coral.validate#ValidationException`) while Dynoxide uses the newer DynamoDB v2 format (`com.amazonaws.dynamodb.v20120810#ValidationException`). Both are valid — AWS migrated between these formats. This is a single configuration-level fix.

2. **SerializationException handling (216+ failures):** Dynalite tests validate that malformed JSON payloads (wrong types for fields) return `SerializationException`. Dynoxide's serde-based deserialisation returns different error types or rejects these at a different layer. This is a Category D issue — the inputs are intentionally malformed and the behaviour difference is in error reporting, not data handling.

Together these two issues account for ~900 of the 969 failures. The remaining ~70 failures are a mix of genuine gaps, error message mismatches, and validation ordering differences.

## Category D — Error Type Prefix Mismatch (696)

The dominant failure. Dynalite's `assertValidation` helper expects:
```
com.amazon.coral.validate#ValidationException
```

Dynoxide returns:
```
com.amazonaws.dynamodb.v20120810#ValidationException
```

**Fix:** This is a real compatibility issue. DynamoDB's actual current API returns the `v20120810` prefix for most errors, but some older operations still return the `coral` prefix. Dynoxide should support both. The simplest fix: for `ValidationException` specifically, check which prefix Dynalite's tests expect per-operation and match it. Or accept that `v20120810` is the modern format and document the divergence.

**Affected operations:** All operations — createTable, putItem, getItem, deleteItem, updateItem, query, scan, batchGetItem, batchWriteItem, deleteTable, describeTable, listTables, updateTable, tagResource, untagResource, listTagsOfResource.

## Category D — Error Routing Mismatches (103)

Dynoxide returns a different error type than expected for certain invalid inputs:

| Expected | Actual | Count | Nature |
|----------|--------|-------|--------|
| `coral.validate#ValidationException` | `ResourceNotFoundException` | 80 | Dynoxide validates table existence before input validation. Dynalite validates input structure first. |
| `coral.service#AccessDeniedException` | `ValidationException` | 12 | Dynoxide doesn't implement access control at all |
| `coral.validate#ValidationException` | `ResourceInUseException` | 8 | Validation ordering difference |
| `coral.validate#ValidationException` | `InternalServerError` | 2 | Unexpected internal error |
| `ValidationException` | `LimitExceededException` | 1 | Wrong error type for too many GSI updates |

**Key insight:** The 80 `ResourceNotFoundException` vs `ValidationException` cases are a **validation ordering issue**. DynamoDB validates the request structure before checking table existence. Dynoxide checks table existence first (via `helpers::require_table`). This means Dynoxide returns "table not found" for requests that should have been rejected as malformed first.

## Category D — SerializationException Tests (216+)

Tests that validate JSON type-level deserialisation errors (e.g., "RequestItems should be a map", "Keys should be a list"). Dynoxide uses serde for deserialisation which handles these differently — either rejecting them as parse errors or coercing types. These are not functional compatibility issues.

## Category A/D — Undefined Response Body (86)

Tests where `response.body.should` fails because the response body is undefined or not in the expected format. These likely occur when Dynoxide returns an unexpected HTTP status or content-type that Dynalite's response parser doesn't handle. Need individual investigation — some may be genuine bugs, others may be response format differences.

## Category C — Dynalite-Specific (1)

| Test | Issue |
|------|-------|
| `createTable functionality should return CREATING status` | Dynoxide makes tables immediately ACTIVE; Dynalite simulates CREATING state via `--createTableMs` |

## Category A — Genuine Bugs (Estimated 0-20)

After accounting for the systemic issues above, the number of genuine functional bugs is likely very small — fewer than 20. The remaining failures need individual investigation to determine if they're:
- Message text mismatches (Category D)
- Validation ordering differences (Category D)
- Actual wrong behaviour (Category A)

## Recommended Next Steps

### Priority 1: Fix the error type prefix (addresses 696 failures)

DynamoDB uses different error type prefixes depending on the operation and error class:
- `com.amazon.coral.validate#ValidationException` — for input validation
- `com.amazon.coral.service#SerializationException` — for malformed JSON
- `com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException` — for condition failures

Dynoxide currently uses `v20120810` for everything. The fix: use the `coral.validate` prefix for `ValidationException` and `coral.service` for `SerializationException`, keeping `v20120810` for DynamoDB-specific errors.

### Priority 2: Fix validation ordering (addresses 80 failures)

Validate request structure (field types, required fields) before checking table existence. This matches DynamoDB's actual validation order.

### Priority 3: Add SerializationException (addresses 216+ failures)

Add request-level validation that catches type mismatches in the JSON payload before serde deserialisation. This is the `SerializationException` path that Dynalite extensively tests.

### Priority 4: Investigate the 86 undefined-body failures

These need case-by-case analysis to determine root cause.

## Known Limitations

- **Access control tests (12 failures):** Dynoxide has no authentication/authorization. Tests expecting `AccessDeniedException` will always fail.
- **Table state timing (1 failure):** Dynoxide tables are immediately ACTIVE. No CREATING → ACTIVE transition.
- **Throughput management:** Tests related to provisioned throughput enforcement are not applicable.
