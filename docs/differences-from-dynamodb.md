# Differences from DynamoDB

Dynoxide aims to answer every request the way DynamoDB does, and the
[conformance suite](https://paritysuite.org) measures how close it gets. This
page lists the places it knowingly doesn't, so you can tell a known difference
from a bug. The [compatibility summary](compatibility-summary.md) has the
detail behind each one.

## Operations it doesn't have

Dynoxide serves every DynamoDB operation that makes sense on one machine. The
rest are cloud infrastructure: backups and point-in-time restore, global
tables, Kinesis streaming, table import and export, reserved capacity, resource
policies and a few more. Calling one returns `UnknownOperationException`. The
[full list](compatibility-summary.md#not-implemented) is in the compatibility
summary.

## Behaviour that differs because it's one process

- **Every read is strongly consistent.** `ConsistentRead` is accepted and
  changes nothing, because SQLite is consistent anyway. You can't reproduce a
  stale, eventually consistent read.
- **Transactions never conflict.** There's no concurrent contention inside one
  process, so `TransactionConflictException` and
  `TransactionInProgressException` don't happen.
- **Streams have one shard.** `DescribeStream` returns a single shard, and its
  `ExclusiveStartShardId` and `Limit` are accepted and ignored.
- **Data isn't split by credentials or region.** DynamoDB Local keeps a
  separate database per access key and region unless you pass `-sharedDb`.
  Dynoxide has one database, so a table created under one set of credentials
  is visible under any other.
- **Signatures aren't checked.** The `Authorization` header has to be present
  and well formed, but any credentials are accepted.

## Small differences in edge cases

- **Number arithmetic** uses arbitrary-precision decimals, which may differ from
  DynamoDB's own implementation at extreme edge cases.
- **Two PartiQL refusals in one statement**: when a statement has both an
  operand of the wrong type for its operator and something refused while the
  statement is read, Dynoxide reports the second and DynamoDB the first.
- **Map order when a too-long request is echoed back in its error** can differ
  for entries that land in the same hash bucket.

The compatibility summary's
[Known Remaining Limitations](compatibility-summary.md#known-remaining-limitations)
has each of these in full.

## Vector search

- **Results are exact.** Dynoxide returns the genuinely nearest vectors, where
  AWS returns approximately the nearest.
- **Ties are always ordered the same way**, where AWS may order equal scores
  differently between requests.
- **Index creation** goes through the same states as on AWS, but not in
  anything like AWS's time.
- **`VectorSearchRequestBytes`** is Dynoxide's own figure, not AWS's.

[Four things about vector search that will never match AWS](compatibility-summary.md#four-things-about-vector-search-that-will-never-match-aws)
has the detail.

## Where AWS itself isn't consistent

Some answers vary between AWS regions, or between two hosts in the same
region. For these Dynoxide picks one answer and keeps to it, so a test
shouldn't depend on either:

- **Scan order across partitions.** Don't depend on which partition a `Scan`
  reaches first.
- **`LastEvaluatedKey` on a page that ends exactly at the last item.** Page
  until `LastEvaluatedKey` is absent, and treat an empty page as the end.
- **How a refused key schema is printed in an error message.** Match the
  constraint text, not the list.

[Where AWS answers the same request differently](compatibility-summary.md#where-aws-answers-the-same-request-differently)
has the captures behind each one.

## Behaviour that has changed between Dynoxide versions

When a release brings an answer closer to DynamoDB's, a test that asserted the
old answer can start failing. The compatibility summary lists these
[by version](compatibility-summary.md#behaviour-changes-by-version).
