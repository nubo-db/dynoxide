# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.5.0] - 2026-10-23

The crate moves to `dynoxide-rs` 3.2.0. Everything else a user installs carries
1.5.0. The release brings read capacity, stream records and SearchVectors
errors in line with DynamoDB, so some requests answer differently: see
Behaviour changes and `docs/compatibility-summary.md`.

### Security

- **`ctutils` moves to 0.4.5** for RUSTSEC-2026-0385, non-constant-time mask
  generation and `u128` comparisons. It reaches the tree through the AWS SDK,
  a dev-dependency only, and never shipped in a released artefact.

### Behaviour changes

- **The `http-server` and `mcp-server` features no longer turn on `cli`.** A
  library that embeds either server stops compiling clap and
  `tracing-subscriber`, which only the binary uses: 22 fewer crates for
  `native-sqlite,http-server`. The default features, `full` and the
  `encrypted-*` bundles include `cli` already, but a build that names the
  server features explicitly needs `cli` added to get the binary. A crate that
  relied on Dynoxide to turn on `tracing-subscriber`'s `env-filter` feature
  needs to enable it itself.
- **A read of a table with a vector index is charged on the billed size.**
  DynamoDB bills a vector attribute at four bytes per dimension on reads now,
  as it already did on writes, so an item whose numbers took it over a 4 KB
  boundary costs a unit less. This applies to `GetItem`, `BatchGetItem`,
  `TransactGetItems`, `Query`, `Scan` and every PartiQL read of the table. A
  read served from a secondary index and the 400 KB item limit still size the
  vector by its numbers ([#264](https://github.com/nubo-db/dynoxide/issues/264)).
- **`GetItem` and `TransactGetItems` charge the whole item whatever the
  projection returns**, as `BatchGetItem` and DynamoDB already did. A read of
  one attribute of a large item used to cost only what it returned.
- An import with `record_streams` records a `MODIFY` carrying the item it
  replaced when it overwrites a key, whether that item was stored before the
  import or came earlier in it, as a `PutItem` over the key does. It used to
  record an `INSERT` with no old image
  ([#265](https://github.com/nubo-db/dynoxide/issues/265)).
- **A stream record's `SequenceNumber` is 21 digits**, within DynamoDB's 21 to
  40, and records already in a database file are shown in the same 21-digit
  form. Every table's next number is at least the current time in
  microseconds, so an in-memory server doesn't normally reissue the numbers
  an earlier run gave out. Numbers keep their order as strings and as numbers.
- **`GetShardIterator` validates `SequenceNumber` as DynamoDB does**, with a
  `ValidationException`. An `AT_SEQUENCE_NUMBER` or `AFTER_SEQUENCE_NUMBER`
  request refuses a missing number, one outside 21 to 40 characters, and one
  that names no record that could have been issued; it used to read the stream
  from its start. `TRIM_HORIZON` and `LATEST` refuse a `SequenceNumber`, where
  they used to ignore it. The short numbers earlier builds returned fail the
  length check, so a consumer holding one has to restart from
  `TRIM_HORIZON`.
- **Downgrading replays the stream.** A `SequenceNumber` saved under this
  release does not resolve on an earlier release, so a consumer that resumes
  from one after a downgrade reads the stream again from its start. Records
  written under this release also report a different eventID on an earlier
  release, so a consumer that de-duplicates by eventID processes them again.

### Fixed

- **A stream record's `eventID` is globally unique.** It was the record's
  sequence number, so an in-memory server issued `1`, `2`, `3` again after
  every restart, and a consumer that skips eventIDs it has already processed
  silently dropped new events. Each new record gets a random 32-character
  hex id when it is written, stored with it so every read returns the same
  one. Records already in a database file keep the eventIDs they reported
  before ([#258](https://github.com/nubo-db/dynoxide/issues/258)).
- Stream records written by an import with `record_streams` go to the shard
  `DescribeStream` lists and follow the table's `StreamViewType`. A consumer
  following `DescribeStream` never saw them, and a `KEYS_ONLY` table got the
  new image anyway ([#260](https://github.com/nubo-db/dynoxide/issues/260)).
- `SearchVectors` answers an explicit null `IndexName`, `TableName`,
  `SearchVector` or `TopK` with a `SerializationException` quoting the
  deserialiser, as DynamoDB does. A missing `TableName` gets the same
  missing-field `ValidationException` as the other three required members,
  ahead of a too-short `IndexName`
  ([#263](https://github.com/nubo-db/dynoxide/issues/263)).
- `SearchVectors` answers a null, empty or two-type value in `SearchVector` or
  `ExpressionAttributeValues` with a `SerializationException` carrying no
  message, and a null where a member's value belongs with the deserialiser's
  `invalid type: null, expected ...`, as DynamoDB does. Other operations are
  unchanged ([#266](https://github.com/nubo-db/dynoxide/issues/266)).
- `SearchVectors` answers a malformed request as DynamoDB's search endpoint
  does. A wrongly typed member quotes the deserialiser, an unknown type lists
  the ones it accepts, and the search vector, expression values and expression
  names are checked before the table is looked up, in DynamoDB's order. An
  invalid `ReturnConsumedCapacity` is refused on its own, without the
  `1 validation error detected:` envelope, and a `TopK` beyond the 32-bit range
  is a `ValidationException`. Its `SerializationException` bodies use a
  lowercase `message`, as that endpoint writes them. Other operations are
  unchanged ([#267](https://github.com/nubo-db/dynoxide/issues/267)).

## [1.4.0] - 2026-10-21

The crate moves to `dynoxide-rs` 3.1.0 for the new `server::serve_with_shutdown`.
Everything else a user installs carries 1.4.0. Most of the release brings
answers in line with DynamoDB, so some requests answer differently: see
Behaviour changes and `docs/compatibility-summary.md`.

### Security

- **`rustls-webpki` moves to 0.103.16** for RUSTSEC-2026-0382. Earlier
  versions could accept a certificate chain through one of the distrusted
  emSign intermediates, whose private keys are now public. `rustls` moves to
  0.23.46 alongside it for RUSTSEC-2026-0346 to RUSTSEC-2026-0349. Both reach
  the tree through dev-dependencies only and never shipped in a released
  artefact.

### Behaviour changes

- `Database::scan` and `Database::query` check a request built in Rust as the
  HTTP path checks it, so they refuse requests they used to run: a `Limit` of
  0 on either, and on `Database::scan` a `Select` or `ReturnConsumedCapacity`
  outside its enum and a `Segment` above 999999. Scan's member errors now come
  from `Database::scan` rather than from decoding the request, so a
  `ScanRequest` with a `Select` of `BAD` decodes.
- For a request built in Rust, an empty `UpdateExpression` comes back as
  `DynoxideError::EnvelopedValidation` rather than `ValidationException`; the
  message is unchanged.
- `dynoxide mcp` refuses `--max-items 0` as it starts, and its `scan` and
  `query` tools refuse a `limit` of 0, as DynamoDB refuses a `Limit` of 0.

### Added

- **`server::serve_with_shutdown` stops an embedded HTTP server cleanly.** It
  serves on a listener you bind until a future you supply resolves, then stops
  accepting, closes idle keep-alive connections, lets running requests finish
  and returns. Stopping a server you had started yourself used to mean
  aborting its task, which left the connection tasks holding the database, so
  a file-backed database's files stayed open (and could not be deleted on
  Windows). `start` now runs on it too, with its Ctrl-C and SIGTERM handling
  unchanged.
- `dynoxide import` reads JSON Lines files named `.jsonl` and `.jsonl.gz`, in a DynamoDB Export directory (`<dir>/<TableName>/data/`) or a flat directory of those files, alongside the `.json` and `.json.gz` parts it already accepted ([#250](https://github.com/nubo-db/dynoxide/pull/250), thanks [@samchungy](https://github.com/samchungy)).

### Fixed

- **A `SearchVectorsRequest` built in Rust now meets the `IndexName`,
  `SearchVector` and `TopK` checks.** An `IndexName` shorter than three
  characters, an empty `SearchVector` or a `TopK` below 1 used to get past
  `Database::search_vectors` and fail later with whatever came next. They now
  get the same validation message as the same request sent over HTTP, in the
  same order. A `TopK` below 1 now gets the `1 validation error detected:`
  form it has over HTTP rather than the bare out-of-range message; `TopK` above
  100 keeps the bare message.
- **Five request-validation messages now match DynamoDB when more than one
  thing is wrong.** UpdateItem with no `TableName` keeps that error beside a
  bad `ReturnValues`, and, when the key is missing too, beside a bad
  `ReturnConsumedCapacity` or `ReturnItemCollectionMetrics`, in the order
  DynamoDB lists them. Query with
  no `TableName` and a `Limit` of 0 reports both, with a bad `Select` first.
  BatchGetItem reports a malformed table name ahead of the total key count.
  DeleteItem treats a `ReturnValues` of `ALL_NEW` or `UPDATED_NEW` as a request
  member: it is joined after the other member errors and reported ahead of any
  expression error. UpdateItem puts an empty `UpdateExpression` in the
  `1 validation error detected:` envelope and reports an empty
  `ConditionExpression` before it.
- `UpdateItem` and PartiQL `UPDATE` refuse a `SET`, `REMOVE`, `ADD` or `DELETE` whose path has a missing or wrong-typed parent: `The document path provided in the update expression is invalid for update`. PartiQL `SET zz.y = 1` no longer creates `zz`.
- Every clause of an update reads the item as it was before the update, in `UpdateItem` and PartiQL: `REMOVE l[0], l[1]` removes both elements, and `SET n = 7, s = n + 1` gives `s` the old `n` plus one.
- PartiQL `UPDATE` refuses overlapping paths (`Two document paths overlap with each other`) and takes one path per `REMOVE` keyword, so `REMOVE a, b` is a syntax error.
- `SET` arithmetic and `list_append` return DynamoDB's errors for a missing attribute, a stored value of the wrong type and a value of the wrong type. A PartiQL `SET n = zz + 1` on a missing `zz` is refused, not read as zero. `ADD` and `DELETE` name the operand type in full with a `typeSet`; the others use the short code (`S`, `N`, `L`).
- PartiQL `+`, `-` and `list_append` take an attribute on either side, and a quoted string is a string.
- An update that appends past the end of a list reads every other index on that list as the list was: an `ADD`, `DELETE` or `REMOVE` at an index the list did not reach leaves the appended element alone, and the appends land in index order, PartiQL's included. A `DELETE` that empties a set held in a list element takes the element out at its original index.
- `Scan` checks its request members as the newer validation layer does, which 20 of 33 regions now run: `Limit`, `Segment`, `TotalSegments` and an empty `TableName` come first in the newer wording, then the table-name format and `ReturnConsumedCapacity`, then `Select`, with an `ExclusiveStartKey` mismatch answered as `The provided starting key is invalid` ([#254](https://github.com/nubo-db/dynoxide/issues/254)).
- `Scan` refuses `Select` of `COUNT`, `ALL_ATTRIBUTES` or `ALL_PROJECTED_ATTRIBUTES` beside `AttributesToGet`, and answers an empty attribute name in an `ExclusiveStartKey` or an expression value bare.
- `Scan` words a bad number or a repeated set member in an attribute value as the newer layer does, with no `ExpressionAttributeValues contains invalid value` wrapper.
- `Scan` reads `Limit`, `Segment` and `TotalSegments` as 32-bit integers that saturate and drop a fraction, so `Limit` 4294967296 is accepted and a negative `TotalSegments` is a validation error rather than a decoding one. It refuses a `Segment` above 999999, checks `Select` of `ALL_ATTRIBUTES` against a keys-only index ahead of the `ExclusiveStartKey`, and on a table with indexes reports an `ExclusiveStartKey` attribute of the wrong type as an index key mismatch.
- `Query` refuses `Select` of `COUNT`, `ALL_ATTRIBUTES` or `ALL_PROJECTED_ATTRIBUTES` beside `AttributesToGet`, puts its `Cannot specify ...` and `ALL_PROJECTED_ATTRIBUTES` messages in the `1 validation error detected:` envelope, reads `Limit` as `Scan` does, and answers an index key of the wrong type in an `ExclusiveStartKey` with `The provided starting key is invalid`. With no `TableName` it reports a bad `ReturnConsumedCapacity` beside the missing name.
- A parallel `Scan` of a table with `TotalSegments` above 4096 returns each item once. The segments past the last hash bucket used to return almost every item.
- A parallel `Scan` that deletes each page as it goes sees its whole segment. Once the item its `ExclusiveStartKey` named was deleted, the next page used to come back empty, with no `LastEvaluatedKey`.

## [1.3.0] - 2026-10-10

The crate moves to `dynoxide-rs` 3.0.0: the `expressions` and `partiql`
modules are no longer public, and three request types gain fields. Everything
else a user installs carries 1.3.0.

### Security

- **An expression containing a multi-byte character could stop the server**
  ([GHSA-gwhp-7jch-7xj6](https://github.com/nubo-db/dynoxide/security/advisories/GHSA-gwhp-7jch-7xj6),
  High). From 0.9.10 to 1.2.1, a character such as `€` or an emoji outside a
  string in an `UpdateExpression` or `ProjectionExpression` made Dynoxide panic
  while building the syntax error, and release builds abort on panic. It is
  now refused with DynamoDB's syntax error.
- **`--help` showed the MCP auth token**
  ([GHSA-3rv6-mrj3-vf9j](https://github.com/nubo-db/dynoxide/security/advisories/GHSA-3rv6-mrj3-vf9j),
  Low). From 0.10.0 to 1.2.1, subcommand help printed the value of
  `DYNOXIDE_MCP_AUTH_TOKEN` when it was set. Help now names the variable
  without showing its value.
- **`aws-smithy-json` moves to 0.63.1** for GHSA-8ffr-xgwf-xj56. It reaches the
  tree only through the AWS SDK the tests and the benchmarks use, never in a
  released artefact, and the advisory covers smithy-rs generated servers,
  which Dynoxide isn't. The fixed release needs Rust 1.94.1, so the test suite
  now needs Rust 1.94.1 or later. The crate, and every build of it, still
  compiles on 1.88.

### Behaviour changes

- A PartiQL `NextToken` is bound to its statement the way DynamoDB binds it:
  to the statement's text exactly as written, and to its parameters by value.
  The same statement pages on, and may change its `Limit`. A page whose
  statement differs at all (a space, a keyword's case, the projection, a map
  literal's key order) or whose parameters are different values is refused
  with `NextToken does not match request`, where a statement that parsed
  alike used to resume. A number parameter is the same value when it is the
  same decimal at the same scale, so `1E2` against `1e+2`, `01` against `1`
  and `1.5E1` against `15` resume, and `0` against `0.0` or `100` against
  `1E2` are refused. A set parameter is the same value with its members in
  any order, a number set's members compared by value whatever their scale.
  A statement with a list or map parameter is never resumed, even with the
  same parameter sent again: its first page still hands out a token, and the
  next page is refused with it. A map literal with more than one key used to
  be refused on its own next page, because its key order was drawn afresh
  for every request; it pages now. Captured against eu-west-2 and us-east-1.
- `UpdateItem` is held to the 400KB limit the way DynamoDB holds it: by a
  charge worked out from the request, alongside the finished item's own size,
  rather than by the finished item less its key. Nothing the table already
  stores enters the charge, so an update beside large attributes, maps, lists
  or sets it leaves alone now succeeds where it was refused. Operands are
  charged for what the request writes, so an arithmetic update, an
  `if_not_exists`, a `list_append` or a copy from another path near the limit
  can now be refused where it succeeded, though the item barely grows. An
  update whose charge is over the limit is refused before its condition is
  evaluated. The legacy `AttributeUpdates` parameter is measured by the
  finished item alone. Captured against eu-west-2. `TransactWriteItems`
  updates and PartiQL `UPDATE` are measured as before.
- A `SET` through a list index past the end of the list appends the value, as
  DynamoDB does, where it used to pad the gap with NULLs; several such writes
  to one list append in the order of their indexes. A path that steps through
  an element past the end of a list, or through a NULL, is refused with
  DynamoDB's document path error where it used to create a map or a list
  there. This covers `UpdateItem` and `TransactWriteItems` updates; PartiQL
  `UPDATE` already appended. Captured against eu-west-2.
- For Rust users of the crate: `AttributeValue`'s `Serialize` writes set
  members in DynamoDB's order (string and binary sets by their bytes, number
  sets by value), so a set no longer keeps the order it was built in through
  serde. Normalising an item for a write sorts its sets in place.
- `ReturnValuesOnConditionCheckFailure` works on the three PartiQL
  operations. Set to `ALL_OLD`, an `UPDATE` or `DELETE` whose WHERE clause is
  false on an item that exists hands back the whole item: on that statement's
  cancellation reason in `ExecuteTransaction`, on the member's `Error` in
  `BatchExecuteStatement`, and on `ExecuteStatement`'s
  `ConditionalCheckFailedException`, which then writes its message as
  `Message` and its number sets in hash order. `NONE`, leaving it out, a
  missing item, a duplicate `INSERT` and a statement without a condition give
  no item. A value other than `ALL_OLD` or `NONE`, an empty string included,
  is refused as a member, ahead of the statement and its parameters, where
  `ExecuteStatement` and `ExecuteTransaction` used to accept anything. A
  transaction's mode takes part in its `ClientRequestToken` match, leaving it
  out counting as `NONE`, and its over-long echo prints the value sent.
  `ExecuteStatement` lists its member errors in DynamoDB's order: the mode,
  `Limit`, the statement, `ReturnConsumedCapacity`, then the parameters.
  Captured against eu-west-2 and us-east-1; this corrects the 1.0.0 entry
  that found the option inert on a batch.
- Opening a database brings it up to schema version 11, and the upgrade is
  one way. It changes what an older Dynoxide reads only for a global secondary
  index with a multi-attribute key, whose entries are rebuilt, and a table with
  a vector index, whose stored item sizes are recomputed. Earlier versions
  don't check the version a file records, so one would open such a file and
  read those indexes or sizes wrongly: if a file has either and you may need
  to take it back to 1.2.x, keep a copy. From this release a file written by a newer Dynoxide is refused,
  with an error naming its schema version and the newest this build reads,
  and is left unchanged, where it used to be opened as it was. That covers
  `Database::new`, an encrypted open, the snapshot a restore reads from, and
  the browser build's database.
- While it opens, a database file waits up to 30 seconds for a lock another
  process holds, where it gave up after five, so a second Dynoxide opening a
  file while the first brings it up to date waits for it and then finds the
  work done. Once open it waits five seconds, as before, so a request held up
  by another process's lock gives up after five seconds rather than holding
  every other request up behind it. A restore still gives up on a busy
  snapshot after five seconds, and an in-memory database is unchanged.

### Added

- The MCP tools that wrap a DynamoDB read or write take
  `return_consumed_capacity` (`TOTAL`, `INDEXES` or `NONE`) and return the
  `ConsumedCapacity` block the DynamoDB API returns for the same request. That
  covers the item tools, `query`, `scan`, `search_vectors`, `batch_get_item`,
  `batch_write_item`, both transaction tools and the three PartiQL tools. Any
  other value is refused with DynamoDB's validation error.
- The MCP `execute_partiql` tool takes `return_values_on_condition_check_failure`
  (`ALL_OLD` or `NONE`), as `ExecuteStatement` does. A tool error for a failed
  condition now carries the item the condition was evaluated against when
  `ALL_OLD` was asked for, on `execute_partiql` and on the item tools, which
  took the argument before but never handed the item back.
- `--log quiet` suppresses informational startup and shutdown messages on
  stderr. It works on `serve` and the no-subcommand form, `serve --mcp`, `mcp`
  (stdio and `--http`), and `import --serve` or `--mcp`. Warnings, errors, the
  first-run MCP token message and the import's own report still print. On
  `import`, `--log` conflicts with `--output`. Without the flag the output is
  unchanged.
- `ServerOptions` (at the crate root, and at `server::ServerOptions`),
  `server::start_with_options` and `mcp::serve_http_with_options` give embedded
  Rust callers the same switch for both servers.
- `Storage::item_count_and_size`, `gsi_item_count_and_size`,
  `lsi_item_count_and_size` and `vector_item_count_and_size` return a table's
  or an index's item count and total size, the figures DescribeTable reports.

### Changed

#### Breaking (Rust API)

- The `expressions` and `partiql` modules are no longer public. They hold the
  parsers for DynamoDB's expression syntax and for PartiQL, whose shape changes
  as compatibility work lands, and this release changed both substantially.
  Expressions and PartiQL statements are still sent through `Database` and the
  `actions` request types, which this does not change.
- `ParameterizedStatement` and `ExecuteStatementRequest` gain
  `return_values_on_condition_check_failure`, and `BatchStatementError` gains
  `item`, so struct literals of them need the field adding, or
  `..Default::default()`.
- `actions::index_capacity::WriteCapacity::condition_check` takes the table's
  vector indexes, so a vector attribute is charged at its billed size.

### Fixed

- An `UpdateItem` whose `UpdateExpression` was only whitespace, such as
  `"   "`, answered 200 and wrote the item: the key alone for a new item, or
  the item as it was for an existing one. 1.2.1 did this too. An expression made
  only of whitespace is now refused as DynamoDB refuses it, as a syntax error
  at its end with the whitespace as sent: `Invalid UpdateExpression: Syntax
  error; token: "<EOF>", near: "   "`, and nothing is written. A
  `ProjectionExpression` of whitespace is refused the same way on every read
  that takes one, where it returned items with no attributes. A
  `ConditionExpression`, `KeyConditionExpression` or `FilterExpression` of
  whitespace was already refused, but with an empty `near`; it now gives the
  whitespace too, in the envelope on `PutItem`, `UpdateItem` and `DeleteItem`
  and bare elsewhere. A transaction's `Update` with an empty
  `UpdateExpression` is refused up front with `Invalid UpdateExpression: The
  expression can not be empty;`, where it wrote an item holding only its key.
  Captured against eu-west-2 and us-east-1, the transactions also against
  ap-southeast-2 and eu-central-1. `TransactGetItems` refuses an empty or
  whitespace `ProjectionExpression` with the same messages `GetItem` gives,
  and does so before it looks up any table, so a `Get` on a table that doesn't
  exist gets the projection error (captured against eu-west-2).
- A legacy `QueryFilter` or `ScanFilter` condition with a `ComparisonOperator`
  DynamoDB doesn't have, such as `FOO`, or with none at all, was dropped, so
  the Query or Scan ran without it, and one whose every condition dropped
  returned every item. A lower-case `eq` ran as `EQ`. 1.2.1 did both. An
  unknown operator, `eq` included, is now refused with `One or more parameter
  values were invalid: Invalid comparison operator`, and a missing one with `1
  validation error detected: Value null at
  'scanFilter.a.member.comparisonOperator' failed to satisfy constraint:
  Member must not be null` (`queryFilter` on a Query). `KeyConditions` takes
  the same rule, where it also ran `eq` as `EQ` and refused an unknown or
  missing operator in Dynoxide's own words. Captured against eu-west-2,
  ap-southeast-2, eu-central-1 and us-east-1; us-east-1 answers an unknown
  operator with the member's enum constraint instead, and the other three
  regions are followed.
- A legacy `AttributeUpdates` action written in another case, such as `put`
  or `Delete`, was read as its upper-case form and applied. 1.2.1 did this
  too. An action other than `ADD`, `DELETE` or `PUT` is now refused before the
  table is looked up, with DynamoDB's own text: `1 validation error detected:
  Member must satisfy enum value set: [ADD, DELETE, PUT]' for Action`.
  Captured against eu-west-2 and us-east-1.
- A legacy condition on an attribute whose name holds a `-`, a `.` or a
  character outside ASCII, such as `user-id` or `s.k`, was refused with a
  syntax error naming an internal placeholder, such as `near: "#kc_user-id"`.
  1.2.1 did this too. `KeyConditions`, `QueryFilter`, `ScanFilter` and
  `Expected` now run on any attribute name, as DynamoDB runs them. Captured
  against eu-west-2 and us-east-1.
- A legacy `QueryFilter` or `ScanFilter` condition whose value didn't read,
  such as `{"NULL": false}` or `{"BOOL": "maybe"}`, was dropped, so the Query
  or Scan ran without that condition and returned items it should have
  filtered out. This was in 1.2.1. Such a condition is now refused as
  DynamoDB refuses it, in `QueryFilter`, `ScanFilter`, `KeyConditions` and
  `Expected` alike: a `NULL` that reads as false (`false`, `"no"`, `"false"`)
  with `Null attribute value types must have the value of true`, ahead of the
  operator's own checks, and a `BOOL` or `NULL` that doesn't read as a
  boolean by failing the whole request with `Unexpected token received from
  parser`. A `KeyConditions` that didn't read no longer leaves the Query
  asking for a key condition. A legacy `IN` checks the type of every value in
  its list, where it looked at the first, and a value whose other member is
  `null` takes the type of the member it has; `Expected` refuses an `IN` of a
  `BOOL` or `NULL` too, as the filters do. A condition's `Exists` reads a
  string as a `BOOL` member does, and one read from a string makes the
  request's refusals bare, as a `BOOL` or `NULL` read from a string does.
  Captured against eu-west-2 and us-east-1.
- `ExecuteTransaction` lists a reason for every statement that failed, as
  DynamoDB does, where it stopped at the first and reported `None` for the
  rest. A key of the wrong type on any statement is reported alone, with
  `None` for every other statement, a statement whose condition is false
  included. A `DELETE` with a condition, of an item that isn't there, does
  nothing when the transaction commits, as before, and is reported as a failed
  condition, with no item, when the transaction is cancelled. Captured against
  eu-west-2 and us-east-1.
- A `TransactWriteItems` that SQLite failed part way through, on a full disk
  or an I/O error, could keep the actions that came after the failure. SQLite
  rolls the whole transaction back on such an error, so each later action
  committed on its own and the transaction's own rollback then failed. A
  storage failure now stops the transaction where it happens and is returned
  as an internal error, so nothing the transaction wrote is kept.
  `ExecuteTransaction` does the same. The error is SQLite's own, such as
  `backend: database or disk is full`, on any write, where it went on to say
  that the rollback had failed too because SQLite had already ended the
  transaction.

- Restoring a snapshot taken by an earlier version brings it up to date as
  opening the file would, running the schema migrations its recorded version
  calls for. It used to be restored as it was, so a snapshot from before a
  schema change came back without that change. The copy is brought up to date
  in a private temporary database before it replaces anything, so a restore
  that fails leaves the database as it was, and `Database::restore_from` opens
  the snapshot file read-only: a path that doesn't exist is now an error,
  where it used to create an empty file and restore that over the database,
  and so is a file that isn't a Dynoxide snapshot (an empty file, `:memory:`
  or another program's database). Restoring from a snapshot in WAL mode may
  leave its `-wal` and `-shm` files beside it. After a restore, transactions
  cached for idempotent retries and vector index creation windows are
  forgotten, as the data they described has gone. A backup or restore that
  finds a database busy keeps trying until it has been busy for five seconds by
  the clock (plus at most one busy wait of SQLite's own) and then reports it
  locked; one that keeps starting over because another connection keeps
  changing the database being copied pauses briefly after each restart and
  gives up after 50 of them.
- A number whose exponent is too large for a 32-bit integer is range-checked
  rather than read with an exponent of 0. `1e99999999999` was stored as 1; it
  is now refused as an overflow, and `1e-99999999999` as an underflow, as
  DynamoDB does. An exponent too large for a 64-bit integer is refused as a
  value that cannot be converted, which is DynamoDB's message for it.
- Zero is held to the number range as well, as if its last written digit were
  a 1, so `0e126` is refused as an overflow and `0e-131` as an underflow, where
  both used to be stored as 0.
- A `KeyConditionExpression` rejects an operand its operator does not take, as
  DynamoDB does: `begins_with` takes a string or binary, and `<`, `<=`, `>`,
  `>=` and `BETWEEN` take a string, number or binary
  ([#220](https://github.com/nubo-db/dynoxide/issues/220)). A number in
  `begins_with(sk, :v)` used to run as a prefix match on the stored key, and a
  boolean, null, list, map or set became an empty prefix that matched every
  item in the partition. The check runs before the table is looked up, so a
  missing table or index reports the operand rather than itself.
- PartiQL's `begins_with` rejects an operand that is not a string or binary,
  on any attribute and under `NOT`, where it used to accept the statement.
- A global secondary index whose key has more than one partition key or sort
  key attribute works as DynamoDB's does. Items sort by each sort key attribute
  in turn, an item missing any key attribute is left out of the index, a Query
  takes an equality on every partition key attribute and on a leading run of
  sort key attributes with a range condition allowed only on the last one
  named, and `LastEvaluatedKey` carries every key attribute. Such an index used
  to sort and filter on its first sort key attribute alone, index items missing
  a later one, and refuse any key condition with three clauses. In a database
  an earlier version wrote, on disk or in the browser's OPFS storage (including
  one from Dynoxide 0.10 to 0.13, which recorded no schema version), such an
  index is rebuilt the first time the database is opened, in one transaction,
  and an interrupted step leaves the database at the version before that step. `UpdateTable` can now add one. CreateTable and UpdateTable
  check such a key as DynamoDB does: at most four partition key and four sort
  key attributes, partition key attributes first, no name twice. A local
  secondary index still takes one sort key attribute. A PartiQL `SELECT` on such
  an index is refused, as DynamoDB refuses it, with `PartiQL statements cannot
  be executed on tables with multi-attribute HASH keys or multi-attribute RANGE
  keys.`; the table itself and its other indexes stay readable and writable.
- A `KeyConditionExpression` with more clauses than the key has attributes is
  refused with DynamoDB's message, `The number of query conditions (3) exceeds
  the number of key attributes defined in the schema (2)`, rather than
  `Unexpected token in KeyConditionExpression: AND`. Two conditions on one key
  attribute report that each key takes only one.
- A key condition value whose type differs from its key attribute's type is
  refused with `Condition parameter type does not match schema type`, on a
  table and on any index. That covers a string compared with a number sort
  key, which used to match nothing, and a map, list, boolean, null or set
  value, which used to fail with `Cannot convert partition key value to
  string`.
- `begins_with` in a key condition is case-sensitive. A lower-case prefix
  matched upper-case sort keys too.
- A key condition can name the sort key before the partition key. A range
  condition on a one-attribute partition key, such as `begins_with(pk, :v)`,
  is refused with `Query key condition not supported`, and like every key
  condition problem that needs the key, only once the table and index are
  found: a missing table or index reports itself first.
- CreateTable with an index reports an attribute definition no key uses as
  `Some AttributeDefinitions are not used`, listing the definitions and the keys
  used, rather than as a count mismatch. An index key attribute with no
  definition names the first index missing one, and its lists, like the unused
  ones, come in DynamoDB's own order. When the request defines fewer
  attributes than that index has key attributes, CreateTable and UpdateTable
  both give `Invalid KeySchema: Some index key attribute have no definition`.
  A GSI key schema of more than eight elements is refused as the request is
  read.
- `TransactWriteItems` rejects an expression over 4096 bytes on any action, and
  a `Put` item nested past 32 levels, with a top-level `ValidationException`
  before any action runs, as DynamoDB does. Both used to come back as a
  `TransactionCanceledException` with a `ValidationError` reason. The `Put`
  item-size check moves forward with them, so all three now win over a missing
  table, a duplicate target or an empty key value elsewhere in the request.
- An `ExpressionAttributeValues` entry nested 32 levels deep that a transaction
  action only compares is no longer refused: the condition is evaluated, as
  DynamoDB does. An `Update` that writes one into the item still cancels on the
  item's nesting limit, now with DynamoDB's shorter reason message. From 33
  levels an entry is refused up front with a top-level `ValidationException`
  naming its key, used or not, where a `Put` whose condition compared one used
  to write its item.
- `TransactWriteItems` rejects an empty or unparseable `ConditionExpression` on
  any action with a top-level `ValidationException` before any action runs,
  where it used to cancel the transaction. An unparseable `UpdateExpression`,
  an `ExpressionAttributeNames` or `ExpressionAttributeValues` entry no
  expression uses, and a value a condition names but the request doesn't
  define are refused the same way, ahead of a missing table, a duplicate
  target or a key that doesn't match the schema. A `Put` that carries
  `ExpressionAttributeValues` and no `ConditionExpression` is accepted, as in
  us-east-1, where it used to cancel.
- Expression syntax errors name the token the parse stopped at and the text
  around it, as DynamoDB does: `Syntax error; token: "x", near: ") x y"`, or
  `token: "<EOF>"` for an expression that ends early. This covers condition,
  filter, key condition and update expressions, which used to say things like
  `Expected operand, got end of expression`. A malformed key condition also
  gains its `Invalid KeyConditionExpression:` prefix.
- An expression over 4096 bytes is refused without the trailing
  `expression size: <n>`, as DynamoDB now sends it. A Scan's `FilterExpression`
  and a transaction's `ConditionExpression` keep the figure, which us-east-1
  still sends there.
- PutItem, UpdateItem and DeleteItem wrap the nesting error for a value past
  32 levels in `1 validation error detected: `, and DeleteItem wraps its
  `ConditionExpression` syntax and size errors the same way. From 63 levels a
  stored item's error drops the wrapper and an expression value is refused
  naming its key. An `UpdateExpression` that leaves the item nested too deep
  gets DynamoDB's short `Nesting Levels have exceeded supported limits`.
- UpdateItem reports a `ConditionExpression` syntax error ahead of an unused
  expression attribute value.
- PutItem, UpdateItem and DeleteItem wrap an unused `ExpressionAttributeNames`
  or `ExpressionAttributeValues` entry in `1 validation error detected: `, and
  report it, along with a value or name the condition uses but doesn't define,
  before looking up the table. GetItem, Query and Scan send it bare, as
  before. DeleteItem now envelopes all its `ConditionExpression` errors,
  including reversed or mixed-type BETWEEN bounds and operand types, and an
  undefined value in UpdateItem's condition gains its
  `Invalid ConditionExpression:` prefix.
- A list index written with a leading zero, such as `l[00]` or `l[01]`, is a
  syntax error in every expression (`token: "0", near: "00]"`), as DynamoDB
  reads it. It used to be accepted as the index its digits spell.
- `DescribeTable` reports `TableSizeBytes` as the total size of the table's
  items, where it was always 0, so the MCP server's `size_bytes` is right too.
  `UpdateTable`'s response carries the same `ItemCount` and `TableSizeBytes`,
  where both were 0. DynamoDB refreshes these figures about every six hours;
  Dynoxide reports them live. Captured against eu-west-2.
- `DescribeTable` and `UpdateTable` report each GSI's and LSI's `ItemCount` and
  `IndexSizeBytes`: the number of items the index holds and the total size of
  what it projects for them, where both were always 0. Each vector index
  reports its entries and their billable size, as DynamoDB does: the vector at
  4 bytes per dimension, its attribute's name and the rest of the projection. Captured against eu-west-2.
- On a table with a vector index, a write is charged for the vector at 4 bytes
  per dimension, whatever numbers it holds, and `TableSizeBytes` counts it the
  same way, as DynamoDB does. Dynoxide used to size it as a list of numbers, so
  a write of a 1,024-dimension embedding could cost two or three units more
  than on DynamoDB. Read units and the 400 KB item limit still size the vector
  by its numbers, which is DynamoDB's rule for those, and so do a GSI's and an
  LSI's share of the write and their `IndexSizeBytes`, even when the index
  projects the vector. A database written by an earlier version has its vector
  tables' sizes recomputed once when it is opened, in one transaction, and
  creating or deleting a vector index recomputes that table's.
- `Database::sweep_ttl`, and the MCP `sweep_ttl` tool that calls it, no longer
  hold the database lock for the whole sweep. The sweep takes the lock a page of
  25 items at a time and lets requests already waiting for it go first between
  pages, so a read or write waits for one page rather than for a read of every
  TTL table. The first page of each table also counts its rows, so the longest
  single hold is that count plus a page. Between pages the sweep waits up to a
  second for the queued requests, including the time they hold the lock, before
  it carries on. A sweep reads no more of a table than it held when the sweep
  reached it, so a table that keeps growing cannot keep it running, and an item
  written while the sweep is under way may be left for the next one.
- `--help` no longer shows the value of `DYNOXIDE_MCP_AUTH_TOKEN` when it is
  set. It still names the variable. The MCP auth types' debug output no
  longer shows the token either.
- A prerelease tag such as `v1.3.0-rc.1` no longer ships as if it were the
  latest release. Its GitHub Release is flagged as a prerelease, so the GitHub
  Action's default `version: latest` stays on the last stable release, and the
  Homebrew tap, the MCP registry and dynoxide.dev are left alone. A stable
  crate version is not published to crates.io from a prerelease tag. npm still
  publishes a prerelease to `next`, and containers still get the exact tag
  only. `docs/versioning.md` has the full list.
- `docs/library.md` no longer tells readers to run
  `cargo add dynoxide-rs --features encryption`, which stops on the
  `native-sqlite` and `encryption` compile guard because the default features
  stay on. It now adds `--no-default-features`, as the rest of the page does.
- PartiQL reads a number written with an exponent as one number, so
  `WHERE pk = 1e5` matches 100000 rather than 1, and `INSERT ... VALUE
  {'pk': 2e3}` parses. A literal whose exponent, or whose scale (the digits
  after the point less the exponent), is out of range is refused as a statement
  that is not well formed, as DynamoDB does.
- PartiQL reads `--` and `/* ... */` comments as whitespace wherever they
  appear. A comment before the WHERE clause used to make a SELECT return every
  row, and a block comment was refused.
- A PartiQL table alias (`FROM "T" t` or `FROM "T" AS t`) on SELECT, UPDATE or
  DELETE is refused with DynamoDB's "Aliasing is not supported". A SELECT used
  to run it as an unfiltered read.
- A PartiQL statement with a token left over after it, such as
  `WHERE pk = 1 foo`, is refused with "Unexpected token after expression"
  rather than run without the extra token. A SELECT's `ORDER BY` clause must
  be well formed and last, so a WHERE written after it is refused rather than
  dropped.
- PartiQL applies a SELECT's `ORDER BY`. It used to be accepted and ignored,
  so `ORDER BY sk DESC` came back ascending. As in DynamoDB, it reads each
  partition the WHERE clause pins, in partition-key order, by the sort key
  term's direction (descending when only the partition key is named), on a
  table or an index, a page at a time when the read is paged. The forms
  DynamoDB refuses are refused with its messages: no WHERE clause, a term that
  is a nested path or not part of the key, a partition key not pinned with `=`
  or `IN`, and more than one partition without the partition key in the
  clause. `NULLS FIRST` and `NULLS LAST` are accepted.
- PartiQL reads a number written with no digit before its point, such as `.5`
  or `-.25e1`, where it used to refuse the statement. A number takes one
  point, so `1.2.3` is refused as a token left over, and an exponent marker
  with no digits (`5e`) is refused as a statement that is not well formed.
- PartiQL checks a number written in a WHERE clause the way it checks one being
  stored, so `WHERE pk = 1e126` is refused with DynamoDB's overflow message
  rather than run as a comparison that matches nothing. Number errors on
  PartiQL literals end with DynamoDB's ` under <path>`, naming where the
  number sits inside the value (`under root`, `under root.x`, `under root[1]`),
  and are reported before the table or item is looked up, the first one
  written first and an UPDATE's WHERE clause before its SET. A literal at the
  very top of the exponent range is reported as an underflow, as DynamoDB's
  PartiQL reports it.
- A PartiQL literal nested 32 levels deep is refused where it reaches that
  level, with DynamoDB's `Nesting Levels have exceeded supported limits` and
  the path to it, and a parameter nested exactly 32 levels deep gets the same
  short message rather than the long one.
- PartiQL statements check every parameter value before reading the
  statement, as any attribute value is checked (its numbers, and empty or
  duplicated sets), so a parameter DynamoDB can't store is refused even when
  the statement doesn't use it: on `ExecuteStatement`, against the statement in
  a `BatchExecuteStatement` (an empty value and `{"NULL": false}` included,
  where a `{"NULL": false}` parameter used to refuse the whole batch), and for
  the whole request in an `ExecuteTransaction`, which writes nothing.
- A PartiQL statement against a table that doesn't exist says `Requested
  resource not found`, as DynamoDB does, without `: Table: <name> not found`;
  in a transaction it cancels with the `ResourceNotFound` reason rather than
  `InternalError`.
- An empty map key anywhere in a PartiQL literal is refused with DynamoDB's
  `Path component cannot be an empty string`, where it used to be stored.
- A PartiQL SELECT whose OR branches name overlapping keys, such as
  `pk = 'a' OR pk = 'a'` or sort-key ranges (`begins_with` included) that meet
  in one partition, is refused with DynamoDB's `Overlapping conditions with
  range keys are not supported in where clause`. As in DynamoDB, a clause with
  a branch that puts two different conditions, or `NOT ... BETWEEN`, on the
  sort key isn't checked, and the check comes after an `ORDER BY` term is
  checked and before the rest of the `ORDER BY` checks.
- A PartiQL key value DynamoDB can't read the key by is refused with its
  messages: `Key value must be of type S, N, or B` naming the key and the type
  for a type no key can have, such as a map, rather than `Invalid key value`,
  and `Key attribute's data type should match its data type in table's schema`
  for a key type that isn't the key's, where a SELECT, UPDATE or DELETE used to
  match nothing and an INSERT gave PutItem's message. Both apply to literals
  and parameters, to the sort key and to an index's keys, and a SELECT's OR
  branches are checked in the order written until one that isn't a key read.
  In a transaction, a SELECT key, or an UPDATE or DELETE key of the wrong key
  type, cancels with GetItem's `The provided key element does not match the
  schema`, an UPDATE or DELETE key no key can have is refused up front, and an
  INSERT keeps PutItem's message.
- A SELECT in a `BatchExecuteStatement` or an `ExecuteTransaction` may name only
  the primary key, as DynamoDB requires: each key attribute once, with `=` or
  an `IN` of one value, the same condition written twice counting once. One
  that also filters on another attribute is refused rather than run, and
  overlapping key conditions are reported first.
- `ExecuteTransaction` reports a statement that doesn't parse with
  `Validation failed in TransactStatements[N]: ` in front, as it already did
  for its other per-statement refusals.
- A PartiQL statement that doesn't begin with SELECT, INSERT, UPDATE or DELETE
  is refused as DynamoDB refuses it, reading it as an expression rather than
  answering `Expected data manipulation` to everything: `Unsupported
  operation: Id` for a lone name, `Path`, `Lit`, `List` and the like for other
  whole expressions, `EXISTS can only be used in ExecuteTransaction write
  requests.` for an operator, comparison or call, and DynamoDB's syntax
  messages where one breaks off or carries on (`Unexpected token after
  expression` for `INSRT INTO ...`, `Expected RIGHT_BRACKET`, `Unexpected
  keyword` and others). `SET a = 1` and `REMOVE a` are refused for having no
  table, and a verb with nothing it needs after it (`DELETE` alone, `INSERT`
  with no `INTO`) is refused in DynamoDB's words.
- A PartiQL statement may end with a semicolon, as in DynamoDB, including
  after a `RETURNING` clause; a second statement after it is refused with
  `Unexpected token after semicolon. (Only one query is allowed.)`.
- A successful PartiQL INSERT, UPDATE or DELETE through `ExecuteStatement`
  returns an empty `Items` list unless it has a `RETURNING` clause, and an
  `ExecuteTransaction` of writes returns an empty `Responses` list, as DynamoDB
  does. They used to return no `Items` at all and one empty entry per
  statement.
- A PartiQL WHERE clause that stops where a condition should start is refused
  in DynamoDB's words: `Unexpected term` when nothing follows `WHERE`, `NOT` or
  an opening parenthesis, `Missing right-hand side expression of infix
  operator` when nothing follows `AND` or `OR`, and `Unexpected operator` or
  `Unexpected keyword` when an operator or a clause keyword stands in its
  place. It used to say `Expected condition in WHERE`.
- PartiQL runs `FROM "T" WHERE ... SET ...` (or `REMOVE ...`) as an UPDATE, as
  DynamoDB does, where it used to say `Expected data manipulation`, and an
  UPDATE of either form takes any number of SET and REMOVE clauses in any
  order. An UPDATE or DELETE takes its key only from equalities among the WHERE
  clause's top-level conditions: a key named only under an `OR`, a `NOT` or an
  `IN`, or no WHERE clause at all, is refused with `Where clause does not
  contain a mandatory equality on all key attributes`, and a key given two
  values with `Multiple conditions on same key`. `pk = 'a' AND pk = 'b'` used
  to update the first item it named. In a transaction, the duplicate-item
  check takes the same key: `(pk = 'b' OR x = 1) AND pk = 'a'` beside another
  statement on `a` is refused as two operations on one item, and beside one on
  `b` it isn't; the check used to read the key from the OR group. Captured
  against eu-west-2 and us-east-1.
- A PartiQL WHERE clause takes `attribute_exists(path)` and
  `attribute_not_exists(path)`, in any case and compared with a boolean, as
  DynamoDB does; both used to fail with `Unknown operator: (`. An operand that
  isn't a document path, or the wrong number of operands, is refused with
  DynamoDB's messages. A call to a function DynamoDB doesn't recognise there,
  `EXISTS(path)` among them, is refused as `Unrecognized function: <name> at
  <line>:<column>:<length>` before the table is looked up; `EXISTS(path)` used
  to run as `attribute_exists`. An OR with no key condition under it no longer
  counts as a second key range for the overlap check, so `pk = 'a' AND (x = 1
  OR x = 2)` runs. A condition function compares with a path, a size, a
  parameter or another call as the boolean it is, with `=`, `<>` or in an
  `IN` list, written on either side (`b = attribute_exists(a)`,
  `? = attribute_exists(a)`, `size(s) = attribute_exists(a)`) and as a
  member of an `IN` list (`b IN [attribute_exists(a)]`), so
  `attribute_exists(a) = b` holds where `b` is the boolean the call is, and
  is unequal to a missing value or one of another type. Each such comparison
  is one condition, so ten `attribute_exists(x) <> y` ANDed run, and
  `pk = 'a' AND attribute_exists(x) = y` reads partition `a` by its key and
  takes an `ORDER BY`. Its `IN` list reads a bare name as an attribute, as
  every other `IN` list does, where a name used to be compared with the
  string of itself. Captured against eu-west-2 and us-east-1.
- PartiQL's `set_add` and `set_delete` work in an UPDATE's SET clause, as in
  DynamoDB, where they used to fail with `Unexpected token after expression`.
  A malformed call is refused with DynamoDB's message and position before the
  table is looked up, members of the wrong type with its incorrect data type
  message, and `<<>>` anywhere with `Empty bags are not supported`. A function
  call in an INSERT value is refused as an unsupported data type.
- A PartiQL number parameter is read the way DynamoDB reads it: an exponent or
  a scale outside a 32-bit integer is `The parameter cannot be converted to a
  numeric value: <text>`, and `1e2147483647` is an underflow, in every position
  and on every PartiQL operation. They used to be range-checked as an item's
  numbers are, as an overflow or underflow by the exponent's sign.
- A PartiQL WHERE clause's `IS` tests NULL, BOOLEAN, BLOB, STRING, NUMERIC,
  STRUCT, TUPLE and LIST as well as MISSING, and `IS NOT` their complement, as
  DynamoDB does; only MISSING used to work. Another type name is refused with
  DynamoDB's `Unsupported data type for IS operator` message, and `INT4`,
  `INT8`, `DOUBLE PRECISION` and `CHARACTER VARYING` read as type names. An
  operator where a value should be, as in `pk = LIKE`, is `Unexpected
  operator`; `pk = LIKE` used to compare with the string `LIKE`. A negated or
  `<>` sort key condition stops the overlap check, as `NOT ... BETWEEN`
  already did. `DELETE FROM` with no table is `Expected identifier for simple
  path`, and `SELECT * FROM` with none is `Unexpected term` or `Unexpected
  keyword`.
- A PartiQL WHERE clause takes `size(path)` and `attribute_type(path, type)`,
  which used to fail with `Unknown operator: (`, and compares any two operands
  as DynamoDB does: a literal or parameter on the left (`'a' = pk` names the
  item an UPDATE or DELETE changes), two paths, two literals, and `size()` on
  either side of a comparison, BETWEEN or IN. A size counts a string's UTF-16
  code units, a binary's bytes and a collection's members. A comparison with a
  missing attribute, or with the size of a value that has none, is true for
  `<>`, as it is in a condition expression; it used to be false. An unknown
  `attribute_type` type, the wrong number of operands, the same path on both
  sides of a comparison, `MISSING` outside `IS`, an ordering operator on a
  condition function and a size standing alone as a condition are each refused
  with DynamoDB's message before the table is looked up, and a comparison with
  nothing after its operator is `Missing right-hand side expression of infix
  operator`. A size takes a number's type checks: a boolean under an ordering
  operator is refused, BETWEEN refuses bounds that are out of order, and a
  string compared with a size is simply unequal (captured against eu-west-2
  and us-east-1). BETWEEN also refuses bounds of different types, as it does
  for a path. A BETWEEN bound or an IN member can be a path or a size as well
  as a value: `size(s) IN [n]` compares with the attribute `n`, where it used
  to compare with the string `'n'`, and `n BETWEEN size(s) AND 10` parses
  (captured against eu-west-2 and us-east-1 for a size on either side; a
  path between path bounds follows the same reading, not yet captured).
- A PartiQL `IN` with one value on a key attribute is read as `=`, as
  DynamoDB reads it: `WHERE pk IN ['a']` reads item a by its key and is
  charged what `pk = 'a'` is, where it used to scan the table, and two
  statements naming the same item that way are duplicates in a batch or a
  transaction (captured against eu-west-2 and us-east-1).
- PartiQL refuses a bag it can't store with DynamoDB's messages and positions:
  one that starts with a string at its first member that isn't one, one that
  starts with a number without a position, and a parameter, `null` or Ion
  literal among its members with their own messages. These used to be `Mixed
  types in ... set literal` or a parse error. An Ion literal between backticks
  is refused by its type where it stands; `X'...'` is a syntax error, as in
  DynamoDB, rather than a name and a string; and a list, map or bag whose
  members aren't separated by commas is a syntax error too.
- A PartiQL keyword such as `inner` or `value` can't be a path's dot component
  unless it is quoted (`m."inner"`), as in DynamoDB; it used to be read as an
  attribute name. Any call in a SELECT list is refused as `Unexpected path
  component` at the function's name, and inside ExecuteTransaction with the
  statement's index; only `COUNT` used to be refused, and without the index.
  `EXISTS(SELECT ...)` in a WHERE clause is refused as `Unsupported token in
  expression: SELECT`.
- When a PartiQL statement has more than one problem, the one reported is the
  one DynamoDB reports: an unrecognised function, an unsupported `IS` type, a
  subquery and a literal it can't store come in the order written, WHERE clause
  first, and ahead of an operand refusal.
- PartiQL refuses a `begins_with` operand that isn't a string or binary, and
  BETWEEN bounds of different types or with the lower above the upper, with
  DynamoDB's messages, `NOT` or not, before the table is looked up.
- A PartiQL INSERT of a key that already exists is reported as a duplicate
  before an item nested to the limit is.
- `BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and
  `TransactWriteItems` refuse a `ReturnConsumedCapacity` other than `INDEXES`,
  `TOTAL` or `NONE` with the ValidationException the single-item operations
  already return, before anything is read or written. They used to accept any
  value and treat it as `NONE`, and the two writes went ahead.
  `TransactWriteItems` now checks its `TransactItems` length, the length of
  its `ClientRequestToken` and then `ReturnConsumedCapacity` before looking the
  token up, so a replayed token is checked too. `BatchWriteItem` checks the
  capacity mode ahead of a table name that fails its pattern, as DynamoDB does.
- `BatchWriteItem` and `TransactWriteItems` refuse a
  `ReturnItemCollectionMetrics` other than `SIZE` or `NONE` before writing
  anything. They used to accept any value. A request with more than one of
  these member errors, such as both enums, or an overlong
  `ClientRequestToken` beside either, now gets them in one
  `N validation errors detected:` message, as DynamoDB gives them.
- A malformed table name in a transaction is a top-level ValidationException
  naming the action's position and type, such as
  `transactItems.2.member.update.tableName`, checked before any action runs.
  `TransactGetItems` used to report it as a cancellation reason, and both
  reported the path as `tableName`. The name must now be 3 to 255 characters,
  as DynamoDB requires inside a request body.
- `BatchGetItem` and `BatchWriteItem` report a malformed table name as a
  constraint on the `RequestItems` map's keys, echoing the map as DynamoDB
  does, and `BatchGetItem` reports it together with an unknown
  `ReturnConsumedCapacity`. An empty request list in `BatchWriteItem`, and an
  empty `Keys` list in `BatchGetItem`, use DynamoDB's messages too.
- A `BatchWriteItem` put whose item has no partition key is refused with "The
  provided key element does not match the schema", as DynamoDB does, where it
  named the missing key.
- A `TransactWriteItems` item with no action is refused with DynamoDB's
  "Invalid Request" message, and one with two actions with "TransactItems can
  only contain one of Check, Put, Update or Delete". An item carrying both a
  `Put` and a `Delete` used to run the `Put` and drop the `Delete`.
- A `TransactGetItems` item without its `Get` is refused with DynamoDB's
  "Value null at 'transactItems.N.member.get' failed to satisfy constraint:
  Member must not be null", together with an unknown
  `ReturnConsumedCapacity`. It used to come back as serde's own "missing
  field" text.
- The single-item operations, `Query`, `Scan` and `SearchVectors` follow the
  member validation most regions now run. DynamoDB is moving each operation to
  a newer validation layer region by region; these match the majority of seven
  regions captured on 2026-10-09, which is the newer layer everywhere except
  `Scan`.
  - A table name of one or two characters is refused for its length before
    the table is looked up, where it used to answer
    `ResourceNotFoundException`. A name that breaks more than one constraint
    gets one error on most operations, both on `UpdateItem` and `Scan`.
  - An empty table name gets the newer "Value at 'TableName'" wording on its
    own, except on `Scan`.
  - `PutItem` and `DeleteItem` report a table-name error together with the
    other member errors; `UpdateItem` reports it alone; `Scan` reports it only
    when nothing else is wrong.
  - `UpdateItem` without its key reports every invalid enum, in the older
    per-value wording, followed by the missing key, as all seven regions do.
  - An unknown `ReturnValues` gets the newer layer's message, which names
    neither the value nor the member.
  - An unknown `ReturnValuesOnConditionCheckFailure` is refused, on the
    single-item operations and on every `TransactWriteItems` action. It used to
    be accepted.
  - `UpdateItem` reports one invalid enum, in DynamoDB's order, without the
    envelope for `ReturnValuesOnConditionCheckFailure` and
    `ReturnConsumedCapacity`.
  - `Query` reports a `Limit` of 0 and an empty name together, ahead of
    everything else, and checks `Select` after mixing `ConditionalOperator`
    with an expression. `Scan`
    reports its member errors in DynamoDB's order.
  - A `ConditionalOperator` is refused unless it joins two or more legacy
    `Expected`, `QueryFilter` or `ScanFilter` conditions, with DynamoDB's
    messages, and joining two or more with a value other than `AND` or `OR`
    gets DynamoDB's generic enum message. Beside an expression it gets the
    mixing message, which `DeleteItem` now wraps in the envelope as `PutItem`
    and `UpdateItem` do. It used to be accepted with nothing to join. `Scan`
    still checks its value as an enum.
  - `SearchVectors` refuses a short or malformed table name without the
    envelope once its other members pass, a `TopK` below 1 in the newer
    wording, and a missing `IndexName`, `SearchVector` or `TopK` with
    DynamoDB's own "missing field" message.
- A request carrying an attribute value nested more than about 60 levels is
  read and refused with DynamoDB's nesting `ValidationException`, rather than
  failing to parse with a bare `SerializationException`. As in DynamoDB, an
  `ExpressionAttributeValues` entry is named in the message and a member such
  as a bad table name or enum is still reported first. Past about 2,000
  levels, where DynamoDB itself stops reading the request, it stays a bare
  `SerializationException`.
- A key, `ExclusiveStartKey`, or legacy `Expected`, `AttributeUpdates`,
  `ScanFilter`, `QueryFilter` or `KeyConditions` value nested 32 or more levels
  is refused with DynamoDB's nesting `ValidationException` before the table is
  looked up. A key used to be reported as not matching the schema, a
  transaction was cancelled, and a legacy condition or filter was evaluated.
  As in DynamoDB, the message sits inside the validation envelope on
  DeleteItem's and UpdateItem's key, `Expected` and `AttributeUpdates`, and
  after the starting-key prefix on a Scan's `ExclusiveStartKey`, and a
  `ScanFilter` value is refused from 33 levels.
- A Query's `ExclusiveStartKey` that doesn't fit the key schema is refused
  with "The provided starting key is invalid" alone, as DynamoDB does, where it
  used to add "The provided key element does not match the schema". An invalid
  value in it (`NULL: false`, an empty set) is reported without the
  starting-key prefix, and a start key from another partition or outside the
  sort key condition is refused with DynamoDB's messages rather than paged
  from.
- A legacy `KeyConditions` value whose type isn't the key's schema type is
  refused with "Condition parameter type does not match schema type", and a
  BETWEEN whose two values differ in type with "AttributeValues inside
  AttributeValueList must be of same type". A map used to be reported as
  "Cannot convert partition key value to string".
- A PartiQL parameter nested 33 or more levels is refused before its statement
  is parsed or its table looked up, as DynamoDB does: at the top level on
  ExecuteStatement and ExecuteTransaction, where a transaction used to be
  cancelled, and against that statement alone in a BatchExecuteStatement. It
  used to run, or come back as "Invalid key value".
- A BatchExecuteStatement member that fails on its own, for a parameter
  nested too deep or holding an empty attribute name, takes no part in the
  batch's read/write and duplicate checks, as DynamoDB does: a read beside a
  write whose parameter is too deep, or two updates of one item where one is,
  now return 200 and run the rest, where the whole batch used to be refused
  (captured against eu-west-2 and us-east-1). The same is applied to a member
  refused for a parameter value DynamoDB can't store or for a COUNT
  projection, not yet captured.
- Set members come back in DynamoDB's order instead of the order they were
  written in. String and binary sets are sorted by their bytes everywhere.
  Number sets are sorted by value from GetItem, BatchGetItem, Query and the
  ReturnValues of PutItem, UpdateItem and DeleteItem, and come back in the
  order of a Java hash set from Scan, TransactGetItems, the PartiQL
  operations, stream records and transaction cancellation reasons, as
  DynamoDB returns them. Sets are now stored sorted, and a row stored before
  this change comes back sorted in responses on the wire (HTTP, MCP and wasm);
  the Rust API returns such a row's sets as they were stored. An update that
  leaves such a set alone no longer fails the type check for an index key the
  set's attribute is named after.
- An empty attribute name is refused with DynamoDB's `Empty attribute name`
  before the table is looked up: in an item or a key, as a map key at any
  depth inside a value, in AttributesToGet, in the legacy Expected,
  AttributeUpdates, QueryFilter and ScanFilter values, in a PartiQL
  parameter, and as an ExpressionAttributeNames value. It used to be stored,
  or ignored. PutItem, UpdateItem and DeleteItem report it inside the
  validation envelope, as DynamoDB does.
- CreateTable lists a GSI's member errors before an LSI's, so two too-short
  index names come back in DynamoDB's order.
- `ItemCollectionMetrics` reports `SizeEstimateRangeGB` as DynamoDB does: a
  lower and an upper bound in whole gigabytes, so a small item collection
  reads `[0.0, 1.0]`. It used to give the collection's exact size twice.
- PutItem, UpdateItem and DeleteItem report more of their request-validation
  errors inside the `1 validation error detected: ` envelope, as DynamoDB does:
  `ExpressionAttributeNames` or `ExpressionAttributeValues` without an
  expression, either map empty, an empty `ConditionExpression`, and every
  malformed legacy `Expected` condition. The values message no longer names
  the missing expressions after it. DeleteItem now envelopes the same families
  PutItem does, including `{"NULL": false}` wherever it sits, and refuses a
  `ReturnValues` other than `ALL_OLD` or `NONE` with DynamoDB's
  `ReturnValues can only be ALL_OLD or NONE`, before the table is looked up.
- A legacy `Expected` value DynamoDB refuses, such as `{"NULL": false}` or a
  number it can't store, is refused on PutItem, UpdateItem and DeleteItem.
  The condition used to be dropped and the write went ahead.
- A number DynamoDB can't store, and a set with a repeated member, are
  refused in the words of the validation layer each operation runs on, as
  DynamoDB refuses them. PutItem, UpdateItem and DeleteItem report them inside
  the validation envelope wherever the value sits, and GetItem, BatchGetItem
  and Query report them bare; an empty number ends `numeric value: `, a
  duplicated string or binary set lists its members quoted, and expression
  values carry no `ExpressionAttributeValues contains invalid value` wrapper.
  BatchWriteItem, the transactions, Scan and PartiQL keep the older wording,
  read numbers as DynamoDB's older layer does (`1e2147483647` is an underflow
  there) and name the key of a bad expression value. The transactions refuse
  an empty or duplicated set in any action up front, before a missing table or
  a condition's syntax error, rather than cancelling. A legacy `Expected`
  value and an `AttributeUpdates` value are checked as an item's are. A
  PartiQL number set literal with a repeated value is refused naming its
  members and path, in a WHERE clause too.
- Expressions containing non-ASCII characters are read as DynamoDB reads
  them. A character such as `é`, `€` or an emoji outside a string is refused
  with the syntax error naming that character, in every expression and in
  transactions, and so is a no-break space, which DynamoDB doesn't treat as
  whitespace. A placeholder whose own name holds one is refused as an invalid
  key of its map, and a PartiQL statement with one outside a string as not
  well formed. Captured against eu-west-2 and us-east-1; us-east-1 names an
  emoji in a filter or a transaction as `?`, and Dynoxide follows eu-west-2.
  In 1.2.1 some of these expressions stopped the server instead of being
  refused, so anyone running Dynoxide where other people's clients can reach
  it should upgrade.
- A `TransactWriteItems` Update whose SET paths overlap, such as
  `SET a.b = :v, a = :m`, is refused before anything is written, with
  DynamoDB's "Two document paths overlap" message, where it used to be
  applied. `UpdateItem` names the two paths in the order they were written.
  Captured against eu-west-2 and us-east-1.
- A PartiQL statement longer than 8,192 characters, or an empty one, is
  refused before it is parsed, as a request member with DynamoDB's message
  (`Member must have length less than or equal to 8192`), on
  `ExecuteStatement`, `BatchExecuteStatement` and `ExecuteTransaction`. The
  limit counts characters, not bytes. Captured against eu-west-2 and
  us-east-1.
- A `BOOL` or `NULL` member's value is read as DynamoDB reads it. The
  strings `true`, `t`, `yes`, `y` and `1`, and `false`, `f`, `no`, `n` and
  `0`, in any case, are taken as the booleans they spell, so `{"BOOL": "no"}`
  stores `false` and `{"NULL": "yes"}` stores a NULL, where both used to be
  refused. Any other string is refused with `Unexpected token received from
  parser`, a number with `NUMBER_VALUE` (or `DECIMAL_VALUE`) `cannot be
  converted to Boolean`, and a list with a `SerializationException` that has
  no message. A number is named by how it is written: a whole number of any
  size, `-0` included, is `NUMBER_VALUE`, and one with a point or an exponent
  `DECIMAL_VALUE`, where `-0` and an integer past 64 bits were named
  `DECIMAL_VALUE`. A `NULL` that reads as false is refused bare as the value is
  read, and named by key in an expression value on `UpdateItem`, `DeleteItem`
  and `Query`, when it was a string or sits beside another member; a lone
  `{"NULL": false}` keeps the envelope. A member set to JSON `null` counts as
  absent, so `{"S": null}` is an empty value and `{"S": "a", "N": null}` a
  string, and `Query` names the key of an empty expression value. Captured
  against eu-west-2 and us-east-1.
- A request that holds a `BOOL` or `NULL` written as a string, or a value with
  more than one member, is read whole by DynamoDB's older validation layer,
  whatever the operation, and Dynoxide now reads it the same way: every value
  it refuses, wherever it sits, comes back bare on `PutItem`, `UpdateItem` and
  `DeleteItem`, where the same refusal alone is wrapped in `1 validation
  error detected:`, and `UpdateItem` names the key of the expression value at
  fault (`ExpressionAttributeValues contains invalid value: ... for key :v`).
  A value with more than one member is checked in DynamoDB's order: a member
  that doesn't read as a boolean, then an empty set, then a number it can't
  read, then a `NULL` that reads as false, and only then the second member,
  where the empty set came after the second member. Captured against
  eu-west-2 and us-east-1.
- A request's own `ConsistentRead`, on `GetItem`, `Query`, `Scan` and each
  table of `BatchGetItem`, and `Query`'s `ScanIndexForward`, read a string by
  the rule a `BOOL` member follows, as DynamoDB reads them: `"yes"` is a
  strongly consistent read and `"no"` reads a Query descending, where every
  string was refused with `Unexpected token received from parser`. Any other
  string still is, and a number is named as it is for a `BOOL`. A `null`
  `ScanIndexForward` reads forward, as leaving it out does, where it was
  refused. Captured against eu-west-2 and us-east-1.
- A PartiQL statement that reads or writes by a key longer than a key can be
  is refused with DynamoDB's words, where it used to run and find nothing: a
  partition key over 2,048 bytes with `Size of hashkey has exceeded the
  maximum size limit of2048 bytes`, and a sort key over 1,024 bytes with
  `Aggregated size of all range keys has exceeded the size limit of 1024
  bytes`. That covers a SELECT on the table or on either kind of index, a
  sort key under any key condition (`=`, a comparison, `BETWEEN` or
  `begins_with`), a partition key beside an `OR` that every alternative
  shares (`pk = <key> AND (x = 1 OR x = 2)`), and the key of an `UPDATE` or a
  `DELETE`; a statement that names several partitions, such as an `OR` of
  keys, still runs. The refusal stands
  on its own on `ExecuteStatement`, against that statement (with its table)
  in a `BatchExecuteStatement`, where it is now charged nothing, and as a
  `ValidationError` cancellation in an `ExecuteTransaction`. Captured against
  eu-west-2 and us-east-1.
- A PartiQL WHERE clause with more than 300 operators is refused with
  DynamoDB's "The expression contains too many operators" message. AND, OR,
  NOT, comparison operators, function calls, `IN`, `BETWEEN` and `IS` each
  count, the NOT in `IS NOT` or `NOT IN` on its own, so `AND size(s) > 0`
  and `AND x IS NOT NULL` cost three; `BETWEEN`'s own AND, an `IN` list's
  members and a bag literal's `<<` and `>>` count nothing. A run of NOTs no
  longer counts as nesting, so one that was refused as nested too deeply now
  runs. Captured against eu-west-2 and us-east-1.
- PartiQL accepts `x NOT IN [...]` and `x NOT BETWEEN lo AND hi`, read as
  `NOT x IN [...]` and `NOT x BETWEEN lo AND hi`, so an item missing `x`, or
  holding a value of another type, matches a numeric range's negation; both
  were refused as an unknown operator. They also follow a `size()`, a literal
  or a parameter, and `NOT IN` follows a condition function, where `NOT
  BETWEEN` takes the same refusal as `BETWEEN`. A sort key read with `NOT
  BETWEEN` reads the range's complement, as `NOT sk BETWEEN` does. A missing
  AND in a WHERE clause's `BETWEEN` is now "Expected AND keyword", and `x NOT`
  followed by anything but `IN`, `BETWEEN` or `LIKE` is an unexpected token
  rather than an unknown operator. Captured against eu-west-2 and us-east-1.
- A PartiQL `IN` or `BETWEEN` on a path that also stands among its own
  members or bounds, such as `x IN [1, x]` or `x BETWEEN 1 AND x`, is refused
  with DynamoDB's "The first operand must be distinct from the remaining
  operands" message, as `x = x` already was. The message names a path by its
  parts, `[m, a]` for `m.a` and `[l, [0]]` for `l[0]`, in a comparison too.
  Where a WHERE clause holds more than one refusal, `MISSING` compared
  without `IS` now outranks a refused operand whichever is written first, and
  an `IN` with too many operands is reported in the order written against a
  refused operand rather than ahead of it. An `IN` with too many operands
  that also holds its own path is refused for the path, wherever it stands
  in the list, where the operand count used to be reported. Captured against
  eu-west-2 and us-east-1.
- A PartiQL `UPDATE` that sets or removes a key attribute is refused with
  `Cannot update attribute <name>. This attribute is part of the key`, and
  writes nothing. It used to store the item under its old key with the new
  key value inside it, so the item no longer matched its own key. It is
  refused before the item is read, so a missing item or a false condition
  makes no difference. In `BatchExecuteStatement` it fails its own statement
  with a `ValidationError` that names the table, and in `ExecuteTransaction`
  it cancels the transaction with a `ValidationError` reason. Captured against
  eu-west-2 and us-east-1 on 10 October 2026.
- A quoted name in a PartiQL statement is one attribute whatever it holds,
  as DynamoDB reads it: `"a.b"` and `"a[0]"` name the top-level attributes
  `a.b` and `a[0]` in a WHERE clause, a condition function, `IS NOT MISSING`
  and a projection, which returns them under those names, and `SET "x.y" =
  5` writes an attribute named `x.y`. They used to be read as `a.b` and
  `a[0]` unquoted, a step into `a`, and `SET "x[0]"` was refused as an
  invalid document path. `"a"."b"` and `"a"[0]` still step into `a`. A key
  attribute whose name holds a `.` or a `[` is named by quoting it, so such
  a table is read by its key and its items can be updated and deleted.
  Captured against eu-west-2 and us-east-1.
- PartiQL `size()` calls nested inside one another, and arithmetic after a
  `size()`, are held to the same depth as nested brackets.
- A PartiQL `IN` with more than 100 operands, negated or not, written as
  literals or as parameters, after a path or a `size()`, is refused with
  DynamoDB's "The IN operator is provided with too many operands" message.
  Captured against eu-west-2.
- A PartiQL WHERE clause that repeats a long value across the alternatives
  it expands to, such as eight `(a = 1 OR a = 2)` pairs alongside a
  4,000-character string, runs as it does on DynamoDB. The engine holds each
  condition once however many alternatives share it, and checks the size of
  an expansion before building it. Captured against eu-west-2.
- A PartiQL condition whose `?` has no parameter to fill it no longer
  matches an item. `flag <> ?` with no parameters used to hold for every item
  without `flag`, so a `DELETE` or `UPDATE` short of its parameters could
  delete or change it.
- A PartiQL statement with more `?` placeholders than the request has
  parameters is refused with DynamoDB's "Number of parameters in request and
  statement don't match." message: on its own on `ExecuteStatement`, against
  that statement alone in `BatchExecuteStatement` (where it no longer counts
  towards the mixed read and write check), and with the statement's index on
  `ExecuteTransaction`. More parameters than placeholders are still accepted.
  An empty `Parameters` list is refused as a request member, on a batch or
  transaction member too, where it refuses the whole request.
  Captured against eu-west-2 and us-east-1.
- A PartiQL request that breaks several member constraints at once, such as
  an over-long statement with an empty `Parameters` list or several over-long
  statements in a batch, lists every one in DynamoDB's "N validation errors
  detected" form rather than the first, and an empty statement in a batch or
  transaction refuses the whole request as a member. `ReturnConsumedCapacity`
  is listed with the rest rather than refused on its own: on
  `BatchExecuteStatement` ahead of the statement list's length, and on
  `ExecuteTransaction` after an over-long `ClientRequestToken` and the
  statements (both orders captured against eu-west-2 and us-east-1). More
  than 25 statements in a batch is refused as a member too, where it used to
  give "Too many statements in BatchExecuteStatement"; its echo of the
  statements is us-east-1's, as eu-west-2 printed a hash in their place that
  can't be reproduced. A batch member's own errors follow
  `ReturnConsumedCapacity` (captured). Inferred rather than captured: an empty
  transaction, or one of more than 100 statements, listed with the token and
  the statements rather than answering after them. The MCP `execute_partiql`, `batch_execute_partiql`
  and `execute_transaction_partiql` tools list a bad mode the same way.
- A transaction's list longer than DynamoDB allows is echoed as DynamoDB
  echoes it: `ExecuteTransaction`'s statements as `ParameterizedStatement(...)`
  and an empty list as `[]`, and the actions of `TransactWriteItems` and
  `TransactGetItems` as `TransactWriteItem(...)` and `TransactGetItem(...)`,
  every field named, where they echoed nothing or Dynoxide's own debugging
  form. An attribute map in any of these echoes, and in `BatchWriteItem`'s,
  lists its entries in the order of a Java hash map sized to its entries, as
  DynamoDB's do, rather than by name: an item, a nested map or
  `ExpressionAttributeNames` of 2 to 6 entries is laid out over four or eight
  buckets, not sixteen. Entries that share a bucket are listed in name order,
  where DynamoDB keeps the order they were sent in. Captured against
  us-east-1, eu-west-1 and us-west-2 (eu-west-2 echoes no action list), the
  map sizes against us-east-1 and eu-west-2.
- A PartiQL WHERE clause's alternatives are held to 16,384 condition slots
  between them, where each alternative was already held to 4,096. Every slot is
  evaluated against every item read, and a clause of a few hundred operators
  could flatten to tens of thousands. The limit is Dynoxide's own, as DynamoDB
  does not flatten a clause; a clause past it is refused as too complex.
- A refused member echoed in a constraint message (`BatchWriteItem`'s and
  `BatchGetItem`'s request items, `BatchExecuteStatement`'s statements, and the
  action list `TransactWriteItems` and `TransactGetItems` echo when it holds
  more than 100) stops at 1 MiB and ends `...`. The Java form these echoes take runs to about
  twenty times the request, so a rejected request near the body limit built
  an error message of hundreds of megabytes. The limit is Dynoxide's own;
  what DynamoDB does with an echo that size has not been captured.
- A malformed `Expected` condition on `PutItem`, `UpdateItem` or `DeleteItem`
  is refused rather than ignored. 1.2.1 ran the write with no condition at
  all, so a guarded `DeleteItem` deleted the item. An unknown `ComparisonOperator` gets
  DynamoDB's "Invalid comparison operator" message, and `EQ` with an empty
  `AttributeValueList` its "Invalid number of argument(s)" message. Captured
  against eu-west-2 and us-east-1.
- The `Debug` form of a map `AttributeValue` writes its entries in key order,
  and the idempotency check on transactions sorts a map's keys itself rather
  than rely on serde_json to, so a build in which another crate turns on
  serde_json's `preserve_order` feature still replays a same-token retry of
  an identical request.
- PartiQL `begins_with` matches a binary attribute against a binary prefix by
  its bytes, and `NOT begins_with` keeps the rest. A binary prefix matched
  nothing and its negation everything. A string prefix still matches only a
  string value. Captured against eu-west-2 and us-east-1.

## [1.2.1] - 2026-09-20

### Security

- **`rmcp` moves to 2.2.0**, clearing GHSA-9pj6-vhgr-3mwh, GHSA-33f5-2c5q-wgwj
  and GHSA-9g45-5xwm-f3wc against the 1.x line. None of the three was reachable
  from a Dynoxide build: two cover client transports this crate never compiles,
  and the session-table leak sits behind rmcp's `stateful_mode`, which the MCP
  HTTP transport sets to `false`. 1.8.0 is the end of the 1.x line, so there was
  no backport to take. The MCP protocol version on the wire is unchanged.
- **`rustls` moves to 0.23.45** for RUSTSEC-2026-0285. It reaches the tree
  through dev-dependencies only and never shipped in a released artefact.

### Changed

- The crate is published to crates.io with Trusted Publishing. The release
  workflow exchanges a short-lived OIDC token for a publish token that expires
  after 30 minutes, so no long-lived registry token is stored on the repository.

## [1.2.0] - 2026-09-11

The crate moves to `dynoxide-rs` 2.0.0 because `ImportCommand` gained a
field. Everything else a user installs carries 1.2.0; see the version split
under Added.

### Behaviour changes

- **`hash` rules now use HMAC-SHA256 keyed on the salt. Every hashed value
  changes.** `SHA256(salt || value)` ran salt and value together as one byte
  string with no boundary between them, and inherited SHA-256's length
  extension. The value is tagged with its DynamoDB type, so the string `"123"`
  and the number `123` no longer share a pseudonym, and a map, list or set is
  hashed over a canonical encoding, so its pseudonym no longer depends on the
  order it was serialised in. A dataset anonymised by 1.1.0 will not join
  against one anonymised by this release: re-import both sides, and rotate the
  salt while you are there.
- **One value has one pseudonym however it is spelled.** `1`, `1.0` and
  `0.1e1` are one number to `hash`, to a seeded `fake` and to the consistency
  map, as they are one number to DynamoDB, and a duplicated set member no
  longer changes a set's pseudonym. The map keeps types apart, so `"42"` and
  `42` are separate entries, and a seeded `fake` on a map, list or set goes
  through the map rather than being treated as deterministic.
- **`dynoxide import` exits 3 when an original value reached the output.**
  That covers a rule that anonymised nothing, a key its template could not
  rebuild, a key the model never templated that still holds a replaced value,
  items matching no entity, keys of an index the model never names on a table
  whose model names an index it lacks, and values a `mask` kept whole. They
  are listed again at the end of the run, `--serve` and `--mcp` refuse to
  start on them, and `--accept-exposure` returns the exit code to 0.
  `ImportSummary` gains a `notices` field carrying every message with its
  concern, and an `exposures` field holding the messages of the ones that
  count.
- **`salt_env` and `seed_env` must be at least 16 bytes.** Generate one with
  `openssl rand -base64 24`. The salt is wiped from memory once the run is
  done with it, as the encryption key already was.
- **`fake` on a numeric attribute draws from the full 64-bit range** rather
  than four digits, so a few hundred items no longer collide.
- **A misspelt table name is an error.** A rule scoped to a table the schema
  file does not have, and a `--tables` name the export does not hold, are
  refused rather than skipped; either used to leave a table out of the output
  with a clean exit. `--tables` also applies to a flat export directory, where
  the filter was ignored before.
- **A rule path is read in full or refused.** `a[0]b` parsed as `a[0]` and
  rewrote the whole list element; it is now an error suggesting `a[0].b`.
- **An export file over the 50 GB limit fails the import** rather than ending
  quietly at the limit, and the cap applies to uncompressed files too. A line
  over 4 MB is refused without first being held in memory.

### Added

- `dynoxide import --data-model <onetable.json>` rebuilds keys from their
  OneTable templates after anonymisation, so `pk = "CUSTOMER#<email>"` and
  every index key built from `email` take the anonymised value and keep their
  prefix. A key is rewritten only where its template reproduces the value it
  arrived with; anything else is left alone and counted. Templates follow
  OneTable's forms, including dotted paths and `${name:length:pad}` padding.
  A model describes one table, so with rules a multi-table export needs
  `--tables` naming the one it describes
  ([#201](https://github.com/nubo-db/dynoxide/issues/201)).
- An import fails when the anonymisation breaks a join: one original key
  taken to two values, by two entities or by one entity across rows that
  shared a partition. Put the attribute in `[consistency] fields`, or use
  `hash`. A split sort key is reported rather than fatal, and `docs/import.md`
  lists what the check cannot see
  ([#202](https://github.com/nubo-db/dynoxide/issues/202)).
- `fake` rules take `seed_env`, naming a secret that makes the generated value
  a function of the original, so a fixture only changes where the source data
  changed. Holds for a fixed build and for scalar values; mixing seeded and
  unseeded rules on one consistency field is reported
  ([#203](https://github.com/nubo-db/dynoxide/issues/203)).
- A rule can name the `tables` it applies to. A rule scoped to tables a run
  leaves out with `--tables` is reported as not applied rather than as having
  anonymised nothing.
- Rules take `values` and `names` tables, in the shape of
  ExpressionAttributeValues and ExpressionAttributeNames, so a match can be
  scoped by key prefix or reach a reserved-word attribute. The rules file is
  validated before any data is read
  ([#200](https://github.com/nubo-db/dynoxide/issues/200)).
- `import --mcp` tells the agent what the import did. `get_database_info`
  returns the run's notices with their concern, and the server instructions
  say how many there were and whether an exposure was accepted with
  `--accept-exposure`. `docs/mcp.md` covers it.
- The crate version and the product version are separate streams. `VERSION`
  holds the product version and every installable artefact reports it;
  `Cargo.toml` holds the crate version. `cargo install dynoxide-rs --version`
  now selects the crate version. `docs/versioning.md` has the rules and
  `scripts/check-versions.sh` enforces them in CI.
- New warnings, counted where a count is possible: a rule that matched nothing
  or rewrote nothing; keys a template could not rebuild; values a `mask` kept
  whole; a rule that rewrites a key or index key directly with no data model;
  an item several entities' templates reproduce; one entity splitting its own
  sort key; a data model supplied with no rules.

### Changed

- Prefer `hash`, or a seeded `safe_email`, for an attribute a key is built
  from. The other `fake` generators repeat within a few hundred items, `mask`
  collides on shared tails, `redact` collapses every item onto one key and
  `null` cannot render a key at all. The up-front warning names the
  consequence per action.
- An import that rebuilds keys, or whose rules name a key attribute, maintains
  secondary indexes on write so an overwritten row leaves no stale index
  entry. Other imports keep the faster path.

#### Breaking (Rust API)

- `ImportCommand` gains a `data_model` field, so struct literals need it
  adding. This is why the crate is 2.0.0.

### Fixed

- `contains` against a number set compares numerically, as DynamoDB does, so
  `contains(tags, :n)` with `:n` as `1` matches a set holding `1.0`. The
  evaluator is shared, so this holds in a `FilterExpression` and a
  `ConditionExpression` alike, not only in an import rule.
- `safe_email` no longer collides at ordinary sizes: addresses carry a 64-bit
  derived suffix in the local part.
- `${name:length:pad}` matches OneTable's rendering for a multi-character pad,
  counting in UTF-16 units as JavaScript does.
- The check for a replaced value left inside a key finds it inside a redacted
  map or list, in another case, and behind any delimiter, and counts only a
  value a rule actually replaced.
- Under `--continue-on-error`, the last batch of a table is covered, and a
  batch that rolled back no longer counts towards the join check, the
  collision count or an exposure: only rows that reached the output do.
- `import --mcp` honours `--mcp-read-only`, as `--serve --mcp` already did.
- Local secondary indexes in the schema file reach the key rebuilder, which
  reports them rather than skipping them, along with any OneTable index the
  table lacks. An index whose hash key is a plain attribute keeps its sort key
  template, and a schema keyed on `PK`/`SK` parses its primary key templates.
- Overwritten rows are counted on the number the database stores, so `1` and
  `1.0` count as the one row they are.
- A rule `path` starting with `#` is rejected rather than silently matching
  nothing, and an empty salt variable is rejected rather than producing
  unsalted SHA-256. A salt or seed that is set but not valid UTF-8 is reported
  as such rather than as unset.
- Compression happens before the rename, so a failed compression leaves nothing
  at the output path.
- `docs/import.md` no longer shows `begins_with(pk, 'USER#')`, which the
  parser rejects; use the `values` table
  ([#200](https://github.com/nubo-db/dynoxide/issues/200)).
- Release pipeline: the container image is version-probed before push, the
  Homebrew formula's test block runs before the tap moves, the website waits on
  both npm packages, prereleases publish with a dist-tag and install through
  the Action, `npm/scripts/publish.sh` refuses a version and release URL that
  name different tags, the npm job checks out the tag it publishes, and a tag
  whose version is not full semver (`v1.2.0-rc..1`) is refused before anything
  is published rather than by npm afterwards.

## [1.1.0] - 2026-09-03

### Behaviour changes

- **`BatchGetItem` with an empty `RequestItems` map now answers the standard
  validation envelope** rather than a bespoke sentence: `1 validation error
  detected: Value at 'RequestItems' failed to satisfy constraint: Member must
  have length greater than or equal to 1`. AWS moved the operation onto that
  message in 32 of the 33 regions observed on 2026-09-02. `BatchWriteItem` has
  not followed and keeps `The requestItems parameter is required for
  BatchWriteItem`, so the two siblings answer differently on purpose.

## [1.0.0] - 2026-08-25

Every artefact moves to 1.0.0 together: the crate, the npm CLI wrapper and its
platform binaries, the browser engine, the container images and the MCP registry
entry. `docs/versioning.md` states what that number promises, what forces a
major, and how conformance fixes are versioned from here.

The browser engine drops its `-preview` suffix and joins the shared version. Its
conformance coverage is unchanged by that; `docs/versioning.md` names what the
suite does and does not exercise.

### Behaviour changes

Read this section before upgrading. This release corrects a large number of
divergences from DynamoDB, and a test that had come to depend on the old answer
will fail. Grouped by what you will actually experience.

**A write can now modify data it previously did not.** These three share one
cause: PartiQL `UPDATE` and `DELETE` evaluate their `WHERE` clause through the
same matcher as `SELECT`, so a change to predicate handling changes which rows a
write touches.

- A predicate on a set, list, map, binary or null attribute compares by value
  instead of never matching. A conditional write on any of those types could
  never fire before and now fires.
- A negated comparison keeps the rows its attribute is missing from. `NOT a='x'`
  skipped rows with no `a` and now matches them, so a write carrying that
  predicate reaches a wider row set.
- Parenthesised grouping and bare `NOT` now parse. `WHERE (a='1')` was a parse
  error, so a write carrying it did nothing; it now executes. Unlike the two
  above, the old behaviour was a visible error rather than a silent no-op.

**Requests that used to be accepted are now rejected**, matching DynamoDB. A
test sending one of these and expecting success fails immediately: the
`UpdateItem` item-size ceiling; the PartiQL 400KB item-size limit;
`ReturnConsumedCapacity` outside `INDEXES`, `TOTAL` and `NONE`; two
`BatchExecuteStatement` and `ExecuteTransaction` request shapes; five rejections
on index-qualified PartiQL `SELECT`; a `SELECT` in `BatchExecuteStatement` that
does not name the primary key or that names an index; ordering comparisons
against operands with no ordering; an index-qualified `SELECT` inside
`ExecuteTransaction`.

One runs the other way and is worth knowing: an ordering comparison against an
unorderable operand used to answer no rows and now rejects the statement, so a
conditional write of that shape fails visibly instead of silently doing nothing.

**Consumed capacity figures move.** Any test asserting exact `ConsumedCapacity`
values will need updating. Index writes are charged on the change to what the
index stores rather than on the finished item, LSI writes are charged at all,
table arms are sized on the larger of an item's before and after images,
transactional and PartiQL surfaces report per-index figures under `INDEXES`,
PartiQL reads are charged on rows walked rather than rows returned, and LSI
base-table reads are charged on bytes moved rather than a flat half unit.

**Results change without an error.** A PartiQL `SELECT` is served from the index
its `FROM` clause names, so sparse membership and projection now apply; an LSI
serves a projection naming an attribute it does not carry by reading the base
item, where it used to return rows of empty objects.

**Crashes became errors.** An unterminated quoted name and a deeply nested
statement are rejected rather than aborting the process, and `wasm-sqlite`
alongside the default features now fails with a message that explains itself.

Every individual change is listed in the sections below, and
`docs/compatibility-summary.md` carries the cumulative behaviour-change record.

### Added

- **Vector indexes and `SearchVectors`.** `CreateTable` and `UpdateTable` take vector index definitions, `DescribeTable` reports them, the item write paths maintain them, and `SearchVectors` returns the nearest entries to a query vector over `COSINE`, `EUCLIDEAN` or `DOT_PRODUCT`. A search can be scoped by a `SearchSchema` HASH attribute and filtered on `INLINE_FILTER` attributes, and reports `VectorSearchRequestBytes`; a write to a vector-indexed table reports a per-index `VectorIndexes` map under `INDEXES`. Available on the native build, the wasm build, and through the MCP tools, which gain a `search_vectors` tool. Scoring is exact brute-force KNN, so the top-k is the true top-k.

  An index added to a live table through `UpdateTable` walks a creation lifecycle rather than arriving ready: it walks the same four phases DynamoDB walks. While it allocates, the base table reports `UPDATING`, `Backfilling` reports `false`, and an `UpdateTable` that cancels the index is refused with the phase to retry in; then it backfills with the table back to `ACTIVE`, the index still `CREATING` and `Backfilling` `true`, where a cancel is accepted; then it reports `ACTIVE` while still refusing searches with `Cannot search backfilling vector index: <name>`; then it answers. A `DeleteTable` is refused for both creating phases. The one online index limit goes with it, and is per table rather than per call: a second index cannot start creating while the first still is, whether the two arrive in one `UpdateTable` or in separate ones. Only a delete of the creating index gets through, which is how a create is cancelled. That reproduces the shape DynamoDB has, including the trap that `ACTIVE` arrives before the index will answer, so a readiness check written against Dynoxide behaves the way it will against the real thing. The durations are not AWS's: the backfill is synchronous here and the window is tens of seconds rather than minutes. An index created as part of `CreateTable` is `ACTIVE` and searchable at once, on both. `docs/compatibility-summary.md` has the detail and a check that works against both.

- The `execute_partiql` MCP tool accepts `ConsistentRead`. Omitting it was harmless while the field only chose a rate; it now also decides whether a select qualified by a GSI is rejected, so an agent had no way to reach either behaviour.

- `BatchExecuteStatement` accepts `ReturnConsumedCapacity` and reports capacity, which it previously had no way to do at all. Per-table entries with index arms, aggregated across the batch. A failed statement is still charged the write it attempted, sized on the larger of the row already stored and the item it carried; a batch in which nothing succeeds reports no capacity, and a table whose statements all failed is omitted entirely. Captured against eu-west-2.

### Changed

- The declared MSRV is now 1.88, which is what the crate has actually needed since the vector index and PartiQL work landed. `rust-version` had read 1.85 since 0.9.5 with nothing verifying it, so the requirement moved while the declaration stayed put: let chains in the action layer and `rmcp`'s reach into `darling 0.23` both want 1.88, and building 1.0.0 on 1.85 would have failed. Raising the floor also unblocked the let-chain suggestions clippy had been suppressing against the old one, so the shipped tree collapses around a hundred nested conditionals into let chains across the action, expression, PartiQL, MCP and import paths. A CI job now compiles against whatever `rust-version` declares, so raising the floor has to be a deliberate edit.

- **Breaking (Rust API):** `storage_backend::StorageBackend` is sealed. It stays public and can still be named and used as a bound, which is what `Database<S>` and the wasm dispatch functions need, but only Dynoxide's own backends implement it. The trait's method set tracks what the engine can do rather than a stable interface, and vector index support alone added seven methods to it. Left open, each such addition would have been a major. `docs/versioning.md` carries the reasoning; unsealing is a minor if third-party backends ever earn their place.

- The wasm surface runs the same pre-deserialisation type checks as the HTTP server, so a mistyped field answers with the captured `SerializationException` on both rather than raw serde text on one. Affects every operation carrying such a check.

- **Breaking (behaviour):** numbers are sized the way DynamoDB sizes them, so consumed capacity moves. Captured byte-exact against eu-west-2 by bisecting the 400KB gate: a number costs one byte, plus `ceil(integer significant digits / 2)` and `ceil(fraction significant digits / 2)`, over the value with leading and trailing zeros trimmed. The old measure was wrong in both directions. It charged for zeros DynamoDB trims, so `0.0000001` was sized at 5 bytes rather than 2; it rounded the halving down where DynamoDB rounds up, so every number with an odd count of significant digits was a byte light; and it never charged for a minus sign, which costs a byte, so every negative number was light too. Both fed consumed capacity on every write, `table_stats`, the query and scan byte budgets, and index capacity, so those figures change. An item sitting on the 400KB boundary can flip either way with it.

- **Breaking (behaviour):** `UpdateItem` measures the limit the way DynamoDB does, which is not the way a put is measured. It applied the same ceiling to the finished item as `PutItem`, where DynamoDB leaves the key attributes out of the figure and charges a fixed amount per action: three bytes for the update, nineteen for each action that writes a value, two for each that clears one, and nothing for the value written, the attribute name, or the expression text. With a one-character key that puts an update's ceiling nineteen bytes below a put's, so an item can be reachable by `PutItem` and out of reach by `UpdateItem`. Updates in that band used to succeed and now fail. Two forms are charged less than DynamoDB charges them, so the engine still accepts a little more rather than less: assigning through a list index costs a byte more, and an arithmetic assignment such as `SET n = n + :one` costs fifty more. The legacy `AttributeUpdates` parameter is charged on the same shapes, inferred from the expression form rather than captured separately.

  The finished item is held to the 400KB limit as well, so the rule is `max(item, item - key attributes + action cost)`. Taking the key out never buys back more than the actions cost: once the key attributes reach that cost the item's own size binds, and the ceiling sits flat at 400KB however long the key gets. With a 20-byte key the two meet exactly. Measured across key lengths from 1 to 1,024 bytes, DynamoDB never accepts a finished item above 409,600.

- **Breaking (behaviour):** `TransactWriteItems` reports an oversized item as DynamoDB does, where both actions used to come back as a cancellation carrying the put wording. A put's size is knowable from the request, so it is now rejected up front with a top-level `ValidationException` before the transaction opens; an update's depends on the stored item, so it stays a `ValidationError` cancellation reason, now reading `Item size to update has exceeded the maximum allowed size`. Code matching on the error type for an oversized put in a transaction sees a `ValidationException` where it saw a `TransactionCanceledException`. A transacted update is measured flat against the resulting item, without the key exclusion and per-action cost the standalone `UpdateItem` carries, so an item `UpdateItem` refuses can still be written inside a transaction. Captured against eu-west-2.

- **Breaking (behaviour):** the PartiQL write surfaces hold the 400KB item-size limit, where they did not check it at all. `INSERT` and `UPDATE` computed an item's size, handed it to storage and never compared it to anything, so `ExecuteStatement`, `BatchExecuteStatement` and `ExecuteTransaction` would store an over-limit row and report its size in table statistics. Both are measured flat against the item, captured against eu-west-2 at a ceiling of 409,600, and report in DynamoDB's words: `Item size has exceeded the maximum allowed size` for an insert, `Item size to update has exceeded the maximum allowed size` for an update. A PartiQL update is not charged the key exclusion and per-action cost that the standalone `UpdateItem` carries, so the two surfaces genuinely differ on the same finished item. Statements that used to succeed now fail.

- **Breaking (behaviour):** the three PartiQL surfaces reject a `ReturnConsumedCapacity` outside `INDEXES`, `TOTAL` and `NONE`, as `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query` and `Scan` already did. They derived their requests rather than validating them, so a typo was read as `NONE` and reported nothing. It now also decides whether a write sizes its indexes, so the same typo quietly skipped that too. A request carrying an unrecognised value used to succeed and now fails.

- **Breaking (behaviour):** `BatchExecuteStatement` and `ExecuteTransaction` now reject two request shapes they used to accept, matching DynamoDB. A batch or transaction may not mix reads and writes, and may not name the same item twice, reads included. Both are rejected up front with a top-level `ValidationException` before any statement runs; a request that previously succeeded in either shape now fails. An unparseable member is unaffected and still reports against itself while the rest of the batch runs. Captured against eu-west-2.

  The four checks cost about 7% on a 25-statement `BatchExecuteStatement`, measured against `958e340`.

#### Breaking (Rust API)

The wire API and the CLI, server and MCP surfaces are unaffected by all of these.

- `StorageBackend` gained seven required methods for vector indexes: `create_vector_table`, `drop_vector_table`, `update_vector_index_definitions`, `insert_vector_items`, `delete_vector_item`, `query_vector_candidates` and `vector_items_for_keys`. `VectorItemRow` and `VectorCandidateRow` are new. A backend outside the crate must add all seven.
- `types::ConsumedCapacity` gained `vector_indexes`, and `actions::index_capacity::WriteCapacity` and `TableCapacity` each gained `vector_bytes`. A struct literal needs `..Default::default()` or the existing constructor.
- `partiql::parser::Statement` variants are `#[non_exhaustive]`, and `Select`, `Update` and `Delete` carry `index_name`. A downstream `match` needs a `..`.
- `partiql::parser::WhereClause` is `#[non_exhaustive]` and carries `wrote_or`.
- `partiql::parser::WhereCondition` is `#[non_exhaustive]` and gained a `NotComparison` variant.
- `actions::batch_execute_statement::BatchStatementRequest` is `#[non_exhaustive]`. Build it from `Default` and assign, or deserialise it.
- `actions::describe_table::execute`, `delete_table::execute`, `update_table::execute` and `search_vectors::execute` take a further `&actions::vector_lifecycle::VectorIndexLifecycle`, which is where the vector index creation lifecycle lives. `Database` owns one and passes its own; a caller driving the actions directly can construct one with `VectorIndexLifecycle::new()`.
- `wasm_api::DispatchContext::new` takes a `&VectorIndexLifecycle` alongside the token caches, so a dispatch reports and enforces the lifecycle the way the native surfaces do.
- `Database::memory_with_clock` is new: an in-memory engine reading time from a caller-supplied `Clock`, so a test outside the crate can drive the vector index creation window without waiting on it. The window's two thresholds are `actions::vector_lifecycle::ACTIVE_AFTER_SECS` and `SEARCHABLE_AFTER_SECS`.
- `partiql::executor::statement_target` is removed. Use `statement_target_in`, which takes a table the caller has already resolved.
- `partiql::executor::execute_page` takes three further arguments: `consistent_read`, `capacity_mode` and an optional `resolved` table.
- `partiql::parser::parse` returns `Result<Statement, ParseError>` rather than `Result<Statement, String>`.
- `WASM_PREVIEW` is renamed `WASM_BACKEND`. It named a maturity level on a build that is no longer a preview, and 1.0.0 freezes the constant into the version contract, so renaming it later would have cost a major. The value and meaning are unchanged: `true` under `wasm-sqlite`, `false` otherwise.
- `partiql::parser::WhereClause::from_conditions` and `from_groups` are removed. Use `from_groups_written`, which also takes whether the clause as written joined anything with `OR`.
- `partiql::parser::CompOp` is a re-export of `expressions::condition::CompOp` rather than its own enum.


### Fixed

- The write paths no longer disagree about the 400KB item-size limit ([#187](https://github.com/nubo-db/dynoxide/issues/187)). A number was sized by counting the digit characters of whatever string it held, and storage expands scientific notation, so the same item measured one size as it arrived and another once stored. `PutItem` checked the limit before that expansion and the other three after it, which is how an item of 8000 attributes holding `N "1E125"` passed `PutItem`, was refused by `BatchWriteItem`, and was then recorded at 552,004 bytes against a 409,600 limit. The recorded size feeds table statistics and the `ItemCollectionMetrics` estimate. Sizing a number by its significant digits makes the figure survive storage, so where each path checks no longer matters. The surfaces that measure differently on purpose, and the two that were not checking at all, are covered in the behaviour changes above.

- `ConsumedCapacity` under `INDEXES` charges index writes on the change to what an index stores, not on the item the write leaves behind ([#176](https://github.com/nubo-db/dynoxide/issues/176)). Each index used to be charged a unit whenever the finished item belonged to it, which is right for a plain insert and wrong for most else:

  - LSI writes are charged. The `LocalSecondaryIndexes` arm never appeared before, and its units never reached the total.
  - An identical overwrite is free, and a write outside a GSI's projection no longer charges that GSI.
  - Moving an index key costs two writes; removing one costs a single delete.
  - `DeleteItem` charges an index only when the deleted item was in it.

  Units are sized on the projected index entry rather than the base item: an insert or delete costs its own image, an in-place update the larger of the two, and a key move both halves rounded separately. `BatchWriteItem` inherits all of it. Two of the old figures over-reported rather than under-reported, so any downstream correction for the old numbers needs removing. Captured against eu-west-2.

- `PutItem`, `UpdateItem` and `BatchWriteItem` size the table arm on the larger of the item's before and after images, matching DynamoDB. They used to size it on the finished item, so shrinking a 3KB item to 100B reported one unit against DynamoDB's three. Items under 1KB are unaffected, and `DeleteItem` already sized on the old image.

- `PutItem` sizes the item after normalising it rather than as it arrived. Normalising expands scientific notation, so an item carrying `1E100` grows on the way to storage; the table arm was measuring the request and the stored size was recording it, while the same item read back measured larger. The same write reported one figure as an insert and another as an overwrite, and disagreed with `BatchWriteItem`, which already sized after normalising.

- An overwrite that reorders a set's members is free, as it is on DynamoDB. Sets are unordered, so re-listing the same members leaves an index's stored view alone, but they are held in order internally and the comparison read that as a change, charging every index projecting the set.

- Enabling `wasm-sqlite` alongside the default features now fails with a message that says so. Cargo adds the default features to whatever a manifest lists, so `dynoxide-rs = { version = "0.13", features = ["wasm-sqlite"] }` also enabled the native backend and the CLI, and the build stopped on three type errors naming neither the feature nor the conflict. The combination was never valid and still is not; what was missing was the diagnosis. Enable the wasm backend with `default-features = false`.

- `TransactWriteItems`, `ExecuteStatement`, `ExecuteTransaction` and `BatchExecuteStatement` report per-index `ConsumedCapacity` under `INDEXES`, where they used to report a `Table` arm and nothing else ([#178](https://github.com/nubo-db/dynoxide/issues/178)). The transactional 2x factor applies to the base table arm alone: an index arm inside a transaction costs what the same write costs outside one. Index units fold into the total, so totals grow on an indexed table under `TOTAL` as well.

- The transactional table arm is sized on the larger of the item's before and after images rather than on the request payload. `Delete` and `ConditionCheck` carry a key and no item, so both used to be sized on the key: deleting a 3KB item reported 2 units against DynamoDB's 6. Invisible below 1KB. A `ConditionCheck` writes nothing and is still charged on the image it read.

- A same-token `TransactWriteItems` or `ExecuteTransaction` replay is charged against the images the first call was sized on, at 4KB read granularity. It used to recompute from the request, which carries no item for a `Delete` or a `ConditionCheck`, so a replayed delete of a 9KB item reported 2 against DynamoDB's 6.

- A PartiQL `SELECT` is served from the index its `FROM` clause names ([#179](https://github.com/nubo-db/dynoxide/issues/179)). `SELECT * FROM "table"."index"` used to discard the index and scan the base table, so it returned items the index does not contain and a name matching no index quietly succeeded. The qualifier tokenises as three tokens and the table name parser took one, which left the rest for the next clause to parse against, so the `WHERE` went with it. That single mis-parse cost `UPDATE` its `SET` clause and made `DELETE` report requiring a `WHERE` clause it plainly had.

  The read now follows the index: sparse membership excludes items the index does not hold, and a `KEYS_ONLY` or `INCLUDE` projection returns what it projects and no more. Capacity lands on the index arm with the table arm at zero, matching `Query` and `Scan`. Continuation tokens carry the base table key, so rows sharing an index key are no longer skipped, and a token is bound to the index that minted it.

  Five rejections come with it, each with DynamoDB's own wording: an unknown index name, a `FROM` path of more than two components, an empty path component, a strongly consistent read of a GSI, and a qualifier on a write statement. A GSI rejects a projection naming an attribute it does not carry; either kind rejects a filter on one, but only when the read is keyed on the index partition key, because an unkeyed read is a scan and a scan matches nothing. Captured against eu-west-2.

- A PartiQL predicate on a set, list, map, binary or null attribute compares by value instead of never matching ([#186](https://github.com/nubo-db/dynoxide/issues/186)). PartiQL carried its own comparison covering strings, numbers and booleans, and answered every other type from a catch-all returning false for `=` and true for `<>`. The same code gates `UPDATE` and `DELETE`, so a write conditioned on any of those types could never fire. Sets compare without regard to order, lists in order, maps on their key set. PartiQL now shares the condition-expression engine's comparison rather than carrying a second copy of it. Captured against eu-west-2: the two surfaces agree on every type.

- An unterminated quoted name is rejected instead of panicking the parser. `SELECT * FROM "` sliced a one-character string from index 1, which panics, and the release profile aborts on panic, so a single malformed statement took the process down. Predates the index qualifier, which added a second way to reach it.

- `BatchStatementRequest` carries `ConsistentRead` ([#183](https://github.com/nubo-db/dynoxide/issues/183)) and `ReturnValuesOnConditionCheckFailure` ([#184](https://github.com/nubo-db/dynoxide/issues/184)). Both were absent, so a member setting either was parsed as though it had not.

  `ConsistentRead` is per member and sets the rate that member's read is charged at: a keyed batch `SELECT` costs 0.5 without it and 1 with it, and a batch mixing the two sums both rates. It does not change which rows come back.

  `ReturnValuesOnConditionCheckFailure` is accepted and inert, which is what DynamoDB does with it. A member whose condition fails returns the same response whether it is `ALL_OLD`, `NONE` or absent, and never carries the item; the same option on a `TransactWriteItems` `ConditionCheck` does return it, which is what rules out a bad measurement. The field is deserialised so a client setting it meets a field dynoxide knows rather than one it drops. Captured against eu-west-2.

- A `SELECT` inside `BatchExecuteStatement` must name the table's primary key, and may not name an index. Both shapes were accepted and served; DynamoDB rejects each against that member while the rest of the batch runs, with the same message. An index-qualified batch read is therefore unreachable even when it does name the key.

- A batch member whose statement ran and failed echoes its table, where one rejected before it ran does not. `ConditionalCheckFailed` and `DuplicateItem` carry `TableName` and a `ValidationError` does not, which is what an invalid `RETURNING` variant is.

- Parenthesised grouping and `NOT` work in a `WHERE` clause. Both were documented as supported and neither was: the clause parser was a flat OR of ANDs, so even `WHERE (a='1')` was a parse error, and `NOT` was recognised only inside `NOT EXISTS` and `NOT BEGINS_WITH`. `AND` binds tighter than `OR`, and a `NOT` over a group is applied by De Morgan. A clause whose flattened form exceeds 256 alternatives is rejected as too complex rather than expanded.

- An ordering comparison rejects an operand whose type has no ordering. DynamoDB orders `S`, `N` and `B`, and rejects `<`, `<=`, `>`, `>=` and `BETWEEN` against anything else before it resolves the table; dynoxide answered no rows. `=` and `<>` are unaffected, being defined for every type. Captured against eu-west-2.

- An index-qualified `SELECT` inside `ExecuteTransaction` is rejected, as it is on DynamoDB, rather than served and charged to the base table arm.

- A PartiQL read is charged on the rows it walked rather than the rows it handed back. DynamoDB sizes a read before the `WHERE` clause and before the projection, so a `SELECT` matching one row costs what one matching every row costs, and naming one attribute costs what naming all of them costs. `Query` and `Scan` already did this; the PartiQL executor summed the rows it was about to return, so any filtered or projected statement under-reported. Captured against eu-west-2.

- A statement nested deeper than the parser can walk is rejected rather than taking the process with it. Around 250 nested parentheses in a `WHERE` clause, or a few thousand leading `NOT`s, overflowed the stack, and the release profile aborts on panic, so a statement of about a kilobyte was enough to stop the host. Nested list and map literals reached the same end by a second route. Both descents now carry a depth budget and reject past it, as an over-complex clause already was.

- A negated comparison keeps the rows its attribute is missing from. `NOT a='x'` answered on rows holding an `a` and skipped the rest, because negation flipped the operator and `<>` is false on a missing path, while `NOT CONTAINS` wrapped instead and kept them. The two disagreed with each other on the same data. Negation now wraps in every form, so a row the comparison cannot answer for is a row the negation matches.

- A `SELECT` inside `BatchExecuteStatement` naming a table that does not exist reports the missing table. It reported that the statement must specify a primary key in the where clause, which it had, because the check that reads a member's target cannot tell a missing key from a missing table and only the reads went through it. The same batch reported a missing table correctly for a write and misleadingly for a read.

- An LSI read that reaches back to the base table is charged on the bytes each base read moved, not a flat half unit apiece. The flat figure came from one capture of three small rows and holds up to 4KB. Captured again across item sizes: the same three rows at 9KB each cost 4.5 rather than 1.5, and 9 under `ConsistentRead`. A `SELECT *` against an `INCLUDE` index does not reach back at all, and did not before.

- Two pieces of repeated work on the write and batch paths are gone. Index writes were sized on every write, and `ReturnConsumedCapacity` defaults to `NONE`, where the response builders discard the figure, so a default write projected the old image of every index for nobody. An overwrite against a table with two indexes now builds two projected entries rather than four. Separately, a batch resolved each statement's table metadata and parsed its key schema twice, and a failing statement a third time; both are per-table facts, so a batch resolves each table once however many statements it carries. A 25-statement batch goes from 51 metadata loads and 51 key schema parses to one of each, which matters most on the wasm backend, where every load crosses the bridge to a JS worker and nothing caches the result.

- An LSI serves a projection naming an attribute it does not carry by reading the base item, which is what DynamoDB does and a GSI cannot. dynoxide returned rows of empty objects. The base reads land on the table arm at read granularity apiece, so three rows served this way report total 2, table 1.5, lsi 0.5.

## [0.13.0] - 2026-07-30

### Added

- PartiQL now works in the browser. The wasm engine serves `ExecuteStatement`, `BatchExecuteStatement` and `ExecuteTransaction`, where it previously answered all three with a 501. Behaviour matches the native build statement for statement, including the `RETURNING` projections, the per-statement error codes `BatchExecuteStatement` reports, and `ClientRequestToken` idempotency on `ExecuteTransaction`. The engine's advertised capability list grows from 14 operations to 17; `CONTRACT_VERSION` is unchanged at 1, since adding an operation is additive, so a pinned client keeps working. The engine bundle grows by about 143 KB raw and 53 KB gzipped, which is the parser and executor now being reachable from the wasm entry points.

### Changed

- **Breaking (Rust API):** `wasm_api::dispatch` and `wasm_api::dispatch_http` take a `DispatchContext`, which borrows the idempotency caches a `ClientRequestToken` needs. Both are behind the `wasm-sqlite` feature, so a native library consumer is unaffected, and the `#[wasm_bindgen]` surface the npm package calls (`open`, `execute`, `dispatchHttp`, `capabilities`, `contract_version`) is unchanged. Callers of `dispatch` build a context from the new public `TokenCaches`. This is separate from `CONTRACT_VERSION`, which does not move.
- The wasm build's documentation no longer describes it as unverified. The preview label stays, and now rests on what remains unimplemented - `TransactWriteItems`, streams, tags and TTL - rather than on an absence of test data.

### Fixed

- PartiQL `SELECT` on `ExecuteStatement` now paginates: `NextToken` is honoured and returned, and `Limit` bounds the rows evaluated rather than the rows matched, matching DynamoDB and the existing Query and Scan semantics. A filtered `SELECT` with a `Limit` can therefore return fewer rows than before for the identical request, and a page can come back short or empty while still carrying a `NextToken`, so callers should follow the token rather than treat a short page as the end of the result. A `SELECT` bound to a partition key now reads in ascending sort-key order, and its sort-key conditions are pushed into the read, so `Limit` paces against rows in the key-condition range rather than the whole partition; a condition a key condition cannot express falls back to the partition-wide read and filters as before. A `NextToken` is now rejected when replayed against a different statement or different `Parameters`, not just a different table, where it previously resumed the walk and silently skipped rows; the mismatch carries DynamoDB's `NextToken does not match request` message (captured eu-west-2) while an undecodable token keeps `Invalid NextToken`. `Limit: 0` is rejected with the message DynamoDB's ExecuteStatement returns (the Scan-shaped wording, also captured); it previously read nothing and returned no token, making a paginated walk look complete.
- PartiQL `SELECT COUNT(*)` is removed. It was a dynoxide-only extension: real DynamoDB rejects the projection outright, so a statement that worked locally broke in production. A `COUNT(...)` projection now returns DynamoDB's exact `ValidationException`, the bare `Unexpected path component` message with the 1-based position of the `COUNT` token (captured eu-west-2), fired before the table-existence check, on `ExecuteStatement`, `BatchExecuteStatement` and `ExecuteTransaction` alike. Note this removes a previously shipped dynoxide feature rather than fixing it in place; there is no replacement, matching DynamoDB, where counting means paging the rows yourself or using Query/Scan with `Select: COUNT`.
- The wasm engine refuses a `CreateTable` carrying an enabled `StreamSpecification` or `Tags`, and an `UpdateTable` carrying a `StreamSpecification`, before creating or changing anything, with the same typed `UnsupportedOperation` envelope and 501 status the unimplemented operations use. Previously the table was created first and the stream or tag step then failed with a 500, so an AWS SDK retried the error and surfaced `ResourceInUseException` for the half-created table; the conformance suite's streams probe scored that as a failure where it now correctly records a skip. Every backend capability refusal (`BackendError::Unsupported`) now maps to this envelope rather than to a 500 `InternalServerError`, so a client - and an SDK retry policy - can tell a scope gap from a server fault.

## [0.12.0] - 2026-07-24

### Changed

- **Breaking (Rust API):** the public `partiql::parser::Statement` enum gained a `returning` field on its `Update` and `Delete` variants, and both variants are now `#[non_exhaustive]`; the public `actions::batch_execute_statement::BatchStatementResponse` struct gained a `table_name` field and is now `#[non_exhaustive]` too. Library consumers that construct or exhaustively match these types must add `..`. This is a source-breaking change for the crate's public API, so the next release is a minor bump (0.12.0). The DynamoDB wire API and the CLI/server/MCP surfaces are unaffected.

### Added

- PartiQL now honours the `RETURNING` clause on `ExecuteStatement`, where dynoxide previously parsed the statement but silently dropped the clause. `DELETE ... RETURNING ALL OLD *` returns the deleted item in `Items` (a present but empty `Items` array on a missing target, matching DynamoDB rather than the classic `DeleteItem` path), and `UPDATE ... RETURNING <ALL|MODIFIED> <OLD|NEW> *` returns the matching projection of the item; the `MODIFIED` variants return only the changed paths (a nested `SET a.b` returns just the changed leaf, not the whole `a` attribute), exclude the primary key, and return an empty `Items` array when nothing was projected. `BatchExecuteStatement` honours a member's `RETURNING` clause; `ExecuteTransaction` rejects one with a top-level `ValidationException`. The `RETURNING` variants DynamoDB does not allow on `DELETE` (`MODIFIED OLD *`, `ALL NEW *`, `MODIFIED NEW *`) are rejected with its exact validation message instead of being ignored ([#137](https://github.com/nubo-db/dynoxide/issues/137)).
- **A test-only HTTP server for the wasm engine, `npm run wasm:serve`.** It exists so the conformance suite can reach the browser build over a socket. It is not a way to run dynoxide and is deliberately not distributed: not in the release binary, not on npm. To run dynoxide, use the native build. It drives the shipping `dist/` bundle in a headless Chromium it installs itself and serves DynamoDB JSON-1.0 on port 8003, with one browser page's worth of concurrency and no TLS. Each start is a fresh in-memory database. The engine gained a `dispatchHttp` worker op so the wire envelope is decided there instead of in the transport, and an operation the preview does not implement returns HTTP 501. See [docs/wasm.md](docs/wasm.md).
- `dynoxide serve` and `dynoxide` (no-subcommand) now accept a `--schema` flag, taking the same DynamoDB DescribeTable JSON format as `import --schema`. On startup, dynoxide creates each table defined in the file and skips any that already exist. This lets you pre-populate an empty database (in-memory or persistent) with the correct table structure without running an import first.

### Fixed

- `OnDemandThroughput` now follows real DynamoDB's semantics on every surface, captured against eu-west-2: CreateTable and UpdateTable both reject it when the effective billing mode is PROVISIONED (each operation with its own captured wording, naming the first present member, read checked first), members must be at least 1 (`-1` is valid only on UpdateTable, where it removes that ceiling), a partial UpdateTable object merges member-wise over the stored ceilings instead of replacing them, the UpdateTable response echoes the merge with `-1` kept verbatim while DescribeTable reports the post-removal state, and switching billing mode to PROVISIONED clears the stored ceilings. The billing gate fires before the range check on both operations, and an `OnDemandThroughput` object with no members is treated as absent: real DynamoDB accepts one at creation and returns `InternalFailure` for one on UpdateTable, which dynoxide deliberately replaces with its deterministic no-change validation error rather than emulating a 500. dynoxide previously stored whatever it was given, on any billing mode, and replaced wholesale on update ([#159](https://github.com/nubo-db/dynoxide/issues/159)).
- The MCP `describe_table` default view now includes `billing_mode`, `table_class` and the capacity settings for the mode the table is in (`provisioned_throughput` or `on_demand_throughput`), where it previously showed none of the table configuration fields and only the `raw: true` view carried them.
- The MCP `create_table` and `update_table` tools now accept `on_demand_throughput`, so the on-demand ceilings that HTTP and wasm could already set and round-trip are reachable over MCP too ([#157](https://github.com/nubo-db/dynoxide/issues/157)).
- The MCP `update_table` tool now accepts `billing_mode`, `provisioned_throughput` and `table_class`, so a table can be switched between PROVISIONED and PAY_PER_REQUEST or moved to STANDARD_INFREQUENT_ACCESS over MCP, as it already could over HTTP and wasm. The handler previously hardcoded all three to none, so the engine saw an empty update ([#156](https://github.com/nubo-db/dynoxide/issues/156)).
- The MCP `create_table` tool now accepts `billing_mode` and `provisioned_throughput`; it previously had neither, so every table created over MCP was PROVISIONED with default throughput. An invalid billing mode is now rejected with DynamoDB's enum validation message on every surface, not just over HTTP ([#154](https://github.com/nubo-db/dynoxide/issues/154)).
- `import --schema` and `serve --schema` no longer drop `BillingMode` and `TableClass` from a DescribeTable response. DescribeTable wraps both in summary objects (`BillingModeSummary`, `TableClassSummary`) that the rebuilt `CreateTableRequest` never read, so an on-demand table came back `PROVISIONED` and `STANDARD_INFREQUENT_ACCESS` came back `STANDARD`. The schema path now unwraps both summaries and, when the billing mode came from the summary, drops the zeroed `ProvisionedThroughput` blocks DescribeTable reports for an on-demand table and its GSIs, which CreateTable would otherwise reject as zero capacity units. A table's own DescribeTable output round-trips without degrading or failing, a provisioned table's capacity values survive intact, and a schema already in CreateTable shape passes through untouched, so an inconsistent one still fails validation exactly as it would on the CreateTable API ([#140](https://github.com/nubo-db/dynoxide/issues/140)).
- PutItem and UpdateItem validation errors now carry DynamoDB's `1 validation error detected: ` envelope on exactly the request-validation families real DynamoDB envelopes: empty and duplicate sets, `{"NULL": false}` in any position (item body, key or expression attribute values), expression syntax and oversize errors, redundant parentheses, the distinct-operand rule for `contains`, expression parameter misuse (`ExpressionAttributeValues` without an expression, mixing `Expected` with `ConditionExpression`), and invalid `ReturnValues`. Data-plane, structural and limit families stay bare, matching DynamoDB: key and index-key type mismatches, cannot-update-key, invalid document paths, references to missing attributes, empty-string key values, empty or multi-typed `AttributeValue` objects, and oversized items. dynoxide previously enveloped only its constraint-collection path and left the rest bare. The split is classified per family at the raising site, never by matching message text, so an attribute value that echoes a bare-family phrase cannot shed its envelope, and the message is identical on every surface: HTTP, wasm, MCP and the in-process Rust API, whose errors gain the prefix for these families. Deserialisation failures on the wasm and MCP surfaces now classify the same way the HTTP server does, instead of leaking an internal marker inside a mis-typed `SerializationException`. Read operations are unchanged and their expression errors stay bare. Confirmed against real DynamoDB in eu-west-2.
- PartiQL `UPDATE` now performs real list-index writes. `SET tags[0] = :v` updates the list element (appending when the index is at or beyond the end) and `REMOVE tags[0]` deletes it and shifts the rest, where dynoxide previously treated `tags[0]` as a literal map key so both the stored item and a `RETURNING MODIFIED` projection over it diverged from DynamoDB. A `RETURNING MODIFIED` projection over list-index paths now packs the changed elements into a dense list in ascending index order (`SET a[0], a[2]` yields `{a: [v0, v2]}`), matching DynamoDB.
- `BatchExecuteStatement` now echoes `TableName` on each successful member response, and a member that fails to parse now carries the short-form `ValidationError` code (matching a per-statement execution error) instead of the long-form `ValidationException`. Both match DynamoDB.
- PartiQL `UPDATE` on a non-existent key now fails with `ConditionalCheckFailedException` (`The conditional request failed`) and creates nothing, where dynoxide upserted the item. `UPDATE` is not an upsert: the target must already exist, matching DynamoDB.
- PartiQL parse errors now use DynamoDB's message wording: `Statement wasn't well formed, can't be processed: <detail>` (previously `... got error: ...`), and a statement that does not begin with a DML keyword reports `Expected data manipulation`. Applies to `ExecuteStatement`, `BatchExecuteStatement`, and `ExecuteTransaction`.
- A `{ "NULL": false }` attribute value is now rejected with the `ValidationException` real DynamoDB returns (`One or more parameter values were invalid: Null attribute value types must have the value of true`), where dynoxide normalised it to `{ "NULL": true }` and accepted it. The `NULL` member must be exactly `true`; this specific input flipped behaviour across AWS regions and has since settled on rejection everywhere. The fix covers the item body and the `DeleteItem` raw expression-value path, where the rejection had surfaced as a mis-typed `SerializationException` leaking an internal prefix rather than the plain `ValidationException`. Confirmed against real DynamoDB in eu-west-2 ([#145](https://github.com/nubo-db/dynoxide/issues/145)).
- `UpdateTable` adding a global secondary index now validates the index's key attributes against the request's own `AttributeDefinitions`, where dynoxide resolved them from the merged stored set and so accepted a new index keyed on an existing table attribute the request did not re-declare. DynamoDB requires a new index's key attributes to appear in the request itself and rejects the omission with `One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions`. An unused definition supplied in the delta is still dropped, and a redeclared attribute still keeps its stored type. Confirmed against real DynamoDB in eu-west-2 ([#144](https://github.com/nubo-db/dynoxide/issues/144)).
- An expression parameter over 4096 bytes is now rejected with DynamoDB's `Invalid <Type>Expression: Expression size has exceeded the maximum allowed size` message, where dynoxide parsed it regardless of length. The length is measured on the raw string as sent, before name and value substitution. The guard covers every expression surface, each carrying the surface-specific `Invalid <Type>Expression:` prefix: `UpdateExpression`, `ConditionExpression`, `FilterExpression` (Query and Scan), `ProjectionExpression`, and `KeyConditionExpression`; on the key-condition surface the size check runs before parsing, so an oversized key condition is rejected for size even when otherwise malformed. Confirmed against real DynamoDB in eu-west-2 ([#146](https://github.com/nubo-db/dynoxide/issues/146)).

## [0.11.4] - 2026-07-17

### Fixed

- Passing a top-level argument together with a subcommand is now a hard parse error, where the argument was silently ignored. The top-level `--host`, `--port`, `--db-path` and `--encryption-key-file` exist for the bare pre-subcommand form (`dynoxide --port 8000`) and only fed the no-subcommand path, so `dynoxide --db-path data.db serve` started an in-memory server, never created the file, and the data was gone on exit with nothing said about it; `dynoxide --port 8893 serve` listened on 8000. Combining one with `serve`, `mcp`, `import` or `healthcheck` now fails up front with clap's conflict error naming the offending option, and the same option after the subcommand keeps working as before ([#141](https://github.com/nubo-db/dynoxide/issues/141)).

## [0.11.3] - 2026-07-05

### Security

- On Windows, the HTTP and MCP listeners now bind with `SO_EXCLUSIVEADDRUSE`, closing a hole where another process running as the same user could take over either port with `SO_REUSEADDR` while dynoxide was serving. Restarting immediately after a clean shutdown still works; a regression test covers the rebind, and CI now runs the unit tests on Windows ([#23](https://github.com/nubo-db/dynoxide/issues/23)).

## [0.11.2] - 2026-07-02

### Fixed

- A `CreateTable` request whose `StreamSpecification` sets `StreamEnabled: false` but also supplies a `StreamViewType` is now rejected with the `ValidationException` real DynamoDB returns (`One or more parameter values were invalid: Table is being created with a stream disabled, UpdateViewType should not be specified`), where dynoxide accepted it. A view type only has meaning when the stream is enabled, so the two cannot be combined at table creation ([#115](https://github.com/nubo-db/dynoxide/issues/115)).
- A `CreateTable` global or local secondary index using `ProjectionType: INCLUDE` without a `NonKeyAttributes` list is now rejected with the `ValidationException` real DynamoDB returns (`One or more parameter values were invalid: ProjectionType is INCLUDE, but NonKeyAttributes is not specified`), where dynoxide accepted it and created the table. `INCLUDE` projects the index key attributes plus an explicit list, so the list is mandatory; the shared projection validator now requires it, closing the gap for both index types ([#116](https://github.com/nubo-db/dynoxide/issues/116)).
- `Query` and `Scan` now return DynamoDB's exact message when `Select: SPECIFIC_ATTRIBUTES` is given with no `ProjectionExpression` or `AttributesToGet`, where dynoxide rejected the request correctly but with its own wording. Both carried the same non-AWS string; the corrected phrase is `Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES`, which `Query` wraps in the `1 validation error detected:` envelope and `Scan` returns bare, matching real DynamoDB ([#121](https://github.com/nubo-db/dynoxide/issues/121)).
- `TransactWriteItems` now reports top-level `ReadCapacityUnits` and `WriteCapacityUnits` in its `ConsumedCapacity`, where only the nested `Table` breakdown carried them. A transactional write reports write capacity (a standalone `ConditionCheck` costs 2 write units on its own table line under `INDEXES`), and a same-token idempotent replay now reports a recomputed transactional read cost, rounded at 4KB read granularity, rather than re-reporting the first call's write units relabelled as read. The two magnitudes diverge above 1KB (writes round at 1KB, reads at 4KB); for a ~1.5KB item the first call reports 4 write units and the replay 2 read units. The replay honours its own `ReturnConsumedCapacity` mode. Single-item operations are unchanged. Confirmed against real DynamoDB by the conformance suite.
- A `TransactWriteItems` call with a `ClientRequestToken` now holds the idempotency lock across the whole first call, closing a window where two concurrent same-token calls could both execute the transaction. The lock was previously released between the cache check and execution, so racing same-token calls each ran the transaction; the second now waits and replays the first's result. Transactions without a token are unaffected.
- PartiQL `ExecuteTransaction` now honours `ClientRequestToken` idempotency, where it ignored the token and re-applied the statements on every call. A same-token, same-statements call within the 600-second window replays the stored result without re-executing (a same-token call with different statements returns `IdempotentParameterMismatchException`), using the same hold-the-lock-across-execute guard as `TransactWriteItems` so concurrent same-token calls serialise rather than double-apply. The cache is separate from the `TransactWriteItems` one, since idempotency is scoped per API operation. `ExecuteTransaction` also now reports transactional `ConsumedCapacity` split by statement kind (write capacity for a write set, read capacity for an all-`SELECT` read set, and read on a replay) at 2 units per statement, replacing a flat 1-unit-per-statement estimate with no read/write split. Confirmed against real DynamoDB by the conformance suite.
- `GetItem`, `Query`, `Scan`, `BatchGetItem`, and `TransactGetItems` now reject an invalid `ProjectionExpression` before any item is read, where dynoxide validated it lazily per row. Overlapping paths (`a` and `a.b`), duplicate paths (`a` and `a`), and undefined expression-attribute names are rejected with DynamoDB's `Invalid ProjectionExpression:` messages, so a lookup that matches nothing still rejects rather than returning an empty result. Confirmed against real DynamoDB in eu-west-2.
- A `ProjectionExpression` selecting several indices of one list now returns them compacted and in ascending index order, where dynoxide returned them in request order: `#l[2], #l[0]` on `[l0, l1, l2]` now yields `[l0, l2]`. Confirmed against real DynamoDB in eu-west-2.
- A `ProjectionExpression` naming two or more sub-attributes of the same list index now returns them merged into a single list element, where dynoxide split each path into its own element: `l[0].a, l[0].b` on `{ l: [ { a, b } ] }` returned `[ { a }, { b } ]` and now returns `[ { a, b } ]`. The merge holds at depth (nested maps and nested lists under one index), distinct indices still stay separate and compact to ascending order, and the fix reaches every projecting read through the shared reconstruction path (`GetItem`, `Query`, `Scan`, `BatchGetItem`, `TransactGetItems`). Confirmed against real DynamoDB in eu-west-2 ([#126](https://github.com/nubo-db/dynoxide/issues/126)).
- `Query` now accepts a `KeyConditionExpression` sort-key comparison with the value on the left (`:lo <= #sk`), treating it as the attribute-on-left form (`#sk >= :lo`) for each of `<`, `<=`, `>`, `>=`, where dynoxide rejected it. A nested or indexed path on a key attribute is now rejected with DynamoDB's message (`Invalid KeyConditionExpression: KeyConditionExpressions cannot have conditions on nested attributes`), replacing dynoxide's own wording. Confirmed against real DynamoDB in eu-west-2.
- `BatchGetItem` now rejects a request that uses an expression `ProjectionExpression` on one table's block and a non-expression `AttributesToGet` on another, where dynoxide accepted it. Real DynamoDB rejects the whole request even when each block is internally consistent. Confirmed against real DynamoDB in eu-west-2.
- `PutItem` and `UpdateItem` validation ordering now matches real DynamoDB: an empty or invalid `TableName` is reported on its own, before the `Return*` enum checks, where dynoxide aggregated them into one envelope. `UpdateItem` additionally stops at the first invalid enum (reporting `ReturnValues`), where `PutItem` continues to aggregate every invalid enum, matching each operation's own behaviour. Confirmed against real DynamoDB in eu-west-2.
- `UpdateTable` now merges the request's `AttributeDefinitions` into the table's existing set, where each call replaced the stored list with only the attributes it carried. DynamoDB treats these as a delta: adding a global secondary index only requires the new index's key attributes, so the table keys and prior indexes' attributes need not be re-declared. Adding two GSIs with delta-only attributes therefore dropped the table keys and the first index's attributes from `DescribeTable`, and a later `PutItem` failed index-key validation with `Index key attribute GSI1PK missing from AttributeDefinitions`. The definitions are now unioned by attribute name, preserving those declared earlier; a redeclared attribute keeps its existing type, matching real DynamoDB, which ignores a conflicting type in the delta rather than overwriting or rejecting it. `UpdateTable` now also keeps `AttributeDefinitions` equal to exactly the attributes used by the table key schema and the current index key schemas: deleting a GSI prunes its now-orphaned key attributes, and an entry supplied in the delta that is used by no key schema is dropped rather than stored (neither is an error). All verified against AWS in eu-west-2 ([#129](https://github.com/nubo-db/dynoxide/issues/129)).

## [0.11.1] - 2026-06-26

### Fixed

- A `ConditionExpression` comparing a Map (`M`) or List (`L`) attribute for equality now works, where `=` always reported not-equal and `<>` always equal regardless of the values. `compare_values` had no arm for document types, so every map or list comparison fell through to the not-equal default; it now compares them deeply - maps order-independently, lists element-wise in order - with nested numbers normalised as elsewhere. The same path backs `IN`, `BETWEEN`, and `contains` over document operands, so those are fixed too ([#103](https://github.com/nubo-db/dynoxide/issues/103)).
- `ExpressionAttributeValues` nested beyond DynamoDB's 32-level document limit are now rejected up front with the same `ValidationException` AWS returns, where before they were accepted and evaluated. The check runs on every path that takes expression values - PutItem, UpdateItem, DeleteItem, Query, Scan, and TransactWriteItems. The stored-item nesting check was also one level too lenient (it accepted a value AWS rejects) and carried a non-AWS message; both now match DynamoDB's limit and wording, confirmed against real AWS in eu-west-2 ([#110](https://github.com/nubo-db/dynoxide/issues/110)).
- Number-set equality in a condition or filter expression now compares at full precision, where it parsed each member to `f64` and so reported two sets differing only beyond ~15 significant digits as equal. It now uses the canonical numeric form, matching DynamoDB and the way number-set duplicates are already detected on write; the fix also covers number sets nested inside a map or list ([#111](https://github.com/nubo-db/dynoxide/issues/111)).
- A `Number` with a leading `+` on the mantissa (`+5`, `+1.5`, `+1e2`) is now accepted and stored normalised (`+5` reads back as `5`), matching real DynamoDB, where dynoxide rejected it with a `ValidationException`. The validator was reworked to accept exactly DynamoDB's numeric grammar, which also closes two pre-existing gaps in the same direction: malformed forms such as `1+2`, `1.2.3`, `+e2`, and a digitless exponent are now rejected, as is any surrounding or internal whitespace (`" 5"` was previously trimmed and accepted). The accept and reject boundary was verified against real DynamoDB ([#109](https://github.com/nubo-db/dynoxide/issues/109)).

## [0.11.0] - 2026-06-24

### Added

- `UpdateTable` on the wasm preview engine: add or delete a global secondary index, with existing rows backfilled into a newly added index, and change the simple table settings (provisioned throughput, billing mode, table class, on-demand throughput, deletion protection). A stream-specification change through `UpdateTable` stays unsupported, since streams remain a preview gap, and a newly added GSI is reported immediately `ACTIVE` rather than transitioning through `CREATING`.
- The wasm engine gained an operation-level `execute` API, and a new npm package, `@dynoxide/wasm-engine`, that ships it. The Worker answers a small versioned RPC - `open`, `execute`, `capabilities`, `contractVersion` - with `{id, op, payload}` in and `{id, ok, result|error}` out, and a bundled `EngineClient` owns the round trip so you deal in objects instead of hand-building postMessage envelopes. `npm run build:wasm` assembles the package: the Worker, the two `.wasm`, the `EngineClient`, and a `manifest.json` stamped with the engine and contract versions. Depend on that built package, not this repo's source. The client checks its `CONTRACT_VERSION` against the engine on boot and fails loudly if they differ, so a stale embed can't quietly mis-read a newer one. The package ships TypeScript types for the client. Still a preview: the wasm path isn't run against the conformance suite.

### Changed

- On the wasm backend, the per-write and per-delete secondary-index fan-out now crosses the JS bridge once per index type rather than once per index operation. Keeping a table's GSIs and LSIs in step with a write is a delete and a re-insert per index, each previously its own bridge crossing; a new `exec_script` primitive carries the whole ordered batch over in a single crossing, so an indexed `PutItem` or `DeleteItem` on a table with K GSIs and L LSIs drops from order K+L crossings to a constant two. Index contents and native behaviour are unchanged ([#85](https://github.com/nubo-db/dynoxide/issues/85)).
- The browser backend moved from `wa-sqlite` to the official [`@sqlite.org/sqlite-wasm`](https://github.com/sqlite/sqlite-wasm) engine, maintained by the SQLite team and versioned to track SQLite releases. The bridge now runs through the `sqlite3.oo1` API over the OPFS SAHPool VFS, which keeps the no-COOP/COEP guarantee that motivated the original VFS choice (it needs no `SharedArrayBuffer`). The `open`/`exec`/`query`/`close` contract is unchanged, so consumers of `@dynoxide/wasm-engine` need no code change. A busy database now recovers once the holder releases it rather than staying busy until reload, and the full 64-bit integer round-trip and the `fnv1a_hash` scalar are re-proven on the new engine ([#61](https://github.com/nubo-db/dynoxide/issues/61)). The shipped SQLite `.wasm` is larger than before (~845 KB against wa-sqlite's ~545 KB).

### Fixed

- An empty-binary key value now surfaces as a top-level `ValidationException` on every path, matching DynamoDB. Previously the lookup path (`GetItem`/`DeleteItem`/`UpdateItem`, batch, and a transact `Update`/`Delete`/`ConditionCheck` `Key`) returned the older `...were invalid:...` wording and, inside a transaction, a `ValidationError` cancellation reason rather than hoisting; the same cancellation-instead-of-hoist gap also affected an empty-binary table item key and an empty-binary secondary-index key in a transaction. This is the binary counterpart to the empty-string key fix [#98](https://github.com/nubo-db/dynoxide/issues/98); real DynamoDB returns the same top-level `...are not valid. ... empty binary value...` messages (table keys, and the put and update forms for secondary-index keys), confirmed identical across four regions.
- Inside a `TransactWriteItems`, an empty-string value in the lookup `Key` of an `Update`, `Delete`, or `ConditionCheck` was wrapped in a `TransactionCanceledException`; it now surfaces as a top-level `ValidationException`, matching DynamoDB and completing the empty-string key fix [#95](https://github.com/nubo-db/dynoxide/issues/95) made for the `Put` item key. Wrong-type and non-scalar lookup keys still cancel with a `ValidationError` reason, and the corrected empty-string message now also matches DynamoDB on the single-action `GetItem`/`DeleteItem`/`UpdateItem` and batch lookup paths ([#98](https://github.com/nubo-db/dynoxide/issues/98)).
- `BatchWriteItem` now reports a wrong-type or non-scalar table key in a put request with DynamoDB's generic `The provided key element does not match the schema`, rather than borrowing `PutItem`'s `Type mismatch for key ...` wording. Real DynamoDB collapses both cases to the schema error inside a batch. The empty-string table-key message and the secondary-index key messages already matched and are unchanged, and `PutItem` and the other put-shaped paths keep the specific type-mismatch message ([#97](https://github.com/nubo-db/dynoxide/issues/97)).
- `Query` and `Scan` now reject two `Select`/`ProjectionExpression` combinations that real DynamoDB rejects before reading any item, where dynoxide previously returned results: a `ProjectionExpression` with any `Select` other than `SPECIFIC_ATTRIBUTES` (such as `ALL_ATTRIBUTES`), and `Select: ALL_PROJECTED_ATTRIBUTES` without an `IndexName`. Both now return a `ValidationException` with DynamoDB's message ([#96](https://github.com/nubo-db/dynoxide/issues/96)).
- Inside a `TransactWriteItems`, a key (table or secondary index) carrying an empty string was wrapped in a `TransactionCanceledException`; it now surfaces as a top-level `ValidationException`, matching DynamoDB. Wrong-type and non-scalar key values still cancel with a `ValidationError` reason, so only the empty-string case changes. A non-scalar table key no longer fails as an internal error before the transaction runs, and an update that sets a secondary-index key to an empty string now returns DynamoDB's distinct update-path message rather than the put-shaped one ([#95](https://github.com/nubo-db/dynoxide/issues/95)).
- A write whose secondary-index (GSI or LSI) key attribute is the wrong type, a non-scalar, or an empty string is now rejected with a `ValidationException` matching DynamoDB's exact message, where before it was silently accepted (kept out of the index but still written to the base table). Validation runs on every write path - put, update, batch, transactional, PartiQL, and import. An update only re-checks an index key it actually changes, so an unrelated update to a row holding a pre-existing bad value still succeeds ([#92](https://github.com/nubo-db/dynoxide/issues/92)).
- A `Scan` or `Query` on a composite global secondary index no longer returns items that are missing the index sort key; they are now excluded from the index (sparse-index behaviour), matching DynamoDB. Index membership was gated on the partition key alone, so an item carrying the partition key but no sort key was written into the index at an empty sort-key position. Membership is now a single shared rule across both global and local secondary indexes, applied on every write path - put, update, batch, transactional, PartiQL, import, and GSI backfill - and it also excludes an item whose index key attribute is present but not a scalar. In-memory databases start fresh each run and are unaffected; only a file-backed database, or a snapshot taken from one, written by an older build carries stray index rows. They clear as each affected item is next written, and a persisted store can rebuild an index by dropping and re-adding it ([#91](https://github.com/nubo-db/dynoxide/issues/91)).
- `PutItem` and the other write paths now accept a `{"NULL": false}` attribute value and read it back as `{"NULL": true}`, where before they rejected it with `One or more parameter values were invalid: Null attribute value types must have the value of true`. The NULL member is typed as a plain boolean in the model, so `false` was valid input all along; AWS has dropped the server-side true-only rule and normalises `false` to `true` on read, and dynoxide now matches. A non-boolean NULL such as `{"NULL": "no"}` is still rejected as a type error ([#62](https://github.com/nubo-db/dynoxide/issues/62)).
- Hardened the wasm engine preview ahead of a stable `@dynoxide/wasm-engine` publish. A body-less operation such as `ListTables` now round-trips instead of failing as a `SerializationException` ([#65](https://github.com/nubo-db/dynoxide/issues/65)). OPFS open tells a busy database (another tab holding its lock) apart from one that is genuinely unavailable: the busy case surfaces a stable `com.dynoxide.wasm#OpfsUnavailable` error rather than silently forking to a separate in-memory store, while a private window or quota error still degrades to an ephemeral session. Re-opening opens the new database before closing the old, so a failed re-open leaves the working session intact, and closing a database releases its OPFS handles so the name is free for another tab ([#64](https://github.com/nubo-db/dynoxide/issues/64)). The bridge round-trips full 64-bit integers, and a cross-backend test pins the `fnv1a_hash` scalar the wasm and native backends share ([#61](https://github.com/nubo-db/dynoxide/issues/61)). A headless-browser CI job exercises the shipped bundle against the real wasm engine and OPFS on every PR ([#68](https://github.com/nubo-db/dynoxide/issues/68)).

## [0.10.0] - 2026-05-29

### Added

- A `StorageBackend` trait in the new `dynoxide::storage_backend` module, decoupling the data layer from a specific SQLite binding. The native rusqlite-backed `Storage` implements the trait, and the action handlers and `Database` now consume it (see Changed). The trait surface also carries a `clock()` accessor for the stream and TTL paths and batch-shaped `put_base_items` / `insert_gsi_items` methods that replaced the last two raw `Storage::conn()` escape hatches in the handlers.
- A `BackendError` enum returned by the trait surface, with an explicit `rusqlite::Error -> BackendError` mapping for the common failure modes (`NotADatabase`, locked / busy, constraint violations, I/O failures), plus an `Unsupported { capability }` variant for a capability a backend cannot serve (the wasm preview uses it for TTL). It is `#[non_exhaustive]` so future backends can add failure modes without a breaking change.
- A `Clock` capability on `Storage` so the trait surface does not assume `std::time`. Stream and TTL paths route their `created_at` and sweep timestamps through the clock; `SystemClock` is the default and `ManualClock` ships as a deterministic test helper. Other `std::time` call sites (idempotency cache, action-handler timestamps, snapshots) remain native-only and are unchanged.
- A `wasm-sqlite` cargo feature and a working WebAssembly backend. dynoxide compiles to `wasm32-unknown-unknown` and runs in the browser against [wa-sqlite](https://github.com/rhashimoto/wa-sqlite) (a WASM build of SQLite) over a wasm-bindgen bridge, persisting to OPFS. `WasmBridgeBackend` implements `StorageBackend`, and `WasmDatabase` (`Database<WasmBridgeBackend>`) exposes the handlers as `async fn` with no `block_on`. It covers create-table, put, get, delete, query, and scan over base tables and both index types (GSI and LSI), with index fan-out atomic with the base write. TTL returns `BackendError::Unsupported`; streams return a preview "not yet implemented" error pending a delivery design; `TransactWriteItems`, tags, table-setting updates, stats, and bulk import are preview placeholders. The native and wasm backends share one set of SQL builders (`storage_backend::sql_builders`), so both issue identical SQL.
- A self-contained browser build: `npm run build:wasm` (wasm-pack + esbuild) emits a `dist/` of three files - a bundled Web Worker plus the two `.wasm` assets (dynoxide ~550 KB, wa-sqlite ~545 KB; ~1.2 MB total). The engine runs in a Web Worker because wa-sqlite's OPFS persistence uses synchronous access handles, which browsers expose only in a Worker; pairing wa-sqlite's synchronous VFS (`AccessHandlePoolVFS`) with its non-async build needs no `SharedArrayBuffer`, and so **no cross-origin isolation (COOP/COEP)** - it drops onto ordinary static hosting. A build-visible `WASM_PREVIEW` constant (`true` under `wasm-sqlite`) marks the preview. The harness under `harness/` loads the same bundled Worker that ships, so a green harness means the shipping artefact works; it exercises CRUD, GSI query/scan, and error-envelope fidelity on OPFS. CI builds the `wasm32-unknown-unknown` target for both the `wasm-sqlite` and `wasm-harness` features on every PR, so the harness's use of `WasmDatabase` and the action types is type-checked too.
- Official Docker image. `docker run -p 8000:8000 ghcr.io/nubo-db/dynoxide` is a ~5 MB drop-in for `amazon/dynamodb-local` in containerised test suites: multi-arch (`linux/amd64` and `linux/arm64`), `FROM scratch`, published to GHCR on each release with Docker Hub and ECR Public mirrors pushed best-effort. The image ships a `HEALTHCHECK` backed by a new `dynoxide healthcheck` subcommand, so `docker ps` and Compose health gates report status without extra tooling ([#3](https://github.com/nubo-db/dynoxide/issues/3)).
- `SECURITY.md`, documenting the MCP HTTP transport's threat model: the bearer-token authentication it now requires, plus the Host and Origin allowlists that back it ([#27](https://github.com/nubo-db/dynoxide/issues/27)).
- MCP HTTP transport options: `--mcp-host`/`--host` to bind beyond loopback, `--mcp-allowed-host`/`--allowed-host` to accept additional `Host` headers by name, and `--mcp-no-auth`/`--no-auth` to disable authentication on loopback binds only. With a token set, these make the transport reachable from outside a container, unblocking the Docker MCP path ([#24](https://github.com/nubo-db/dynoxide/issues/24)).

### Changed

- `Database` is now generic over its storage backend: `Database<S>`, monomorphised, no `dyn`. The parameter defaults to the native rusqlite backend, so existing code that names `Database` is unaffected, and a new `NativeDatabase` alias names that default explicitly. The action handlers are now `async` and route through the `StorageBackend` trait. `NativeDatabase` keeps the historical synchronous public API: each method drives the handler future to completion with `block_on` (via `pollster`), and because the native backend's futures never suspend, that `block_on` never parks the thread, so it stays safe inside the tokio-based HTTP and MCP servers.
- `DynoxideError` is now `#[non_exhaustive]`. Match arms in downstream code must include a wildcard. Done now, while 0.10.0 is already a breaking release, so later variant additions stay non-breaking.
- **Breaking:** the MCP HTTP transport (`dynoxide mcp --http`, `dynoxide serve --mcp`) now requires bearer-token authentication on every request. On a loopback bind, dynoxide generates a token on first run, persists it to a per-user config file, and prints a client-config snippet; later runs reuse it silently. **Existing clients break until updated**: add `"headers": { "Authorization": "Bearer <token>" }` to your MCP client config. A non-loopback bind requires an explicit token via `--mcp-token`/`--token` or `DYNOXIDE_MCP_AUTH_TOKEN` and will not start without one. The stdio transport is unaffected ([#27](https://github.com/nubo-db/dynoxide/issues/27)).
- **Breaking (library API):** `dynoxide::mcp::serve_http` and `serve_http_with_shutdown` now take an `HttpOptions` struct (bind host, `AuthMode`, extra allowed hosts) in place of a bare `port: u16`. Embedders constructing the MCP HTTP server must build `HttpOptions` and choose an `AuthMode`.
- `rusqlite` is now an optional dependency behind the `native-sqlite` feature (on by default, so native builds are unchanged). The crate type-checks with rusqlite absent, which is the precondition for the wasm build. Cross-platform wall-clock paths (the idempotency cache, `created_at` stamps, and `SystemClock`) moved to `web-time` - `std::time` on native, the browser clock on wasm. The native binary now builds behind a `cli` marker feature (pulled in by `http-server`, `mcp-server`, and `import`), so it is skipped in backend-neutral builds such as `--features wasm-sqlite`. The `DynoxideError::SqliteError` variant is consequently `native-sqlite`-gated and absent on backend-neutral builds, which matters only for code that matches it by name on a wasm target.

### Fixed

- PartiQL `DELETE` and `UPDATE` now evaluate the non-key predicates in a `WHERE` clause instead of acting on the key alone. Before, the executor pulled the primary key out of the `WHERE` and ignored the rest, so `DELETE FROM "t" WHERE pk = 'a' AND NOT begins_with(name, 'x')` deleted the row even when `name` began with `x`, mutating a row the filter should have excluded (a data-correctness bug predating v0.9.5). The write paths now run the full condition against the fetched item, the same `matches_where` pass `SELECT` already uses: a present item whose non-key predicate is false raises `ConditionalCheckFailedException`, matching how AWS treats a PartiQL write whose condition fails, and a missing item stays a silent no-op ([#54](https://github.com/nubo-db/dynoxide/issues/54)).
- `DescribeTable` now returns a stable `TableId` instead of a freshly generated UUID on every call. The id is a random UUID assigned once at create time and persisted (a new `table_id` column, added to existing databases through the versioned schema migration and backfilled), so it stays the same across calls, `CreateTable` returns the same value, and a dropped-and-recreated table gets a new one, matching AWS ([#55](https://github.com/nubo-db/dynoxide/issues/55)).
- `UpdateItem` evaluates an `UpdateExpression` against the pre-update item image and accepts parenthesised arithmetic. `SET a = :v, b = a` now gives `b` the old value of `a` rather than the value assigned earlier in the same call, and `SET c = (c - :v)` parses and applies on the `BigDecimal` path instead of being rejected with `Expected operand in SET, got (` ([#35](https://github.com/nubo-db/dynoxide/issues/35)).
- `UpdateItem` `ReturnValues: UPDATED_NEW` matches AWS granularity. A nested `SET parent.child = :v` returns only the changed fragment `{parent: {M: {child}}}` instead of the whole `parent` map, and a REMOVE-only update omits `Attributes` entirely rather than returning an empty map ([#36](https://github.com/nubo-db/dynoxide/issues/36)).
- Paginating a `Query` over a GSI no longer drops items when several entries share the same index key and the base table has only a partition key. On a hash-only base table the continuation cursor lost its base-key component and stalled after the first page, the same defect [#38](https://github.com/nubo-db/dynoxide/issues/38) fixed for `Scan`; the `Query` path now carries the base partition key, so every tied item is returned across the paged walk ([#52](https://github.com/nubo-db/dynoxide/issues/52)).
- `TransactWriteItems`, `TransactGetItems` and PartiQL `ExecuteStatement` now report `ConsumedCapacity` the way AWS does. A transactional write charges 2 WCU per item and a transactional read 2 RCU per item including a missing one (each item rounded up before the 2x factor); the `TransactGetItems` `INDEXES` breakdown carries `Table.ReadCapacityUnits`; and `ExecuteStatement` returns the `ConsumedCapacity` block whenever `ReturnConsumedCapacity` is requested instead of omitting it ([#37](https://github.com/nubo-db/dynoxide/issues/37)).
- PartiQL `ExecuteStatement` accepts the bracket `IN [...]` list form, not just `IN (...)`, and evaluates `NOT begins_with(...)` as a negated predicate. `IS NOT MISSING` already evaluated; the gaps were the bracket list and the `NOT` function arm, which the same statement bundles together ([#40](https://github.com/nubo-db/dynoxide/issues/40)).
- `DescribeTable` now round-trips `OnDemandThroughput` and reports the full `SSEDescription` shape. A table created with `OnDemandThroughput` reports its `MaxReadRequestUnits` and `MaxWriteRequestUnits` back; the value lives in a new `on_demand_throughput` column added through the versioned schema migration, so existing on-disk databases pick it up on open. Server-side encryption enabled with the AWS-managed key now reports `SSEType: KMS` and a `KMSMasterKeyArn` alongside `Status: ENABLED`, where before it returned the status alone ([#44](https://github.com/nubo-db/dynoxide/issues/44)).
- `UpdateTable` now accepts a lone `TableClass` or `OnDemandThroughput` change instead of rejecting it with `At least one of ProvisionedThroughput, BillingMode, ... is required`. Both fields are validated (an unknown `TableClass` is a `ValidationException`) and persisted, so the change shows up on the next `DescribeTable` ([#45](https://github.com/nubo-db/dynoxide/issues/45)).
- `DeleteTable` on a table with deletion protection enabled now returns the exact AWS message, `Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection first.`, in place of the ARN-prefixed wording dynoxide used before ([#46](https://github.com/nubo-db/dynoxide/issues/46)).
- `TransactGetItems` now omits `Item` from a response entry when a `ProjectionExpression` matches no attribute on an otherwise-present item, matching AWS. The projection always re-injects the table key, so the entry previously came back as a key-only object instead of being omitted ([#39](https://github.com/nubo-db/dynoxide/issues/39)).
- `BatchWriteItem` now rejects a `PutRequest` whose item is missing the table key with a 400 `ValidationException` rather than a 500 `InternalServerError`. The duplicate-key detection pass extracted keys before validating them; it now validates first, the same ordering the single-item write paths already use ([#39](https://github.com/nubo-db/dynoxide/issues/39)).
- Paginating a `Scan` over a GSI no longer drops items when several entries share the same index key and the base table has only a partition key. On a hash-only base table the continuation cursor lost its base-key component and stalled after the first page; it now carries the base partition key, so every tied item is returned across the paged walk ([#38](https://github.com/nubo-db/dynoxide/issues/38)).
- A single-item write (`PutItem`, `DeleteItem`, `UpdateItem`) and its GSI/LSI index fan-out now run in a single transaction. A failure partway through the fan-out rolls the whole write back rather than leaving a base row with a half-applied (torn) index. The same per-item atomicity now also covers `BatchWriteItem` (each write request) and the TTL sweep (each expired-item delete). This matches DynamoDB, where a single-item write does not half-apply to its indexes.
- Write paths now roll back on a failed `COMMIT` and surface a failed `ROLLBACK` rather than leaving the connection stuck mid-transaction, which would make the next write fail. Every write path shares one transaction helper for this.
- A client-facing `ValidationException` raised inside a backend method (the 50-tag limit in `set_tags`) keeps its 400 status across the `StorageBackend` boundary instead of collapsing to a 500.
- Tighter expression and scan validation, to match what real DynamoDB rejects
  (surfaced by the conformance suite). Dynoxide now turns away redundant
  parentheses like `((a = :b))` in condition, filter, and key-condition
  expressions; `contains(x, x)` with the same operand on both sides; and
  `begins_with` handed a number instead of a string or binary. These are
  rejected up front, before any items are scanned
  ([#31](https://github.com/nubo-db/dynoxide/issues/31)).
- `size()` now measures strings in UTF-16 code units rather than bytes, so
  values with emoji or accented characters report the length DynamoDB returns.
- A negative `Segment` on a parallel scan is now rejected rather than accepted.

### Notes

- Existing native code that names `Database` keeps working unchanged: the new generic parameter defaults to the rusqlite backend and the synchronous method signatures are identical. The one deliberate behaviour change is index fan-out atomicity (see Fixed); it is more DynamoDB-correct, and the conformance suite still passes. Tests, conformance, and benchmarks pass against the same observable surface as before.
- Building dynoxide for `wasm32-unknown-unknown` is now supported via the `wasm-sqlite` feature (see Added). The wasm backend is a preview: it is not run against the conformance suite that covers the native build, so its correctness rests on its own CRUD/query/scan/GSI/LSI tests for now. The engine runs in a Web Worker (OPFS's synchronous file handles are Worker-only) and needs no cross-origin isolation, so it works on ordinary static hosting.

## [0.9.13] - 2026-05-11

### Security

- Close a DNS rebinding vulnerability in the MCP HTTP transport
  ([GHSA-89vp-x53w-74fx](https://github.com/modelcontextprotocol/rust-sdk/security/advisories/GHSA-89vp-x53w-74fx) /
  [CVE-2026-42559](https://www.cve.org/CVERecord?id=CVE-2026-42559)) by upgrading `rmcp`
  from 1.1.1 to 1.6.0 in both lockfiles. A malicious page could make the
  user's browser send requests to a loopback MCP server with a non-loopback
  `Host` header, which the server would then process. Affects 0.9.3 to 0.9.12.
  Users running `dynoxide mcp --http` or `dynoxide serve --mcp` should
  upgrade; stdio transport is unaffected.

- Close a related cross-origin CSRF gap: a page could `fetch` the loopback
  endpoint with `mode: 'no-cors'`, and the Host check would pass while the
  Origin header went unchecked. Affected write tools: `put_item`,
  `update_item`, `delete_item`, `create_table`, and `batch_write_item`.
  Fixed by setting an explicit Host and Origin allowlist on
  `StreamableHttpServerConfig`. Native MCP clients (Claude Code, Cursor,
  the dynoxide CLI) don't send an Origin header and are unaffected.

## [0.9.12] - 2026-05-04

### Fixed

- Unix: port releases immediately after `dynoxide serve` shuts down. The listener used to skip `SO_REUSEADDR`, leaving leftover `TIME_WAIT` sockets from connected clients to block restart for ~60s. Live-listener conflict detection is unaffected: `SO_REUSEADDR` only bypasses `TIME_WAIT`, not active sockets.

  Windows: unchanged. `SO_REUSEADDR` lets another process hijack an active bind there, so we leave it off.

## [0.9.11] - 2026-05-04

### Fixed

- `dynoxide serve --mcp` now exits cleanly on Ctrl+C when an MCP client (Claude Code, Cursor) is holding a connection open. The MCP server's graceful-shutdown drain used to wait for those connections forever, hanging the process until something SIGKILLed it ([#22](https://github.com/nubo-db/dynoxide/issues/22))

### Security

- Refresh `Cargo.lock` for the dependabot patches reachable within MSRV: `aws-lc-sys` 0.37.1 to 0.40.0 (5 high-severity AWS-LC issues), `openssl` 0.10.75 to 0.10.79 (5 buffer-overflow advisories), `rand` 0.8.5 to 0.8.6. Remaining `rustls-webpki` / `time` / `aws-sdk-dynamodb` alerts are dev-dependency only (test-suite AWS SDK chain, not the production binary) and stay pinned by MSRV 1.85 until v0.10.0

## [0.9.10] - 2026-04-27

### Fixed

- 16 places where dynoxide's error strings drifted from real AWS DynamoDB. Mostly small things you only notice when you assert the message: `tableName` length validation is now per-operation (1 char on read/write, 3 stays on `CreateTable`), `Select` enum order matches AWS rather than alphabetical, `Query` vs `Scan` `Limit=0` messages are different on purpose now, batch/transact empty and oversize requests use the standard validation envelope, and `UpdateExpression`/`ProjectionExpression` syntax errors include the AWS `near: "..."` window ([#11](https://github.com/nubo-db/dynoxide/issues/11), [#12](https://github.com/nubo-db/dynoxide/issues/12), [#13](https://github.com/nubo-db/dynoxide/issues/13), [#15](https://github.com/nubo-db/dynoxide/issues/15), [#16](https://github.com/nubo-db/dynoxide/issues/16), [#17](https://github.com/nubo-db/dynoxide/issues/17), [#18](https://github.com/nubo-db/dynoxide/issues/18))
- `TransactGetItems` with a bad action key now comes back as a `TransactionCanceledException` with `ValidationError` rather than HTTP 500. The 500 was a real leak: the dedup loop called the server-fault helper before key validation ([#19](https://github.com/nubo-db/dynoxide/issues/19))

## [0.9.9] - 2026-04-24

### Fixed

- `KeyConditionExpression` now accepts parenthesised sub-expressions, matching DynamoDB. Forms like `(#pk = :pk) AND (#sk = :sk)` previously returned `ValidationException: Expected attribute name, got (`. Both outer-wrap and per-condition parens are now handled ([#4](https://github.com/nubo-db/dynoxide/issues/4), [#7](https://github.com/nubo-db/dynoxide/pull/7))
- `UpdateItem` and `TransactWriteItems.Update` now evaluate `ConditionExpression` against the existing item before populating key attributes for upsert. Previously `attribute_exists(pk)` on a non-existent key succeeded and created a ghost item ([#5](https://github.com/nubo-db/dynoxide/pull/5))
- Paginated `Scan` on a GSI now returns all items when multiple items share the same GSI partition key. Previously the second page returned 0 items because the pagination cursor used only `(gsi_pk, gsi_sk)` instead of the full 4-tuple primary key ([#6](https://github.com/nubo-db/dynoxide/pull/6))
- `<>` on missing attributes now returns true, matching DynamoDB. All other comparison operators continue to return false on missing operands. Previously `<>` also returned false, breaking `PutItem` conditional idioms like `status <> "working"` against fresh keys ([#8](https://github.com/nubo-db/dynoxide/pull/8))

## [0.9.8] - 2026-04-06

### Fixed

- Dynoxide no longer orphans when backgrounded in npm scripts (`dynoxide & sleep 1 && npm run seed && react-router dev`) -- the port is released when the parent process exits ([nubo-db/dynoxide#2](https://github.com/nubo-db/dynoxide/issues/2))
- The Rust server now handles SIGTERM for graceful shutdown, not just SIGINT (Ctrl+C) -- `kill <pid>` now works as expected
- The npm wrapper switches from `spawnSync` to async `spawn` with explicit signal forwarding (SIGINT, SIGTERM, SIGHUP) and double-signal SIGKILL escalation
- Parent-death detection via PPID polling catches the backgrounded case where no signal is delivered to the wrapper

## [0.9.7] - 2026-04-02

### Fixed

- Benchmark sanity checks were blocking README updates during release - 10 stale values from the v0.9.6 pipeline now corrected
- Binary download size in README was wrong (~5 MB, actually ~3 MB compressed / ~6 MB on disk)
- Docker image sizes now show both download and on-disk measurements - the old "225 MB" was the compressed download, the actual on-disk size is 471 MB
- MCP tool count in README was 33, should be 34 - `execute_transaction_partiql` was missing from the list
- npm README had incorrect `--input` and `--db-path` flags for the import command (should be `--source`, `--schema`, `--output`)
- Dropped the `serve` subcommand from npm examples (bare `dynoxide --port 8000` is the preferred form)

### Changed

- Restructured release pipeline for token efficiency and reliability - dispatch verification, idempotent crate/npm publishing, template-based Homebrew formula updates
- npm publishing uses OIDC provenance via a dedicated `npm.yml` workflow
- Cross-compilation switched to cargo-zigbuild for aarch64-musl targets
- Commit Cargo.lock for reproducible CI builds (was previously gitignored)
- Updated npm package README to reflect current CLI usage and features

### Security

- Updated `aws-lc-sys` 0.37.1 to 0.39.1 (10 high-severity advisories - PKCS7 verification bypass, timing side-channel in AES-CCM, CRL/name constraint issues)
- Updated `rustls-webpki` 0.103.9 to 0.103.10 (2 medium-severity CRL Distribution Point matching issues)

## [0.9.6] - 2026-03-27

### Fixed

- Statically link the MSVC C runtime on Windows so the release binary no longer requires VCRUNTIME140.dll
- Switch Linux aarch64 target to musl for fully static binaries (matching x86_64)

### Changed

- Drop the separate x86_64-unknown-linux-gnu release target (the musl build is already fully portable)

## [0.9.5] - 2026-03-24

### Added

- **DynamoDB conformance suite** - 526 independently written tests across 3 tiers, validated against real DynamoDB ground truth. Dynoxide: 100%. DynamoDB Local: 92%. See [dynamodb-conformance](https://github.com/paritysuite/dynamodb-conformance).
- **Dynalite external conformance** - 817/1039 passing (87.1% DynamoDB parity) against Dynalite's test suite, where real DynamoDB itself only passes 51%
- **DynamoDB compatibility documentation** - a public compatibility summary covering operation, expression, index, and PartiQL support, with a DynamoDB Local comparison column
- **Correctness fixes** - 41 issues resolved across core operations and PartiQL
- **Reserved word validation** - 573 DynamoDB reserved keywords rejected in ConditionExpression, UpdateExpression, FilterExpression, and ProjectionExpression with correct error messages
- **README benchmark automation** - CI benchmark numbers auto-updated via template markers and Python script; PR-based review with sanity checking
- **IdempotentParameterMismatchException** - TransactWriteItems detects same token with different payload
- **AccessDeniedException** - returned for tag operations on non-existent ARNs (matches DynamoDB behaviour)

### Changed

- **BigDecimal replaces f64** for all number comparisons and arithmetic - eliminates silent precision loss beyond 15 significant digits; f64 fast-path for ≤15 significant digits preserves performance
- **PartiQL INSERT** now fails with `DuplicateItemException` if item already exists (previously silently overwrote)
- **PartiQL tokeniser** - correct handling of negative numbers, escaped single quotes, unknown characters (error instead of silent skip)
- **Query/Scan COUNT** now returns filtered count, not scanned count, when `FilterExpression` is present
- **begins_with sort key** - SQL LIKE wildcards (`%`, `_`) properly escaped
- **Condition + write operations** wrapped in SQLite transactions to prevent TOCTOU races
- **1MB response limit** now counts all scanned items, not just filtered results
- **GSI query/scan LastEvaluatedKey** now includes base table key attributes
- **BatchWriteItem** rejects duplicate keys within the same request
- **TransactWriteItems** - 4MB size check uses accurate item size calculation; CancellationReasons returned as structured top-level JSON field; `ReturnValuesOnConditionCheckFailure` returns ALL_OLD item on condition failure
- **UpdateItem** rejects empty update expressions; protects key attributes from REMOVE/ADD/DELETE
- **ReturnValues** validated against allowed values per operation
- **UnprocessedKeys** in BatchGetItem preserves per-table settings
- **SET on list index beyond bounds** extends the list with NULL padding (previously returned error)
- **SET on empty list** at index 0 now succeeds
- **Projection with list index** correctly reconstructs list structure (previously created Map where List was needed)
- **Select validation** - invalid Select values and SPECIFIC_ATTRIBUTES without ProjectionExpression rejected
- **ConsistentRead on GSI** rejected with correct error message
- **Limit of 0** rejected with constraint error
- **Query/Scan validation ordering** matches DynamoDB (input validation before table existence check)
- **Expression attribute usage** validated syntactically (at parse time) not semantically (at runtime) - fixes false positives with `if_not_exists` short-circuiting
- **SerializationException** pre-checks for non-list field types with DynamoDB-compatible error format
- **Error type prefix** - `ValidationException` uses `com.amazon.coral.validate#` prefix matching real DynamoDB
- **BatchExecuteStatement** uses short error codes (`ResourceNotFound` not fully qualified type) and rejects empty Statements array
- **UpdateTable GSI delete** returns `ResourceNotFoundException` for non-existent GSI (previously `ValidationException`)
- **StreamSpecification** included in DescribeTable response
- Stack overflow protection: 32-level nesting depth limit on item validation (matches DynamoDB)
- AND/OR short-circuit evaluation in condition expressions

### Fixed

- `size()` function no longer evaluates on invalid attribute types
- Idempotency tokens correctly compared in TransactWriteItems
- PutItem no longer double-reads item for conditional checks
- GSI sort key replacement handles all edge cases
- Nested projection preserves document structure (no longer flattens)
- Double-quote identifier escaping in PartiQL
- PartiQL DELETE with missing sort key returns proper error
- PartiQL nested SET paths create correct nested structure (no longer creates literal dot-notation keys)
- PartiQL SELECT with nested map paths resolves correctly
- TTL expiry cleans up LSI entries (previously left orphans)
- GSI/LSI name collision detected and rejected at CreateTable time
- LSI pagination uses composite cursor to handle duplicate sort key values
- ExecuteTransaction breaks on first failure (previously continued executing then rolled back)
- Partition size calculation for ItemCollectionMetrics sums across base table and all LSI tables
- Error message fidelity improvements across empty string, deletion protection, scan segment, and query validation messages

## [0.9.4] - 2026-03-16

### Added

- **Local Secondary Indexes (LSI)** - full lifecycle: creation, query/scan routing, projection types (ALL, KEYS_ONLY, INCLUDE), sparse index behaviour, write path maintenance across all operations including TTL expiry
- **ExecuteTransaction** - PartiQL transactional execution with all-or-nothing semantics, condition checks, per-statement cancellation reasons, ConsumedCapacity support
- **Parallel Scan** - SQLite-level segment filtering via registered FNV-1a scalar function; validated segment/total parameters
- **CreateTable extensions** - `SSESpecification`, `TableClass` (validated), `Tags` (inline), `DeletionProtectionEnabled` with enforcement on DeleteTable and toggle via UpdateTable
- **PartiQL WHERE clause extensions** - `BETWEEN`, `IN`, `CONTAINS`, `IS MISSING`, `IS NOT MISSING`, `OR`, `NOT`, parenthesised grouping
- **PartiQL nested path projections** - `SELECT address.city, tags[0] FROM ...` with correct nested structure preservation
- **PartiQL REMOVE clause** - `UPDATE ... REMOVE attribute`
- **PartiQL SET expressions** - arithmetic (`count + 1`), `list_append`, `if_not_exists` in SET clauses
- **PartiQL IF NOT EXISTS** - `INSERT ... VALUE {...} IF NOT EXISTS`
- **PartiQL set literals** - `<< 'a', 'b', 'c' >>` syntax for SS/NS/BS
- **PartiQL COUNT(*)** and **LIMIT** support
- **Item validation** - empty string/set rejection, number precision validation (38 significant digits, ±9.99E+125 range), set deduplication (NS by numeric equivalence)
- **Unused expression attribute rejection** - unreferenced `ExpressionAttributeNames`/`ExpressionAttributeValues` entries return `ValidationException`
- **ReturnItemCollectionMetrics** - partition collection size across base table and all LSI tables
- **Per-GSI ConsumedCapacity** - `INDEXES` mode returns per-GSI breakdown in `GlobalSecondaryIndexes` map

### Changed

- **TrackedExpressionAttributes** - unified expression resolution with usage tracking; removed duplicate untracked code paths (~400 LOC reduction)
- **ScanParams / QueryParams structs** replace parameter sprawl in storage layer
- **CreateTableMetadata** consolidates previously triple-duplicated row mapping
- **GSI/LSI secondary indexes** on `(base_pk, base_sk)` / `(table_pk, table_sk)` columns - eliminates full table scans during index maintenance
- **Schema v5 migration** with automatic secondary index creation on existing tables

## [0.9.3] - 2026-03-12

### Added

- **MCP Server** - 33 tools exposing DynamoDB operations for coding agents (Claude Code, Cursor, etc.)
  - stdio and Streamable HTTP transports
  - `--read-only`, `--max-items`, `--max-size-bytes` safety flags
  - `bulk_put_items` tool for batch loading
  - OneTable `--data-model` integration with entity-aware agent context and `--data-model-summary-limit`
  - `--mcp` flag on `dynoxide serve` to run MCP alongside HTTP server
  - Snapshots: `create_snapshot`, `restore_snapshot`, `list_snapshots`, `delete_snapshot` with auto-snapshot before `delete_table`
  - `get_database_info` tool with data model context
- **Import CLI** - `dynoxide import` for DynamoDB Export data (JSON Lines format)
  - Anonymisation rules: fake, mask, hash, redact, null actions
  - Cross-table consistency for specified fields
  - zstd compression (`--compress`)
  - `--continue-on-error`, `--tables` filtering, atomic `--force` overwrite
  - Stream-aware import (reproduces source table's StreamSpecification)
- **CLI restructuring** - `dynoxide serve`, `dynoxide mcp`, `dynoxide import` subcommands
- Database introspection and port conflict detection on startup
- `RUST_LOG` debug tracing throughout HTTP and MCP servers

## [0.9.2] - 2026-03-04

### Added

- **SQLCipher encryption** - `encryption` feature (vendored OpenSSL via SQLCipher) and `encryption-cc` feature (Apple CommonCrypto backend) for encryption at rest
- Secure key handling via `--encryption-key-file` or `DYNOXIDE_ENCRYPTION_KEY` environment variable
- **UpdateTable** - `StreamSpecification` support, GSI create/delete with backfill
- **Tag operations** - `TagResource`, `UntagResource`, `ListTagsOfResource`
- **ReturnValuesOnConditionCheckFailure** for TransactWriteItems
- **GitHub Action** - `nubo-db/dynoxide@v1` with optional `snapshot-url` preloading
- **Homebrew formula** - `brew install nubo-db/tap/dynoxide`
- Release CI workflow with cross-platform binary builds (Linux x86_64/aarch64/musl, macOS Intel/Apple Silicon, Windows)
- Private-to-public repo publishing pipeline
- DynamoDBStreams target prefix - server accepts `DynamoDB_20120810.ListStreams` and Streams-prefixed actions
- `From`/`TryFrom` conversions for request/response types
- `item!` macro for ergonomic item construction in tests
- Table metadata cache for reduced SQLite round-trips
- Stripped release binaries

### Changed

- `nubo-app` → `nubo-db` GitHub organisation rename

## [0.9.1] - 2026-02-16

### Added

- `Server` and `X-Dynoxide-Version` headers on all HTTP responses
- `TableArn`, `LatestStreamArn`, and related ARN fields in API responses
- Comprehensive benchmarking suite comparing Dynoxide against DynamoDB Local and LocalStack
  - Criterion, iai-callgrind, and custom benchmark binaries
  - CI workflows for regression detection and historical tracking
  - Standard 13-step workload with JVM warmup protocol

### Changed

- `http-server` feature is now enabled by default
- Package renamed to `dynoxide-rs` for crates.io publishing

### Fixed

- Rustdoc warnings
- README version reference

## [0.9.0] - 2026-02-15

### Added

- Core DynamoDB emulator backed by SQLite via `rusqlite`
- In-memory and persistent database modes
- Table operations: CreateTable, DeleteTable, DescribeTable, ListTables
- Item operations: PutItem, GetItem, DeleteItem, UpdateItem
- Query and Scan with full expression support and pagination
- Batch operations: BatchGetItem, BatchWriteItem
- Transactions: TransactWriteItems, TransactGetItems
- Global Secondary Indexes (GSI)
- DynamoDB Streams (all four view types)
- TTL with background sweep
- Full expression language: KeyCondition, Filter, Condition, Projection, Update
- PartiQL: ExecuteStatement, BatchExecuteStatement
- ReturnConsumedCapacity (TOTAL and INDEXES modes)
- HTTP server (axum-based, DynamoDB JSON wire protocol)
- 300+ tests

[Unreleased]: https://github.com/nubo-db/dynoxide/compare/v1.5.0...HEAD
[1.5.0]: https://github.com/nubo-db/dynoxide/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/nubo-db/dynoxide/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/nubo-db/dynoxide/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/nubo-db/dynoxide/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/nubo-db/dynoxide/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/nubo-db/dynoxide/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/nubo-db/dynoxide/compare/v0.13.0...v1.0.0
[0.13.0]: https://github.com/nubo-db/dynoxide/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/nubo-db/dynoxide/compare/v0.11.4...v0.12.0
[0.11.4]: https://github.com/nubo-db/dynoxide/compare/v0.11.3...v0.11.4
[0.11.3]: https://github.com/nubo-db/dynoxide/compare/v0.11.2...v0.11.3
[0.11.2]: https://github.com/nubo-db/dynoxide/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/nubo-db/dynoxide/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/nubo-db/dynoxide/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/nubo-db/dynoxide/compare/v0.9.13...v0.10.0
[0.9.13]: https://github.com/nubo-db/dynoxide/compare/v0.9.12...v0.9.13
[0.9.12]: https://github.com/nubo-db/dynoxide/compare/v0.9.11...v0.9.12
[0.9.11]: https://github.com/nubo-db/dynoxide/compare/v0.9.10...v0.9.11
[0.9.10]: https://github.com/nubo-db/dynoxide/compare/v0.9.9...v0.9.10
[0.9.9]: https://github.com/nubo-db/dynoxide/compare/v0.9.8...v0.9.9
[0.9.8]: https://github.com/nubo-db/dynoxide/compare/v0.9.7...v0.9.8
[0.9.7]: https://github.com/nubo-db/dynoxide/compare/v0.9.6...v0.9.7
[0.9.6]: https://github.com/nubo-db/dynoxide/compare/v0.9.5...v0.9.6
[0.9.5]: https://github.com/nubo-db/dynoxide/releases/tag/v0.9.5
