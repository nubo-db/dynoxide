# MCP Server

Dynoxide includes an [MCP](https://modelcontextprotocol.io) server that exposes DynamoDB operations as tools for coding agents (Claude Code, Cursor, etc.).

## stdio transport (default)

```sh
dynoxide mcp
dynoxide mcp --db-path data.db
```

## Streamable HTTP transport

```sh
dynoxide mcp --http --port 19280
```

The HTTP transport requires a bearer token on every request. On a loopback
bind with no token supplied, dynoxide generates one on first run, saves it to a
per-user config file (`~/.config/dynoxide/mcp-token` on Linux,
`~/Library/Application Support/dynoxide/mcp-token` on macOS), and prints a
ready-to-paste client snippet; later runs reuse it silently. Supply your own
with `--token` or the `DYNOXIDE_MCP_AUTH_TOKEN` environment variable (the flag
wins if both are set).

| Flag | Purpose |
|------|---------|
| `--host <HOST>` | Bind address (default `127.0.0.1`). Non-loopback binds require an explicit token. |
| `--token <TOKEN>` / `DYNOXIDE_MCP_AUTH_TOKEN` | Use a fixed token instead of the persisted one. |
| `--allowed-host <HOST>` | Accept an additional `Host` header by name (repeatable); needed for non-loopback access by hostname. |
| `--no-auth` | Disable authentication. Loopback binds only; prints a warning. |

Prefer the environment variable or the persisted file over `--token` for
anything beyond one-shot debugging, because flag values leak into shell history and
`ps`. To rotate the token, delete the persisted file (or change
`DYNOXIDE_MCP_AUTH_TOKEN`) and restart; there is no rotation mechanism by
design.

On the `serve` subcommand the equivalent flags are prefixed
(`--mcp-host`, `--mcp-token`, `--mcp-no-auth`, `--mcp-allowed-host`) because
`serve` already owns `--host`/`--port` for the DynamoDB server.

To run the HTTP transport from the container image, see
[MCP over HTTP in Docker](#mcp-over-http-in-docker).

### HTTP client configuration

Point an HTTP-transport MCP client at the endpoint and send the token in an
`Authorization` header:

```json
{
  "mcpServers": {
    "dynoxide": {
      "type": "http",
      "url": "http://127.0.0.1:19280/mcp",
      "headers": { "Authorization": "Bearer <TOKEN>" }
    }
  }
}
```

## Claude Code configuration

Add to your `mcp.json`:

```json
{
  "mcpServers": {
    "dynoxide": {
      "command": "dynoxide",
      "args": ["mcp"]
    }
  }
}
```

Or with a persistent database:

```json
{
  "mcpServers": {
    "dynoxide": {
      "command": "dynoxide",
      "args": ["mcp", "--db-path", "dev.db"]
    }
  }
}
```

With a OneTable data model for single-table designs:

```json
{
  "mcpServers": {
    "dynoxide": {
      "command": "dynoxide",
      "args": ["mcp", "--db-path", "dev.db", "--data-model", "onetable.json"]
    }
  }
}
```

## Available tools (35)

| Category | Tools |
|----------|-------|
| Tables | `list_tables`, `describe_table`, `create_table`, `delete_table`, `update_table` |
| Items | `get_item`, `put_item`, `update_item`, `delete_item` |
| Batch | `batch_get_item`, `batch_write_item`, `bulk_put_items` |
| Query | `query`, `scan` |
| Vector search | `search_vectors` |
| Transactions | `transact_get_items`, `transact_write_items` |
| PartiQL | `execute_partiql`, `batch_execute_partiql`, `execute_transaction_partiql` |
| TTL | `update_time_to_live`, `describe_time_to_live`, `sweep_ttl` |
| Tags | `tag_resource`, `untag_resource`, `list_tags_of_resource` |
| Streams | `list_streams`, `describe_stream`, `get_shard_iterator`, `get_records` |
| Snapshots | `create_snapshot`, `restore_snapshot`, `list_snapshots`, `delete_snapshot` |
| Info | `get_database_info` |

## Vector indexes

An agent can create a vector index and search it without a wire client. `create_table` takes `vector_indexes`, `update_table` takes `vector_index_updates` to add or remove one, `describe_table` reports them alongside the GSIs, and `search_vectors` runs the search.

An index added to a live table through `update_table` is not searchable straight away. The first `search_vectors` against it answers `Cannot search backfilling vector index: <name>`, and it keeps answering that for a window that outlasts the `ACTIVE` `describe_table` reports, so waiting for the status is not enough. Retry the search and treat the refusal as "not yet". The table itself cannot be dropped while the index reports `CREATING`, which lifts earlier than the search does, so a successful `delete_table` is not evidence that a search would have worked. Cancelling the index is refused for a short window first, and the refusal names the phase to retry in. An index created as part of `create_table` is searchable at once and needs none of this.

The search is exact brute-force KNN over the whole index, so the top-k it returns is the true top-k. That is one of four things about vector search that will never match AWS, all of which can make a test pass here and fail there; `docs/compatibility-summary.md` sets them out in plain terms. `COSINE` and `EUCLIDEAN` score as distances, where a self match is 0; `DOT_PRODUCT` scores as a similarity and can be negative. The vector attribute is left out of results unless a projection expression names it and the index projects it. A table carrying a vector index has to be `PAY_PER_REQUEST`.

The tools are thin wrappers over the same engine the wire surface uses, so an agent sees the same behaviour and the same error text a DynamoDB client would. A tool's arguments arrive already parsed, without the order they were written in or a name written twice, so where DynamoDB would report whichever of two faults was written first, a tool may report the other one.

## Consumed capacity

The tools that wrap a DynamoDB read or write take an optional `return_consumed_capacity`, with the values `ReturnConsumedCapacity` takes on the wire: `TOTAL`, `INDEXES` or `NONE`. That covers the item tools, `query`, `scan`, `search_vectors`, `batch_get_item`, `batch_write_item`, both transaction tools and the three PartiQL tools. Leaving it out is the same as `NONE`: the reply is unchanged. Any other value is refused with DynamoDB's validation error.

With `TOTAL` or `INDEXES` the reply gains a `ConsumedCapacity` key, spelt in PascalCase like the rest of the reply, holding the figures the DynamoDB API returns for the same request. `get_item` with `TOTAL`:

```json
{
  "Item": {"pk": {"S": "user#1"}, "email": {"S": "alice@example.com"}},
  "ConsumedCapacity": {"TableName": "Users", "CapacityUnits": 0.5}
}
```

`INDEXES` breaks the total down by table and index. A `put_item` on a table with one GSI:

```json
{
  "ConsumedCapacity": {
    "TableName": "Users",
    "CapacityUnits": 2.0,
    "Table": {"CapacityUnits": 1.0},
    "GlobalSecondaryIndexes": {"byEmail": {"CapacityUnits": 1.0}}
  }
}
```

The single-item tools, `query`, `scan` and `execute_partiql` return one object. The batch and transaction tools, `batch_execute_partiql` and `execute_transaction_partiql` return an array with one entry per table, as DynamoDB does: `"ConsumedCapacity": [{"TableName": "Users", "CapacityUnits": 0.5}]`. `search_vectors` reports `VectorSearchRequestBytes` rather than capacity units.

When `--max-items` caps a `query` or `scan`, the figure is for the capped read, because that is the read that ran. On `query`, `scan` and `search_vectors` the block counts towards `--max-size-bytes`, so a page that only just fitted can be refused once capacity is asked for.

## Failed conditions

`put_item`, `update_item`, `delete_item` and `execute_partiql` take an optional `return_values_on_condition_check_failure`, with the values `ReturnValuesOnConditionCheckFailure` takes on the wire: `ALL_OLD` or `NONE`. With `ALL_OLD`, a write whose condition fails comes back as a `ConditionalCheckFailedException` tool error that also carries the item the condition was evaluated against, under `item`:

```json
{
  "error_type": "ConditionalCheckFailedException",
  "message": "The conditional request failed",
  "retryable": false,
  "item": {"pk": {"S": "user#1"}, "version": {"N": "3"}}
}
```

On `execute_partiql` that covers an UPDATE or DELETE whose WHERE condition is false for an item that exists. Leaving the argument out is the same as `NONE`, and any other value is refused with DynamoDB's validation error.

## Logging

`--log quiet` suppresses the `MCP HTTP server listening on ...` line and the
`Loaded data model` line, on both transports and on `serve --mcp`. The warning
printed when authentication is disabled and the message showing a newly
generated token still print, as do errors.

```sh
dynoxide mcp --http --log quiet
```

## Safety options

```sh
# Read-only mode - rejects all write operations
dynoxide mcp --read-only --db-path prod-snapshot.db

# Limit query/scan results
dynoxide mcp --max-items 100 --max-size-bytes 65536
```

## Snapshots

The MCP server supports database snapshots for safe experimentation:

- `create_snapshot` - saves a point-in-time copy of the database
- `restore_snapshot` - rolls back to a previous snapshot
- `list_snapshots` - lists available snapshots
- Auto-snapshot before `delete_table` (last 10 kept automatically)

## Data Model Context

For single-table designs, raw DynamoDB metadata (`pk` is type `S`, `GSI1` exists) tells an agent almost nothing. The `--data-model` flag loads a [OneTable](https://doc.onetable.io/) schema so the agent sees entity names, key templates, GSI mappings, and type discriminator attributes.

```sh
dynoxide mcp --data-model schema.json
dynoxide mcp --data-model schema.json --db-path data.db
```

The data model is context-only - dynoxide does not validate writes against the schema. See [docs/mcp-data-model.md](mcp-data-model.md) for the full format reference, options, and examples.

## Serving an import

`dynoxide import --mcp` loads a DynamoDB export into memory and serves it over stdio, so an agent can work on a copy of real data that disappears with the process. With `--rules`, the data is anonymised on the way in, and the run is gated on the result: if an original value reached the output, the import exits 3 before any server starts, and `--accept-exposure` lets it serve anyway. `--mcp-read-only` applies here as it does to `dynoxide mcp --read-only`, so the copy can be served without letting the agent write to it. The [exit codes](import.md#exit-codes) section of the import docs sets out what counts as an exposure.

The agent does not see the import's stderr, so the server tells it instead. The instructions name the import and its counts, and `get_database_info` returns an `import` object with every notice, its concern (`exposure` or `caution`), and whether exposures were accepted. Read it before treating the data as anonymised.
