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

## Available tools (34)

| Category | Tools |
|----------|-------|
| Tables | `list_tables`, `describe_table`, `create_table`, `delete_table`, `update_table` |
| Items | `get_item`, `put_item`, `update_item`, `delete_item` |
| Batch | `batch_get_item`, `batch_write_item`, `bulk_put_items` |
| Query | `query`, `scan` |
| Transactions | `transact_get_items`, `transact_write_items` |
| PartiQL | `execute_partiql`, `batch_execute_partiql`, `execute_transaction_partiql` |
| TTL | `update_time_to_live`, `describe_time_to_live`, `sweep_ttl` |
| Tags | `tag_resource`, `untag_resource`, `list_tags_of_resource` |
| Streams | `list_streams`, `describe_stream`, `get_shard_iterator`, `get_records` |
| Snapshots | `create_snapshot`, `restore_snapshot`, `list_snapshots`, `delete_snapshot` |
| Info | `get_database_info` |

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

