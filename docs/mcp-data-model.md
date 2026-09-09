# MCP Data Model Reference

The `--data-model` flag loads a [OneTable](https://doc.onetable.io/) schema so the MCP agent sees entity names, key templates, GSI mappings, and type discriminator attributes instead of raw DynamoDB metadata.

## Usage

```sh
dynoxide mcp --data-model schema.json
dynoxide mcp --data-model schema.json --db-path data.db
```

With `--data-model`, the MCP instructions include a compact entity summary and `get_database_info` returns the full model:

```json
{
  "data_model": {
    "schema_format": "onetable:1.1.0",
    "type_attribute": "_type",
    "entities": [
      {
        "name": "Account",
        "pk_template": "account#${id}",
        "sk_template": "account#",
        "type_attribute": "_type",
        "gsi_mappings": []
      },
      {
        "name": "User",
        "pk_template": "account#${accountId}",
        "sk_template": "user#${email}",
        "type_attribute": "_type",
        "gsi_mappings": [
          { "index_name": "GSI1", "pk_template": "user#${email}", "sk_template": "user#" }
        ]
      }
    ]
  }
}
```

The agent knows which entity types exist, how their keys are structured, and which GSI to query for a given access pattern before making a single query.

## Index name resolution

OneTable uses shorthand keys internally (e.g. `gs1`). If the index definition includes a `name` field (e.g. `"name": "GSI1"`), the parser uses the DynamoDB-facing name so it matches `describe_table` output and works directly with `query --index-name`.

## Options

```sh
# Control how many entities appear in MCP instructions (default: 20, 0 = suppress)
dynoxide mcp --data-model schema.json --data-model-summary-limit 10

# With serve --mcp (uses --mcp-data-model prefix)
dynoxide serve --mcp --mcp-data-model schema.json
```

## Important

The data model is context-only for MCP. Dynoxide does not validate writes against the schema. The MCP instructions note this explicitly so agents don't assume enforcement.

The same file has one active use: `dynoxide import --data-model` rebuilds keys from their entity templates after anonymisation, so a rule on `email` also rewrites `user#${email}`. See [import.md](import.md#single-table-designs).
