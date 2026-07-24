# Import CLI

Import data from DynamoDB Export (JSON Lines format) into a Dynoxide database, with optional anonymisation.

Import runs in one of two mutually exclusive modes. **File mode** (`--output`)
writes a SQLite file, vacuums it, and optionally compresses it. **Serve mode**
(`--serve` or `--mcp`) imports into an in-memory database and starts a server
on top of it, leaving nothing on disk.

## Basic import

```sh
dynoxide import \
  --source ./export-data/ \
  --schema schema.json \
  --output snapshot.db
```

The `--source` directory should follow DynamoDB Export structure:

```
export-data/
├── Users/
│   └── data/
│       └── 00000000.json.gz
└── Orders/
    └── data/
        └── 00000000.json.gz
```

The `--schema` file contains DescribeTable JSON (the output of `aws dynamodb describe-table`):

```sh
aws dynamodb describe-table --table-name Users > schema.json
```

## Table filtering

```sh
dynoxide import --source ./export/ --schema schema.json --output snapshot.db \
  --tables Users,Orders
```

## Anonymisation

Create a rules file (`rules.toml`):

```toml
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }

[[rules]]
match = "attribute_exists(phone)"
path = "phone"
action = { type = "mask", keep_last = 4, mask_char = "*" }

[[rules]]
match = "attribute_exists(ssn)"
path = "ssn"
action = { type = "hash", salt_env = "ANON_SALT" }

[[rules]]
match = "attribute_exists(notes)"
path = "notes"
action = { type = "redact" }

[consistency]
fields = ["userId", "email"]
```

```sh
ANON_SALT=my-secret-salt dynoxide import \
  --source ./export/ \
  --schema schema.json \
  --rules rules.toml \
  --output anonymised.db
```

**Action types:**

| Action | Description |
|--------|-------------|
| `fake` | Replace with generated data (`safe_email`, `name`, `phone_number`, `address`, `company_name`, `sentence`, `word`, `first_name`, `last_name`) |
| `mask` | Keep last N characters, mask the rest (`keep_last`, `mask_char`) |
| `hash` | SHA-256 hash with salt from env var (`salt_env`, required) |
| `redact` | Replace with `[REDACTED]` |
| `null` | Replace with NULL |

**Consistency:** Fields listed in `[consistency].fields` produce the same anonymised value across all tables in a single import run. Same input + same salt = same output.

## Options

```sh
# Overwrite an existing output file
dynoxide import --source ./export/ --schema schema.json --output snapshot.db --force

# Continue importing when a batch fails instead of aborting
dynoxide import --source ./export/ --schema schema.json --output snapshot.db --continue-on-error

# Compress output with zstd
dynoxide import --source ./export/ --schema schema.json --output snapshot.db --compress
# Produces snapshot.db.zst
```

## Serve mode

Import straight into memory and serve it, with no file written. Useful for a
throwaway environment seeded from a production export, where you want the data
to disappear when the process does.

```sh
# HTTP server on the imported data
dynoxide import --source ./export/ --schema schema.json --serve --port 8000

# stdio MCP server on the imported data
dynoxide import --source ./export/ --schema schema.json --mcp

# Both: HTTP on --port, MCP over HTTP on --mcp-port
dynoxide import --source ./export/ --schema schema.json --serve --mcp --mcp-port 8100
```

`--serve` and `--mcp` both conflict with `--output`. Used alone, `--mcp` starts
a stdio MCP server; combined with `--serve` it starts an HTTP MCP server
instead.

| Flag | Default | Description |
|------|---------|-------------|
| `--serve` | off | Start an HTTP server on the imported data |
| `--host` | `127.0.0.1` | Bind address (requires `--serve`) |
| `--port` | `8000` | HTTP port (requires `--serve`) |
| `--mcp` | off | Start an MCP server on the imported data |
| `--mcp-port` | `8100` | MCP HTTP port, used with `--serve --mcp` |
| `--mcp-read-only` | off | Reject write operations over MCP (requires `--mcp`) |

Anonymisation applies the same way in serve mode: pass `--rules` and the data
is masked before it reaches the in-memory database.

