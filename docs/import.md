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
| | `fake` also takes an optional `seed_env`, below |
| `redact` | Replace with `[REDACTED]` |
| `null` | Replace with NULL |

**Consistency:** Fields listed in `[consistency].fields` produce the same anonymised value across all tables in a single import run. Same input + same salt = same output.

### Making `fake` repeatable

By default `fake` draws a new value every time it runs, so importing the same
export twice gives two different sets of names and addresses. That is fine for
a throwaway database and awkward for one committed as a test fixture, where
every refresh rewrites values that did not change and buries any real
difference in churn.

Give the rule a `seed_env` and the generated value becomes a function of the
original:

```toml
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email", seed_env = "ANON_SEED" }
```

```sh
ANON_SEED=a-secret-value dynoxide import ...
```

The same input and the same seed give the same output on every run, so a
fixture only changes where the source data did. A different seed gives an
entirely different mapping.

**The seed is a secret, for the same reason the hash salt is.** Without it,
anyone holding the original data could re-run the import and reproduce the
mapping from real value to fake one, which undoes the anonymisation. An empty
variable is rejected rather than accepted quietly, since that is the shape an
unset CI secret takes.

A seeded rule does not need its field in `[consistency]`. The derivation
already guarantees one input maps to one output everywhere.

### Generated values and collisions

`safe_email` used to produce roughly nine thousand possible addresses, a first
name against three `example.` domains. That is small enough that a few hundred
items produce repeats, and a repeat is not cosmetic: if the attribute is one a
key is built from, two people collapse onto the same key and one row overwrites
the other.

Generated addresses now carry a derived suffix in the local part, so
`alice@example.com` becomes something of the form
`juvenal.3f2a91b8@example.com`. It is still an address, and there is enough
room that repeats do not happen at any realistic size.

The other generators keep their pools. `word`, `first_name` and the rest are
small, so prefer `hash` or a seeded `safe_email` for anything a key is built
from.

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

