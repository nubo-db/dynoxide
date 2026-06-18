# Import CLI

Import data from DynamoDB Export (JSON Lines format) into a Dynoxide database, with optional anonymisation.

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

