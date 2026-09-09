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

The promise is scoped to a fixed build. The value is produced by a generator
drawing from a seeded random stream, and neither the stream nor the generator's
word lists promise to stay identical across upgrades of those dependencies. So
the same dynoxide gives the same answer every time, and a future dynoxide may
not. Refresh a fixture in one go rather than expecting values to survive an
upgrade untouched.

Only scalar values are derived. A map, list or set has no stable byte order to
hash, so those draw fresh each time even with a seed set, rather than claim a
repeatability they cannot deliver.

**The seed is a secret, for the same reason the hash salt is.** Anyone holding
both the original data and the seed can re-run the import and reproduce the
mapping from real value to fake one, which undoes the anonymisation. Use a
randomly generated value, not a memorable one: a short or guessable seed can be
searched offline against a handful of known pairs. An empty variable is
rejected rather than accepted quietly, since that is the shape an unset CI
secret takes.

Omitting `seed_env` is not the same risk. Without a seed there is no mapping to
reproduce, because each run draws fresh. The exposure comes from a seed that
someone else can obtain or guess.

A seeded rule does not need its field in `[consistency]`. The derivation
already guarantees one input maps to one output everywhere.

Do not mix rule shapes on one consistency field. A seeded rule derives its
value and never touches the consistency map, so pairing it with an unseeded
rule, a different seed or a different generator on the same field means one
input can leave with two different values depending on which rule matched. The
import reports it, because nothing in the output would.

### Generated values and collisions

`safe_email` used to produce roughly nine thousand possible addresses, a first
name against three `example.` domains. That is small enough that a few hundred
items produce repeats, and a repeat is not cosmetic: if the attribute is one a
key is built from, two people collapse onto the same key and one row overwrites
the other.

Generated addresses now carry a derived suffix in the local part, so
`alice@example.com` becomes something of the form
`juvenal.3f2a91b8c4d5e6f7@example.com`. It is still an address, and the space
is around 1.7e23.

That is a probabilistic bound rather than a guarantee. Duplicates become
likely far sooner than the size of the space suggests, so the number matters:
a shorter suffix would still give roughly a one in a hundred chance of some
duplicate across a million distinct inputs, which is an ordinary export. The
import counts collisions either way, because a merged identity is silent.

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

