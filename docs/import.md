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
match = "begins_with(pk, :prefix)"
values = { ":prefix" = "USER#" }
path = "fullName"
action = { type = "fake", generator = "name" }

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

**Match expressions** use DynamoDB ConditionExpression syntax, so anything a
ConditionExpression can say works: `attribute_exists`, `attribute_not_exists`,
`attribute_type`, `begins_with`, `contains`, `size`, comparisons, `BETWEEN`,
`IN`, and `AND` / `OR` / `NOT`. As on DynamoDB, an operand that is not a path
is a `:name` reference rather than an inline literal, and the rule's `values`
table supplies it in the shape of ExpressionAttributeValues. Strings become
`S`, integers and floats `N`, booleans `BOOL`.

An attribute whose name is a DynamoDB reserved word (`name`, `status`, `type`
and the rest) goes through a `names` table as `#alias`, exactly as
ExpressionAttributeNames would:

```toml
[[rules]]
match = "begins_with(pk, :prefix) AND attribute_exists(#n)"
values = { ":prefix" = "USER#" }
names = { "#n" = "name" }
path = "name"
action = { type = "fake", generator = "name" }
```

The rules file is validated before any data is read. A reference with nothing
behind it, a name or value nothing references, and an operand of the wrong
type for its function all fail there, with the same messages DynamoDB gives
for the equivalent request.

**Consistency:** Fields listed in `[consistency].fields` produce the same anonymised value across all tables in a single import run. Same input + same salt = same output. Consistency is per field: `email` and `contactEmail` holding the same address get different fake values unless both are derived from one attribute, which is what `--data-model` does for keys.

### Single-table designs

Rules rewrite whole attribute values. In a single-table design the sensitive
value is usually also inside a key, `pk = "CUSTOMER#a.okonkwo@example.co.uk"`,
and a rule on `email` never touches it. Without a data model the import
reports this once in its warnings, because the output would otherwise look
anonymised while every partition key still holds the real address.

Pass the same [OneTable](https://doc.onetable.io/) schema the MCP server uses
as `--data-model` and the importer rebuilds keys from the anonymised
attributes:

```sh
dynoxide import \
  --source ./export/ \
  --schema schema.json \
  --rules rules.toml \
  --data-model onetable.json \
  --output anonymised.db
```

For each item the importer resolves the entity (by the type attribute,
`_type` by default, or failing that by which entity's key templates reproduce
the item's keys), notes which of `pk`, `sk` and the GSI keys the entity builds
from a template such as `user#${email}`, applies the rules to the attributes,
then renders those keys again. The prefix survives, and the key and the
attribute agree by construction, so a rule on `email` is all a `User` entity
needs. Which model attributes hold the primary key comes from the schema's
`indexes.primary`, the DynamoDB attribute names come from the `--schema` file,
and GSI keys are matched by index name, so the OneTable index `name` must
match the DynamoDB index.

Templates follow OneTable's own forms: `${name}`, a dotted path that reaches
into a map (`${address.city}`), and `${name:length:pad}` sort padding, which
prefixes the value with `pad` (default `0`) until it is `length` characters.
An unclosed `${` fails the import rather than leaving a key silently
unrebuilt, and so does a key built from another templated key, which could
only be rendered correctly in dependency order.

An item carrying the type attribute is matched by it. One that does not is
matched on the shape of its keys, constant templates included, since a
constant `sk` is often the only thing separating two entities that share a
partition template.

An index counts when the entity templates either of its keys, so a GSI that
hashes on a plain attribute such as a tenant id and sorts on
`user#${email}` still gets its sort key rebuilt.

A key is only rewritten when its template reproduces the value the item
arrived with. A key the template cannot reproduce, or that names an attribute
the item does not carry, is left as it is and reported once per entity and
key. That last case is the one to watch for: an `Order` item whose `pk` holds
the customer's email but which has no attribute holding that email cannot be
rebuilt, because there is nothing to derive it from. Give the entity an
attribute for the value and put it in the template.

**Name that attribute the same on every entity that shares the value, and
list the name in `[consistency] fields`.** Both halves are load-bearing,
because consistency is keyed on the attribute name. An `Order` carrying
`customerEmail` where the `Customer` carries `email` is two namespaces, so
the same real address anonymises to two different values, both keys rebuild
correctly, and the order lands in a different partition from its customer.
The same name in both places with no `[consistency]` entry fails the same
way, since each item is then anonymised independently. Name it `email` on
both and list `email`, and the order stays in its customer's partition.

The importer enforces the second half. When two entities build the same key
from the same template, an anonymised attribute that template reads has to be
consistency-tracked, or their keys cannot agree. That is a warning when the
model says it could happen, and an error once two entities are seen to take
the same original key to different values:

```
entity 'Customer' and entity 'Order' took the same original pk to different
values, so their keys no longer agree and they will not join.
Template 'CUSTOMER#${email}' reads 'email': add 'email' to [consistency] fields
```

It compares the whole key rather than one attribute the template reads, so two
composite keys that merely share a component are left alone:
`TENANT#a#${email}` and `TENANT#b#${email}` never agreed and have no join to
lose. It fails on what the anonymisation actually produced, rather than on the
model or on a shared input, so importing one entity's slice still works, and
so does a deterministic action such as `hash`, which takes both entities to
the same result and keeps them joined without any `[consistency]` entry at
all. Every outcome for a key is kept rather than the first, so a break is
found whatever order the export happens to be in.

The field it names is always a top-level one, because that is what
`[consistency] fields` is keyed on. A template reading `${contact.email}`
is reported as `contact`. Nothing is written on the error path, so a failed
import leaves no database rather than a broken one. Matching on the key and
its template, rather than on the attribute name, keeps this off entities that
merely reuse a name: `account#${id}` and `project#${id}` are different
entities' own ids and never had a join. An attribute no rule rewrites is left
alone too, since its keys still agree.

**Use `fake` or `hash` for an attribute a key is built from.** `redact`,
`null` and `mask` produce the same output for every input, so every item of
that entity would render an identical key and overwrite the previous one,
leaving a database with fewer rows than the export. The importer says so
before it reads any data, and counts the collisions if you go ahead anyway.

**A rule that names a key attribute directly wins on the items it rewrote.**
That key is not rebuilt from its template and the rule's value is stored
instead. It is decided from what the rules actually did to each item, not
from which rules looked like they would match, because each rule's condition
sees the item as the rules before it left it. Deciding otherwise would leave
a key unrebuilt on items the rule never touched, real value intact.

When `--mcp` is set, `--data-model` also serves as the MCP data model unless
`--mcp-data-model` is given.

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

