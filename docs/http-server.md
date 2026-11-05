# HTTP Server

Start the server:

```sh
dynoxide --port 8000
```

With a persistent database:

```sh
dynoxide --db-path data.db --port 8000
```

With encryption (requires the `encrypted-server` build):

```sh
# Generate a key
openssl rand -hex 32 > key.hex
chmod 600 key.hex

# Start with key file
dynoxide --db-path data.db --encryption-key-file key.hex

# Or via environment variable
DYNOXIDE_ENCRYPTION_KEY=$(cat key.hex) dynoxide --db-path data.db
```

With tables pre-created from a schema:

```sh
# Generate schema
aws dynamodb describe-table --table-name Users > schema.json

# Start with schema
dynoxide --schema schema.json --port 8000
```

Then use the AWS CLI or any DynamoDB SDK pointed at localhost:

```sh
aws dynamodb list-tables --endpoint-url http://localhost:8000

aws dynamodb put-item \
  --endpoint-url http://localhost:8000 \
  --table-name Users \
  --item '{"pk": {"S": "user#1"}, "name": {"S": "Alice"}}'

aws dynamodb get-item \
  --endpoint-url http://localhost:8000 \
  --table-name Users \
  --key '{"pk": {"S": "user#1"}}'
```

Works with any language or SDK that supports custom endpoints: Python (boto3), Node.js (AWS SDK v3), Go, Java, etc.

## Logging

The default output is unchanged when `--log` is omitted. To suppress
informational startup and shutdown messages on stderr, use quiet mode:

```sh
dynoxide --log quiet --port 8000
```

With `--mcp`, quiet mode also covers the MCP listening line and the
`Loaded data model` line. Two MCP lines still print, because they are about
access rather than progress: the warning that `--mcp-no-auth` has turned
authentication off, and the message showing a newly generated token. Errors
always print.

Quiet mode removes the `Dynoxide listening on ...` line, so a script that waits
for Dynoxide to come up should wait on the port or on `GET /` rather than on
that line.

With `--port 0` the OS assigns a free port, and the startup line reports the
port it assigned. A test harness that runs several servers at once can start
each on port 0 and read its port from that line, rather than choosing a free
port itself and racing other processes to bind it. That needs the default
logging mode, since quiet mode removes the line.

`RUST_LOG` continues to control tracing independently.
