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

