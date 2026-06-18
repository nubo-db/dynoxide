# DynamoDB Streams

Dynoxide supports DynamoDB Streams with all four view types: `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES`, and `KEYS_ONLY`.

## Enabling streams

Streams are enabled per-table via `StreamSpecification` in `CreateTable` or `UpdateTable`, exactly like real DynamoDB:

```sh
# Via AWS CLI
aws dynamodb create-table \
  --endpoint-url http://localhost:8000 \
  --table-name Events \
  --key-schema AttributeName=pk,KeyType=HASH \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

# Enable on an existing table
aws dynamodb update-table \
  --endpoint-url http://localhost:8000 \
  --table-name Events \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

Via the MCP server, pass `stream_specification` to `create_table` or `update_table`.

## Reading stream records

```sh
# List streams
aws dynamodbstreams list-streams --endpoint-url http://localhost:8000

# Describe a stream to get shard IDs
aws dynamodbstreams describe-stream \
  --endpoint-url http://localhost:8000 \
  --stream-arn arn:aws:dynamodb:local:000000000000:table/Events/stream/...

# Get a shard iterator and read records
aws dynamodbstreams get-shard-iterator \
  --endpoint-url http://localhost:8000 \
  --stream-arn <stream-arn> \
  --shard-id <shard-id> \
  --shard-iterator-type TRIM_HORIZON
```

## Streams with import

If the `--schema` file (DescribeTable JSON) contains a `StreamSpecification`, streams are automatically enabled on the imported table. No extra flags needed. The import faithfully reproduces the source table's configuration:

```json
{
  "Table": {
    "TableName": "Events",
    "StreamSpecification": {
      "StreamEnabled": true,
      "StreamViewType": "NEW_AND_OLD_IMAGES"
    }
  }
}
```

Note: Imported items do not generate stream records by default (bulk import bypasses stream recording for performance). Stream recording begins for writes made after import completes.

