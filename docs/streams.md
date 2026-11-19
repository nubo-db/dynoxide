# DynamoDB Streams

Dynoxide supports DynamoDB Streams with all four view types: `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES`, and `KEYS_ONLY`.

## Enabling streams

Streams are enabled per table with `StreamSpecification` in `CreateTable` or `UpdateTable`, as on real DynamoDB:

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

If the `--schema` file (DescribeTable JSON) contains a `StreamSpecification`, the imported table gets streams enabled with the same settings as the source table. No extra flags are needed:

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

Imported items don't generate stream records by default, because bulk import skips stream recording for speed. Writes made after the import finishes are recorded as normal.

