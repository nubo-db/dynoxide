# Laravel

Laravel can keep its cache, atomic locks, sessions and failed jobs in
DynamoDB. Dynoxide runs all four locally, so development and tests need
neither AWS nor `amazon/dynamodb-local`.

Laravel's own DynamoDB integration tests pass against it: on 11 November 2026,
`tests/Integration/Cache/DynamoDbStoreTest.php`,
`tests/Integration/Queue/DynamoBatchTest.php` and
`tests/Integration/Queue/DynamoBatchTestWithTTL.php` from laravel/framework
(commit `c33d357`) ran against Dynoxide 1.6.0, with the environment Laravel's
own CI uses for `amazon/dynamodb-local`: 18 tests, all passing.

## The tables

Laravel expects two tables, and Dynoxide can create them as it starts. Save
this as `dynamodb-tables.json` beside your `docker-compose.yml`:

```json
[
  {
    "Table": {
      "TableName": "cache",
      "KeySchema": [{ "AttributeName": "key", "KeyType": "HASH" }],
      "AttributeDefinitions": [{ "AttributeName": "key", "AttributeType": "S" }],
      "BillingModeSummary": { "BillingMode": "PAY_PER_REQUEST" }
    }
  },
  {
    "Table": {
      "TableName": "failed_jobs",
      "KeySchema": [
        { "AttributeName": "application", "KeyType": "HASH" },
        { "AttributeName": "uuid", "KeyType": "RANGE" }
      ],
      "AttributeDefinitions": [
        { "AttributeName": "application", "AttributeType": "S" },
        { "AttributeName": "uuid", "AttributeType": "S" }
      ],
      "BillingModeSummary": { "BillingMode": "PAY_PER_REQUEST" }
    }
  }
]
```

`cache` holds the cache, the locks and, with the `dynamodb` session driver,
the sessions. You only need `failed_jobs` if failed jobs go to DynamoDB too.
Both names are Laravel's defaults.

## Run Dynoxide

With Docker Compose:

```yaml
services:
  dynamodb:
    image: ghcr.io/nubo-db/dynoxide
    command: serve --host 0.0.0.0 --port 8000 --schema /data/dynamodb-tables.json
    ports:
      - "8000:8000"
    volumes:
      - ./dynamodb-tables.json:/data/dynamodb-tables.json:ro
```

Or without Docker, from the project directory:

```sh
npx dynoxide --port 8000 --schema dynamodb-tables.json
```

Either way the data lives in memory and goes when Dynoxide stops, which is
what a test suite wants. To keep it, add `--db-path`; see
[Docker](installation.md#docker).

## Point Laravel at it

In `.env`:

```ini
CACHE_STORE=dynamodb
DYNAMODB_ENDPOINT=http://localhost:8000
AWS_ACCESS_KEY_ID=dynoxide
AWS_SECRET_ACCESS_KEY=dynoxide
AWS_DEFAULT_REGION=us-east-1
```

On Laravel 10 and earlier the cache variable is `CACHE_DRIVER`, not
`CACHE_STORE`. Use `http://dynamodb:8000` instead when the app runs in the
same Compose project.

For tests, Laravel's `phpunit.xml` already sets `CACHE_STORE` and
`SESSION_DRIVER` to `array`, and PHPUnit keeps the first value it sees, so
change those lines rather than adding new ones. Any non-empty credentials work: Dynoxide checks they're there and
never checks the signature.

`config/cache.php` already reads `DYNAMODB_ENDPOINT` for its `dynamodb` store,
so the cache and locks need nothing more.

### Sessions

Set `SESSION_DRIVER=dynamodb`. Sessions then go through the `dynamodb` cache
store and its table.

### Failed jobs

Set `QUEUE_FAILED_DRIVER=dynamodb`. Unlike the cache store, the `failed` entry
in Laravel's generated `config/queue.php` has no credentials, region or
endpoint, so add them:

```php
'failed' => [
    'driver' => env('QUEUE_FAILED_DRIVER', 'database-uuids'),
    'database' => env('DB_CONNECTION', 'sqlite'),
    'table' => 'failed_jobs',
    'key' => env('AWS_ACCESS_KEY_ID'),
    'secret' => env('AWS_SECRET_ACCESS_KEY'),
    'region' => env('AWS_DEFAULT_REGION', 'us-east-1'),
    'endpoint' => env('DYNAMODB_ENDPOINT'),
],
```

## In GitHub Actions

Swap the image in the service container:

```yaml
services:
  dynamodb:
    image: ghcr.io/nubo-db/dynoxide
    ports:
      - 8000:8000
```

A service container can't be given `--schema`, so create the tables from the
test suite instead. A base test case can do it once per run, ignoring the
error when the table is already there:

```php
use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;

private static bool $tablesCreated = false;

protected function setUp(): void
{
    parent::setUp();

    if (self::$tablesCreated) {
        return;
    }
    self::$tablesCreated = true;

    $client = new DynamoDbClient([
        'region' => 'us-east-1',
        'version' => 'latest',
        'endpoint' => env('DYNAMODB_ENDPOINT'),
        'credentials' => ['key' => 'dynoxide', 'secret' => 'dynoxide'],
    ]);

    try {
        $client->createTable([
            'TableName' => 'cache',
            'KeySchema' => [['AttributeName' => 'key', 'KeyType' => 'HASH']],
            'AttributeDefinitions' => [['AttributeName' => 'key', 'AttributeType' => 'S']],
            'BillingMode' => 'PAY_PER_REQUEST',
        ]);
    } catch (DynamoDbException $e) {
        if ($e->getAwsErrorCode() !== 'ResourceInUseException') {
            throw $e;
        }
    }
}
```

Add `failed_jobs` the same way if you use it, with `application` as the
partition key and `uuid` as the sort key.
