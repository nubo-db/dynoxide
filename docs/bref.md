# Bref

[Bref](https://bref.sh) runs PHP on AWS Lambda, and Bref apps often keep
Laravel's cache and sessions, Symfony's cache, or the data behind a DynamoDB
Streams handler in DynamoDB. Dynoxide runs all of that locally, beside Bref's
own Docker images, so development and tests need no AWS account and no
credentials.

Everything on this page was run on 27 November 2026 with Dynoxide 1.8.1,
`bref/bref` 3.0.17 on `bref/php-84-dev:3` (PHP 8.4), and `aws/aws-sdk-php`
3.408.0. The Laravel section used Laravel 13.44.1 with `bref/laravel-bridge`
3.1.8, and the Symfony section Symfony 7.4.24 with
`rikudou/psr6-dynamo-db-bundle` 4.2.0.

## Local development with Docker Compose

Bref's [local development guide](https://bref.sh/docs/local-development) runs
the app in `bref/php-84-dev:3` with Docker Compose. Add Dynoxide as a second
service, and have it create your tables as it starts from a `--schema` file:

```yaml
services:
  dynamodb:
    image: ghcr.io/nubo-db/dynoxide
    command: serve --host 0.0.0.0 --port 8000 --schema /data/dynamodb-tables.json
    ports:
      - "8000:8000"
    volumes:
      - ./dynamodb-tables.json:/data/dynamodb-tables.json:ro

  app:
    image: bref/php-84-dev:3
    volumes:
      - .:/var/task
    environment:
      AWS_ENDPOINT_URL_DYNAMODB: http://dynamodb:8000
      AWS_ENDPOINT_URL_DYNAMODB_STREAMS: http://dynamodb:8000
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: dynoxide
      AWS_SECRET_ACCESS_KEY: dynoxide
    depends_on:
      dynamodb:
        condition: service_healthy
```

The endpoint is the Compose service name, `dynamodb`, not `localhost`: inside
the app container, `localhost` is the app container itself. Any non-empty
credentials work, because Dynoxide checks they're there and never checks the
signature. The data lives in memory and goes when the container stops; to
keep it, see [Docker](installation.md#docker).

`dynamodb-tables.json` holds the tables in the shape `aws dynamodb
describe-table` prints. The sections below give the tables each one needs.

The AWS SDK for PHP reads `AWS_ENDPOINT_URL_DYNAMODB` from 3.298.9.
`bref/bref` doesn't require the SDK and `bref/laravel-bridge` accepts 3.222
or later, so an older version can be installed. Check yours with
`composer show aws/aws-sdk-php` if requests go to AWS instead of Dynoxide.
The Laravel and Symfony set-ups below pass the endpoint in their own config
and don't depend on it.

## Running functions

`bref-local` invokes a function in the app container, and the function's
DynamoDB client picks up the endpoint from the environment:

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use Aws\DynamoDb\DynamoDbClient;

$dynamoDb = new DynamoDbClient([]);

return function (array $event) use ($dynamoDb): array {
    $dynamoDb->putItem([
        'TableName' => 'orders',
        'Item' => [
            'id' => ['S' => $event['id']],
            'total' => ['N' => (string) $event['total']],
        ],
    ]);

    $item = $dynamoDb->getItem([
        'TableName' => 'orders',
        'Key' => ['id' => ['S' => $event['id']]],
    ])['Item'];

    return ['id' => $item['id']['S'], 'total' => (int) $item['total']['N']];
};
```

```sh
docker compose run app vendor/bin/bref-local create-order.php '{"id":"order-7","total":42}'
```

The `orders` table it writes to:

```json
[
  {
    "Table": {
      "TableName": "orders",
      "KeySchema": [{ "AttributeName": "id", "KeyType": "HASH" }],
      "AttributeDefinitions": [{ "AttributeName": "id", "AttributeType": "S" }],
      "BillingModeSummary": { "BillingMode": "PAY_PER_REQUEST" },
      "StreamSpecification": { "StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES" }
    }
  }
]
```

The stream isn't needed to run the function. It's there for the
[Streams recipe](#dynamodb-streams) below.

## Tests

Bref adds nothing to test set-up, so the usual tools apply:

- The [PHP package](https://github.com/nubo-db/dynoxide-php#readme),
  `nubo-db/dynoxide`, has a PHPUnit extension that starts Dynoxide for the
  test run, creates the tables in a schema file and points the AWS SDK at it.
- On Laravel 12 or 13, the [Laravel package](laravel.md#with-the-package)
  does the same for the cache, sessions and failed jobs.

## Laravel on Bref

Bref's [caching page](https://bref.sh/docs/laravel/caching) sets the
`dynamodb` store's key and expiry attributes to `id` and `ttl` in
`config/cache.php`:

```php
'dynamodb' => [
    'driver' => 'dynamodb',
    'key' => env('AWS_ACCESS_KEY_ID'),
    'secret' => env('AWS_SECRET_ACCESS_KEY'),
    'region' => env('AWS_DEFAULT_REGION', 'us-east-1'),
    'table' => env('DYNAMODB_CACHE_TABLE', 'cache'),
    'endpoint' => env('DYNAMODB_ENDPOINT'),
    'attributes' => [
        'key' => 'id',
        'expiration' => 'ttl',
    ],
],
```

The cache, atomic locks and, with the `dynamodb` session driver, sessions all
share that one table, keyed on `id`:

```json
[
  {
    "Table": {
      "TableName": "cache",
      "KeySchema": [{ "AttributeName": "id", "KeyType": "HASH" }],
      "AttributeDefinitions": [{ "AttributeName": "id", "AttributeType": "S" }],
      "BillingModeSummary": { "BillingMode": "PAY_PER_REQUEST" }
    }
  }
]
```

Bref's [sessions page](https://bref.sh/docs/laravel/sessions) keeps Laravel's
default attribute names instead, `key` and `expires_at`, in a table keyed on
`key`. Whichever you use, the table's key has to match the `attributes`
setting. If it doesn't, every write fails with `Missing the key id in the
item` (or `key`).

Laravel's `config/cache.php` passes `DYNAMODB_ENDPOINT` to the client, so set
that rather than the SDK's variable. In the app service's `environment`:

```yaml
CACHE_STORE: dynamodb
SESSION_DRIVER: dynamodb
DYNAMODB_CACHE_TABLE: cache
DYNAMODB_ENDPOINT: http://dynamodb:8000
AWS_ACCESS_KEY_ID: dynoxide
AWS_SECRET_ACCESS_KEY: dynoxide
AWS_DEFAULT_REGION: us-east-1
```

Then Artisan runs against Dynoxide:

```sh
docker compose run app php artisan tinker --execute 'Cache::put("greeting", "hello", 60); dump(Cache::get("greeting"));'
```

The table doesn't need a TTL setting locally. On AWS, TTL deletes expired
items in the background, but Laravel checks the expiry itself when it reads,
so an expired value is never returned either way.

## Symfony on Bref

Bref's [Symfony caching page](https://bref.sh/docs/symfony/caching) uses
`rikudou/psr6-dynamo-db-bundle`. Install it with one of its dependencies held
at 3.2.0:

```sh
composer require rikudou/psr6-dynamo-db-bundle rikudou/psr6-dynamo-db:3.2.0
```

On a fresh install, bundle 4.2.0 picks up `rikudou/psr6-dynamo-db` 3.2.1,
which expects a different clock object from the one the bundle passes it.
Every cache read then fails with a `TypeError`, against AWS as well as
Dynoxide. A [fix](https://github.com/RikudouSage/DynamoDbCachePsr6Bundle/pull/41)
is open on the bundle; once a release includes it, the pin can go.

The bundle's Flex recipe enables it in the `prod` environment only, so
locally, in `dev`, the app never reaches DynamoDB. Enable it for `dev` too in
`config/bundles.php`:

```php
Rikudou\DynamoDbCacheBundle\RikudouDynamoDbCacheBundle::class => ['prod' => true, 'dev' => true],
```

The bundle's `client_config` takes only a region, so give it a client of your
own for `dev`, pointed at Dynoxide. In
`config/packages/dev/rikudou_dynamo_db_cache.yaml`:

```yaml
rikudou_dynamo_db_cache:
    table: '%env(DYNAMODB_CACHE_TABLE)%'
    replace_default_adapter: true
    client_service: dynoxide.dynamodb_client

services:
    dynoxide.dynamodb_client:
        class: AsyncAws\DynamoDb\DynamoDbClient
        arguments:
            - endpoint: '%env(DYNAMODB_ENDPOINT)%'
              region: us-east-1
              accessKeyId: dynoxide
              accessKeySecret: dynoxide
```

Set `DYNAMODB_ENDPOINT: http://dynamodb:8000` in the app service's
`environment`. The recipe already put `DYNAMODB_CACHE_TABLE=cache` in `.env`.
The table is keyed on `id`, the bundle's default, so it's the same `cache`
table as in the Laravel section. With that in place, services autowired with
`CacheInterface` read and write Dynoxide, and `bin/console` commands run with
`docker compose run app php bin/console ...`.

The bundle is built on AsyncAws, which doesn't read
`AWS_ENDPOINT_URL_DYNAMODB`. AsyncAws does read `AWS_ENDPOINT_URL`, but that
sends every AsyncAws client to Dynoxide, SQS and S3 included, which is why
the client above sets its endpoint itself.

## DynamoDB Streams

On Lambda, a DynamoDB Streams handler receives a batch of stream records as
its event. Dynoxide keeps the records too, so a test can make the changes,
read the records back and hand them to the handler as Lambda would.

This handler records every change to an order in an `order_events` table. The
snippets assume `composer.json` autoloads `App\` from `src/` and `Tests\`
from `tests/`:

```php
<?php

namespace App;

use Aws\DynamoDb\DynamoDbClient;
use Bref\Context\Context;
use Bref\Event\DynamoDb\DynamoDbEvent;
use Bref\Event\DynamoDb\DynamoDbHandler;

final class OrderEventsHandler extends DynamoDbHandler
{
    public function __construct(private DynamoDbClient $dynamoDb)
    {
    }

    public function handleDynamoDb(DynamoDbEvent $event, Context $context): void
    {
        foreach ($event->getRecords() as $record) {
            $this->dynamoDb->putItem([
                'TableName' => 'order_events',
                'Item' => [
                    'order_id' => $record->getKeys()['id'],
                    'sequence' => ['S' => $record->getSequenceNumber()],
                    'event' => ['S' => $record->getEventName()],
                    'total' => $record->getNewImage()['total'] ?? $record->getOldImage()['total'],
                ],
            ]);
        }
    }
}
```

The tables, with the stream on `orders`. For the polling script below, put
both in `dynamodb-tables.json`:

```json
[
  {
    "Table": {
      "TableName": "orders",
      "KeySchema": [{ "AttributeName": "id", "KeyType": "HASH" }],
      "AttributeDefinitions": [{ "AttributeName": "id", "AttributeType": "S" }],
      "BillingModeSummary": { "BillingMode": "PAY_PER_REQUEST" },
      "StreamSpecification": { "StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES" }
    }
  },
  {
    "Table": {
      "TableName": "order_events",
      "KeySchema": [
        { "AttributeName": "order_id", "KeyType": "HASH" },
        { "AttributeName": "sequence", "KeyType": "RANGE" }
      ],
      "AttributeDefinitions": [
        { "AttributeName": "order_id", "AttributeType": "S" },
        { "AttributeName": "sequence", "AttributeType": "S" }
      ],
      "BillingModeSummary": { "BillingMode": "PAY_PER_REQUEST" }
    }
  }
]
```

### In tests

A stream record from `GetRecords` is nearly the record Lambda delivers. It
lacks `eventSourceARN`, and the SDK turns `ApproximateCreationDateTime` into
a date object that Bref expects back as epoch seconds. This class reads the
records written since its last call and makes those two changes:

```php
<?php

namespace Tests;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDbStreams\DynamoDbStreamsClient;
use Bref\Event\DynamoDb\DynamoDbEvent;

final class StreamEvents
{
    /** @var array<string, string> the next shard iterator for each stream */
    private array $iterators = [];

    public function __construct(
        private DynamoDbClient $dynamoDb,
        private DynamoDbStreamsClient $streams,
    ) {
    }

    /** Reads the records written since the last call, as the event Lambda would deliver. */
    public function next(string $table): DynamoDbEvent
    {
        $streamArn = $this->dynamoDb->describeTable(['TableName' => $table])['Table']['LatestStreamArn'];
        $iterator = $this->iterators[$streamArn] ??= $this->firstIterator($streamArn);

        $result = $this->streams->getRecords(['ShardIterator' => $iterator]);
        $this->iterators[$streamArn] = $result['NextShardIterator'];

        $records = [];
        foreach ($result['Records'] as $record) {
            $record['dynamodb']['ApproximateCreationDateTime'] = $record['dynamodb']['ApproximateCreationDateTime']->getTimestamp();
            $records[] = $record + ['eventSourceARN' => $streamArn];
        }

        return new DynamoDbEvent(['Records' => $records]);
    }

    private function firstIterator(string $streamArn): string
    {
        $shardId = $this->streams->describeStream(['StreamArn' => $streamArn])['StreamDescription']['Shards'][0]['ShardId'];

        return $this->streams->getShardIterator([
            'StreamArn' => $streamArn,
            'ShardId' => $shardId,
            'ShardIteratorType' => 'TRIM_HORIZON',
        ])['ShardIterator'];
    }
}
```

It reads only the first shard. A Dynoxide stream has one, while a stream on
AWS can have many, so this is for Dynoxide only.

The test makes three changes to an order and checks the handler recorded each
one:

```php
<?php

namespace Tests;

use App\OrderEventsHandler;
use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDbStreams\DynamoDbStreamsClient;
use Bref\Context\Context;
use PHPUnit\Framework\TestCase;

final class OrderEventsHandlerTest extends TestCase
{
    public function test_it_records_every_change_to_an_order(): void
    {
        $dynamoDb = new DynamoDbClient([]);
        $streams = new DynamoDbStreamsClient(['endpoint' => getenv('AWS_ENDPOINT_URL_DYNAMODB')]);
        $events = new StreamEvents($dynamoDb, $streams);
        $handler = new OrderEventsHandler($dynamoDb);

        $key = ['id' => ['S' => 'order-1']];
        $dynamoDb->putItem(['TableName' => 'orders', 'Item' => $key + ['total' => ['N' => '42']]]);
        $dynamoDb->updateItem([
            'TableName' => 'orders',
            'Key' => $key,
            'UpdateExpression' => 'SET #total = :total',
            'ExpressionAttributeNames' => ['#total' => 'total'],
            'ExpressionAttributeValues' => [':total' => ['N' => '50']],
        ]);
        $dynamoDb->deleteItem(['TableName' => 'orders', 'Key' => $key]);

        $handler->handleDynamoDb($events->next('orders'), Context::fake());

        $recorded = $dynamoDb->query([
            'TableName' => 'order_events',
            'KeyConditionExpression' => 'order_id = :id',
            'ExpressionAttributeValues' => [':id' => ['S' => 'order-1']],
        ])['Items'];

        $this->assertSame(
            [['INSERT', '42'], ['MODIFY', '50'], ['REMOVE', '50']],
            array_map(fn (array $item) => [$item['event']['S'], $item['total']['N']], $recorded),
        );
    }
}
```

It runs with the PHP package's PHPUnit extension, which starts Dynoxide and
creates the tables from the schema above, saved as `tests/schema.json`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<phpunit bootstrap="vendor/autoload.php">
    <testsuites>
        <testsuite name="default">
            <directory>tests</directory>
        </testsuite>
    </testsuites>
    <extensions>
        <bootstrap class="Dynoxide\PHPUnit\Extension">
            <parameter name="schema" value="tests/schema.json"/>
        </bootstrap>
    </extensions>
</phpunit>
```

The extension sets `AWS_ENDPOINT_URL_DYNAMODB` but not the Streams variable,
so the test gives the Streams client its endpoint. Without it, the client
goes to AWS.

### In local development

To watch the handler react to writes made through the app, this script polls
the stream and passes each batch to the handler through `bref-local`:

```php
<?php

// Reads a table's stream and hands each batch of records to a handler
// through bref-local, the way Lambda would. For local development only.
// Usage: php stream-poll.php <table> <handler.php>

require __DIR__ . '/vendor/autoload.php';

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDbStreams\DynamoDbStreamsClient;

[, $table, $handler] = $argv;

$dynamoDb = new DynamoDbClient([]);
$streams = new DynamoDbStreamsClient([]);

$streamArn = $dynamoDb->describeTable(['TableName' => $table])['Table']['LatestStreamArn'];
$shardId = $streams->describeStream(['StreamArn' => $streamArn])['StreamDescription']['Shards'][0]['ShardId'];
$iterator = $streams->getShardIterator([
    'StreamArn' => $streamArn,
    'ShardId' => $shardId,
    'ShardIteratorType' => 'LATEST',
])['ShardIterator'];

echo "Watching $table\n";

while ($iterator !== null) {
    $result = $streams->getRecords(['ShardIterator' => $iterator]);
    $iterator = $result['NextShardIterator'] ?? null;

    if ($result['Records'] === []) {
        sleep(1);
        continue;
    }

    $records = [];
    foreach ($result['Records'] as $record) {
        $record['dynamodb']['ApproximateCreationDateTime'] = $record['dynamodb']['ApproximateCreationDateTime']->getTimestamp();
        $records[] = $record + ['eventSourceARN' => $streamArn];
    }

    $event = tempnam(sys_get_temp_dir(), 'stream');
    file_put_contents($event, json_encode(['Records' => $records]));
    passthru('vendor/bin/bref-local --path ' . escapeshellarg($event) . ' ' . escapeshellarg($handler));
    unlink($event);
}
```

`bref-local` needs a file that returns the handler:

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use App\OrderEventsHandler;
use Aws\DynamoDb\DynamoDbClient;

return new OrderEventsHandler(new DynamoDbClient([]));
```

Run the script in the background, then write to `orders`:

```sh
docker compose run -d app php stream-poll.php orders order-events.php
docker compose run app vendor/bin/bref-local create-order.php '{"id":"order-7","total":42}'
```

Here the Streams client finds Dynoxide through
`AWS_ENDPOINT_URL_DYNAMODB_STREAMS`, which the Compose file sets. It starts
from `LATEST`, so only changes made after it starts reach the handler.

## Caveats

- **Dynoxide is for development and tests.** Deploy against DynamoDB.
- **`bref-ssm:` values call AWS.** Bref resolves them through SSM when the
  function starts, so locally give those variables plain values instead.
- **The SDK version.** `AWS_ENDPOINT_URL_DYNAMODB` needs `aws/aws-sdk-php`
  3.298.9 or later, as
  [Local development with Docker Compose](#local-development-with-docker-compose)
  says.
