# Dynoxide

A DynamoDB emulator backed by SQLite, verified against real AWS by a public conformance suite. A single static binary that starts in milliseconds, with no runtime or database to install.

## Install

```sh
npm install --save-dev dynoxide
```

Or run directly without installing:

```sh
npx dynoxide --port 8000
```

## Usage

Start an HTTP server:

```sh
dynoxide --port 8000
```

With a persistent database:

```sh
dynoxide --db-path data.db --port 8000
```

Then point any DynamoDB SDK at `http://localhost:8000`:

```sh
aws dynamodb list-tables --endpoint-url http://localhost:8000
```

## Starting a server from your tests

`start()` runs Dynoxide on a free port and returns its endpoint, region and credentials. The server stops when you call `stop()`, and also when the Node process exits, however it exits.

```js
import { start } from "dynoxide";
import { DynamoDBClient, ListTablesCommand } from "@aws-sdk/client-dynamodb";

const server = await start();
const client = new DynamoDBClient({
  endpoint: server.endpoint,
  region: server.region,
  credentials: server.credentials,
});

const { TableNames } = await client.send(new ListTablesCommand({}));
console.log(TableNames);

await server.stop();
```

The options are all optional:

| Option | |
|---|---|
| `dbPath` | A database file that survives a restart. Give each server its own file. Without it, the database lives in memory. |
| `schema` | A schema file whose tables exist before the server is ready. |
| `binary` | A Dynoxide binary to use instead of the one npm installed, 1.7.0 or later. `DYNOXIDE_BINARY` does the same. |
| `readyTimeoutMs` | How long to wait for the server to be ready. Defaults to 10000. |

`stop()` closes the server's stdin, which shuts it down cleanly, and kills it if it's still running after 5 seconds (on Linux and macOS it gets a SIGTERM and another 5 seconds first). Calling it again returns the same promise. Where the runtime has `Symbol.asyncDispose`, `await using server = await start()` stops the server at the end of the block.

If the server can't start, `start()` rejects with an error that includes the last lines of the server's stderr. `start()` is tested on Node 18 and later.

### With Vitest

Start one server for the whole run in a `globalSetup` file (Vitest 3 or later), and pass its endpoint to the tests:

```js
// vitest.global-setup.js
import { start } from "dynoxide";

export default async function setup(project) {
  const server = await start();
  project.provide("dynamoEndpoint", server.endpoint);
  return () => server.stop();
}
```

```js
// vitest.config.js
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: { globalSetup: "./vitest.global-setup.js" },
});
```

```js
// in a test file
import { inject } from "vitest";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const client = new DynamoDBClient({
  endpoint: inject("dynamoEndpoint"),
  region: "us-east-1",
  credentials: { accessKeyId: "dynoxide", secretAccessKey: "dynoxide" },
});
```

Every test file shares that one server, so give each file its own tables. In TypeScript, declare the value so `inject` knows its type:

```ts
declare module "vitest" {
  export interface ProvidedContext {
    dynamoEndpoint: string;
  }
}
```

## MCP Server

Dynoxide includes an [MCP](https://modelcontextprotocol.io) server for coding agents (Claude Code, Cursor, etc.):

```sh
dynoxide mcp
dynoxide mcp --db-path data.db
```

## Import

Load data from a [DynamoDB table export](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html) into a local Dynoxide instance:

```sh
dynoxide import --source ./export-data/ --schema schema.json --output data.db
```

## Supported Platforms

| Platform | Architecture |
|---|---|
| macOS | x64, arm64 (Apple Silicon) |
| Linux | x64, arm64 |
| Windows | x64 |

## How It Works

This package installs a platform-specific prebuilt binary via npm's `optionalDependencies`. No compilation, no Docker, no JVM.

The binary is the same one available via [Homebrew](https://github.com/nubo-db/homebrew-tap), [GitHub Releases](https://github.com/nubo-db/dynoxide/releases), and [crates.io](https://crates.io/crates/dynoxide-rs).

## Links

- [Full documentation and benchmarks](https://github.com/nubo-db/dynoxide)
- [Changelog](https://github.com/nubo-db/dynoxide/blob/main/CHANGELOG.md)
- [DynamoDB conformance results](https://github.com/paritysuite/dynamodb-conformance#results)

## Licence

MIT or Apache-2.0, at your option.
