# Dynoxide

A fast, lightweight DynamoDB emulator backed by SQLite. Drop-in replacement for [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) and [dynalite](https://github.com/mhart/dynalite). No Docker, no JVM.

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
- [DynamoDB conformance results](https://github.com/nubo-db/dynamodb-conformance#results)

## Licence

MIT or Apache-2.0, at your option.
