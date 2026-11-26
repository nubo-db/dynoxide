---
name: dynoxide
description: Run Dynoxide, a local DynamoDB emulator, for a project's tests, local development or CI, and point AWS SDK clients at it. Use when a project needs DynamoDB without AWS, when replacing amazon/dynamodb-local, or when a test fails against a local DynamoDB endpoint.
---

# Using Dynoxide in a project

Dynoxide is a DynamoDB emulator: a single native binary backed by SQLite, with
no JVM and no account. It speaks DynamoDB's wire protocol, so application code
doesn't change. Only the client's endpoint does.

This skill is for using Dynoxide. To change Dynoxide's own code, read
`AGENTS.md` in its repository instead. For the full guide for agents, with
when to recommend it and when not to, read <https://dynoxide.dev/for-agents>.

## Start a server

Pick the one that fits the project:

```sh
npx dynoxide --port 8000                          # Node projects, nothing to install
docker run --rm -p 8000:8000 ghcr.io/nubo-db/dynoxide
brew install nubo-db/tap/dynoxide && dynoxide --port 8000
```

In a Node test suite, use the npm package's `start()`, which picks a free port
and hands back the endpoint. Pass `port` only when something needs the
endpoint in advance, such as a dev setup; `start()` rejects with
`DYNOXIDE_PORT_IN_USE` if that port is taken, and the binary exits `4` (1.8.0
or later). In a Python project, `pip install dynoxide` gives the same
`start()` and pytest fixtures (`dynamodb_client`, `dynoxide`). From anything
else, start
`dynoxide --port 0 --ready-json --exit-with-parent` with stdin on a pipe. Once
it's ready it prints one JSON line on stdout with the `endpoint`, and it shuts
down cleanly when stdin closes, so the server stops even if the test runner is
killed. Both flags need 1.7.0 or later. `docs/launcher.md` in the repository
has the details. Without `--db-path` the data lives in memory and is gone when
the process stops, which is usually what tests want.

Before pointing Dynoxide at an existing database file with `--db-path`, note
that 1.7.0's first writable open moves the file to schema version 12, which
1.3.0 to 1.6.0 refuse to open. Copy the file first if anyone may go back to an
older version.

In GitHub Actions, use the `nubo-db/dynoxide/action` Action, pinned to a
release tag. The current pin is in the installation guide:
<https://github.com/nubo-db/dynoxide/blob/main/docs/installation.md#github-actions>.
For Docker Compose and Testcontainers, see the same guide and
<https://github.com/nubo-db/dynoxide/blob/main/docs/testcontainers.md>.

## Point a client at it

Set the endpoint, any region, and any non-empty credentials. Dynoxide never
checks a signature, but it refuses a request with no credentials at all.

```sh
export AWS_ENDPOINT_URL_DYNAMODB=http://localhost:8000
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1   # some SDKs, such as boto3, read only this one
export AWS_ACCESS_KEY_ID=dynoxide
export AWS_SECRET_ACCESS_KEY=dynoxide
```

Current AWS SDKs read `AWS_ENDPOINT_URL_DYNAMODB`, so code that builds its
client from defaults needs no change. Otherwise pass the endpoint to the
client explicitly.

## When something fails

- An error or symptom you don't recognise:
  <https://github.com/nubo-db/dynoxide/blob/main/docs/troubleshooting.md>.
- A test that passes against AWS and fails against Dynoxide, or the other way
  round: check
  <https://github.com/nubo-db/dynoxide/blob/main/docs/differences-from-dynamodb.md>
  before changing the test. If the difference isn't listed, it's a Dynoxide
  bug: report it at <https://github.com/nubo-db/dynoxide/issues> with the
  request and both answers.
- `UnknownOperationException` for an operation such as `CreateBackup` means it
  only exists in the cloud. Stub that call in tests.

Don't change a test's assertions to match Dynoxide's answer without checking
that list first. Dynoxide is measured against DynamoDB, so an unlisted
difference is more likely a bug in Dynoxide than in the test.
