# Troubleshooting

Each entry starts from what you see: an error message, or a symptom when
there's no message. If you run Dynoxide through Testcontainers, its own
failures are covered in [Testcontainers](testcontainers.md#troubleshooting).

## `port 8000 is already in use (detected listener on 127.0.0.1:8000)`

Something is already listening on the port. Dynoxide checks before it binds,
and it checks the address you asked for and its counterpart (the wildcard
address for a loopback one, and loopback for the wildcard), so a Docker
container published on `0.0.0.0:8000` blocks `dynoxide --port 8000` on
`127.0.0.1` too. The address in brackets is where it found the listener.

Stop whatever holds the port, or pick another one. In a test suite, `--port 0`
has the operating system choose a free port, and Dynoxide prints the one it
got:

```sh
dynoxide --port 0
# Dynoxide listening on http://127.0.0.1:54321
```

## A client gets `UnknownOperationException`

There are three ways to get it.

- **A DynamoDB operation that only makes sense in the cloud**, such as
  `CreateBackup`, `CreateGlobalTable`, `ExportTableToPointInTime` or
  `PutResourcePolicy`. Dynoxide answers `400` with
  `{"__type":"com.amazon.coral.service#UnknownOperationException"}`. The
  [compatibility summary](compatibility-summary.md#not-implemented) lists them.
  If code you're testing calls one, stub that call.
- **A request without an `X-Amz-Target` header, or with one Dynoxide doesn't
  know.** Same answer. A hand-written HTTP call is the usual cause; the AWS
  SDKs always set it.
- **A request to a path other than `/`.** Dynoxide answers `404` with the body
  `<UnknownOperationException/>`. Check the endpoint you configured has no
  path on the end: `http://localhost:8000`, not `http://localhost:8000/dynamodb`.

## A client gets `MissingAuthenticationTokenException`

The client sent no credentials. Dynoxide never checks a signature, but it does
need the `Authorization` header to be there. Give the client any non-empty
access key and secret. [Testcontainers](testcontainers.md#a-client-with-no-credentials-gets-missingauthenticationtokenexception)
has the details and a Java example.

## `error: Internal error: unable to open database file: <path>`

Dynoxide couldn't create or open the file given to `--db-path`. Either the
directory doesn't exist (Dynoxide won't create it) or the user running
Dynoxide can't write to it.

In Docker this usually means running as a non-root user against the image's
own `/data`, which belongs to root. Mount a directory your user owns, as in
[Running as nonroot](installation.md#running-as-nonroot).

## The connection is refused or reset when Dynoxide runs in a container

Dynoxide binds to `127.0.0.1` unless told otherwise, and inside a container
that address can only be reached from inside the same container. The
published image's default command already passes `--host 0.0.0.0`, so this
only happens when you override the command, or run the binary in an image of
your own. Add `--host 0.0.0.0`:

```sh
docker run --rm -p 8000:8000 ghcr.io/nubo-db/dynoxide \
  serve --host 0.0.0.0 --port 8000 --log quiet
```

## Tables are gone after a restart

Without `--db-path`, Dynoxide keeps everything in memory, and the Docker image
does the same unless you give it a path. That suits test suites, which want a
clean start, but nothing survives the process. Pass `--db-path` to keep data
in a file, and in Docker put that file on a volume, as in
[Docker](installation.md#docker) or the Compose example there.

## A test passes against AWS and fails against Dynoxide, or the other way round

Check [Differences from DynamoDB](differences-from-dynamodb.md) first. If the
difference isn't listed there, it's a bug worth
[reporting](https://github.com/nubo-db/dynoxide/issues/new): include the
request, what AWS answered, and what Dynoxide answered.
