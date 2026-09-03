# Testcontainers

Dynoxide's image runs under Testcontainers in every language, but two things
have to be right first: the wait strategy, and the fact that no existing
DynamoDB module will drive it. Both have specific causes and short fixes.

If you arrived here from a timeout error, skip to
[Troubleshooting](#troubleshooting).

## The contract

| | |
|---|---|
| Image | `ghcr.io/nubo-db/dynoxide:1.1` |
| Port | `8000` |
| Wait on | `GET /` returning `200` |
| Command | the image default, `serve --host 0.0.0.0 --port 8000` |
| Region | `us-east-1` |
| Credentials | any non-empty pair. The header shape is checked, the signature is not |

`GET /` answering `200` is the readiness signal, and it means the engine will
serve, not just that the socket is open. `docs/versioning.md` names it as part
of the CLI and wire contract, so it will not move inside 1.x, and
`tests/container_contract.rs` holds it.

Pin to `:1.1` rather than `:1` for a test fixture. Both float, but `:1` picks up
conformance fixes, which change behaviour within the major on purpose. See
[installation](installation.md) for the full tag semantics.

## Recipes

### Java

```java
GenericContainer<?> dynoxide = new GenericContainer<>(
        DockerImageName.parse("ghcr.io/nubo-db/dynoxide:1.1"))
    .withExposedPorts(8000)
    .waitingFor(Wait.forHttp("/").forPort(8000).forStatusCode(200));

dynoxide.start();
String endpoint = "http://" + dynoxide.getHost() + ":" + dynoxide.getMappedPort(8000);
```

Setting `waitingFor` is not optional here. Without it the default strategy runs
and fails, for the reason in [Troubleshooting](#troubleshooting).

### Node

```ts
const dynoxide = await new GenericContainer("ghcr.io/nubo-db/dynoxide:1.1")
  .withExposedPorts(8000)
  .withWaitStrategy(Wait.forHttp("/", 8000).forStatusCode(200))
  .start();

const endpoint = `http://${dynoxide.getHost()}:${dynoxide.getMappedPort(8000)}`;
```

### Go

```go
ctr, err := testcontainers.Run(ctx, "ghcr.io/nubo-db/dynoxide:1.1",
    testcontainers.WithExposedPorts("8000/tcp"),
    testcontainers.WithWaitStrategy(
        wait.ForHTTP("/").
            WithPort("8000/tcp").
            WithStatusCodeMatcher(func(status int) bool { return status == 200 }),
    ),
)
```

Go's default port strategy already survives a shell-free image, so the HTTP wait
here is about correctness rather than rescue: a listening socket is not the same
as a ready engine.

### .NET

```csharp
var dynoxide = new ContainerBuilder()
    .WithImage("ghcr.io/nubo-db/dynoxide:1.1")
    .WithPortBinding(8000, true)
    .WithWaitStrategy(Wait.ForUnixContainer()
        .UntilHttpRequestIsSucceeded(r => r
            .ForPath("/").ForPort(8000).ForStatusCode(HttpStatusCode.OK)))
    .Build();

await dynoxide.StartAsync();
```

Note `HttpStatusCode.OK`, not `BadRequest`. See
[the module note](#an-existing-dynamodb-module-does-not-work-with-the-image-swapped).

### Other languages

Every Testcontainers implementation has an HTTP wait strategy. Point it at `/`
on port 8000 and expect `200`.

The container also carries a `HEALTHCHECK`, so a healthcheck-based wait
(`Wait.forHealthcheck()` in Java, `Wait.forHealthCheck()` in Node) works too and
needs no path or port. The container probes itself every 250ms until it first
reports healthy, then every 30 seconds, so a wait on it resolves in well under
a second. Needs Docker Engine 25.0 or newer.

## Troubleshooting

### `Timed out waiting for container port to open`

Java, using the default wait strategy. The full message names the host and the
port it expected to be listening.

The image is built `FROM scratch`, so it contains the binary and nothing else:
no shell, no busybox, no libc. Java's default `HostPortWaitStrategy` checks
liveness partly by running `/bin/sh` inside the container to read
`/proc/net/tcp`. That exec fails, the failure is wrapped in an
`IllegalStateException`, and what surfaces is a timeout that says nothing about
the real cause.

Set an explicit wait strategy, as in the [Java recipe](#java).

### `The HostPortWaitStrategy will not work on a distroless image`

Node, using the default wait strategy. The full line is `The
HostPortWaitStrategy will not work on a distroless image, use an alternate wait
strategy`, logged at error level, and the container then times out anyway.

Same cause as the Java case. Node's internal port check works out from exit
codes 126 and 127 that there is no shell, logs this, and returns false, which
the caller retries until it gives up.

The message says "distroless". The image is `FROM scratch`, which is emptier
still, but the fix is the same: set an explicit wait strategy, as in the
[Node recipe](#node).

### An existing DynamoDB module does not work with the image swapped

Pointing a DynamoDB module at `ghcr.io/nubo-db/dynoxide` fails, for a different
reason in each language:

- **Go** hardcodes the entrypoint `java -Djava.library.path=./DynamoDBLocal_lib`
  and the command `-jar DynamoDBLocal.jar`. Dynoxide has no JVM.
- **.NET** waits for `GET /` to return `400`, which is what DynamoDB Local
  answers. Dynoxide answers `200`, which is what real AWS answers, so it fails
  that module's health check for being closer to DynamoDB.
- **Rust** waits for the log line `Initializing DynamoDB Local with the
  following configuration`, which Dynoxide never prints.

Use a plain container with the wait strategy above instead.

### A client with no credentials gets `MissingAuthenticationTokenException`

Dynoxide checks that an `Authorization` header is present and well formed. It
never verifies the signature, but a request without the header is refused with
`400 MissingAuthenticationTokenException`.

An SDK client with no credentials configured sends no header and gets this. The
error names authentication, which sends people looking for a credentials
problem that is not there. Hand the client any non-empty pair:

```java
AwsBasicCredentials.create("dynoxide", "dynoxide")
```

### There is no `sharedDb`

DynamoDB Local partitions its data by access key and region, and its module
exposes a `sharedDb` flag to turn that off. Dynoxide does not partition, so
there is nothing to share and no flag to set. A table created under one set of
credentials is visible under any other.
