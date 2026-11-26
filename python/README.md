# dynoxide for Python

[Dynoxide](https://dynoxide.dev) is a DynamoDB-compatible engine backed by SQLite, built for tests and local development. This package starts a Dynoxide server from Python or pytest. The binary comes in the wheel, so there's no Java, no Docker and nothing downloaded at run time.

```sh
pip install dynoxide
```

## Start a server

```python
import boto3
import dynoxide

with dynoxide.start() as server:
    client = boto3.client("dynamodb", **server.client_kwargs())
    client.create_table(
        TableName="Users",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    print(client.list_tables()["TableNames"])
```

`start()` runs the server on 127.0.0.1 on a free port and returns once it's ready for requests. Leaving the `with` block stops it. Without a `with`, call `server.stop()`. A server you forget to stop, or don't keep a reference to, runs until Python exits, and goes away then however Python exits, even with `kill -9`. That last part needs Dynoxide 1.7.0 or later: an older binary given through `binary=` or `DYNOXIDE_BINARY` is left running if Python is killed outright.

`client_kwargs()` holds the endpoint, the region (`us-east-1`) and a pair of dummy credentials (`dynoxide` / `dynoxide`). Dynoxide accepts any credentials, so nothing real is needed.

### Options

```python
dynoxide.start(
    db_path="data/dev.db",       # keep the data in a file between runs (default: in memory)
    schema="tests/schema.json",  # create tables at startup
    port=8000,                   # a fixed port, for a dev setup (default: any free port)
    ready_timeout=10.0,          # seconds to wait before giving up
    binary="/path/to/dynoxide",  # a different binary (see below)
    binary_from_path=False,      # True: use the dynoxide on PATH, from Homebrew say
    log=print,                   # where the one start-up line goes (default: the "dynoxide" logger)
)
```

Keep test suites on port 0, the default. If a fixed port is taken, `start()` raises `PortInUseError`, which has the port in `.port`.

If it can't start, `start()` raises `DynoxideError`. Its `code` says why (`BINARY_MISSING`, `UNSUPPORTED_PLATFORM`, `TOO_OLD`, `VERSION_CHECK_FAILED`, `SPAWN_FAILED`, `EXITED`, `PORT_IN_USE`, `BAD_READY_LINE`, `PORT_ZERO` or `TIMEOUT`), and the message ends with the server's last lines of stderr.

The server's output goes to temporary files, removed when it stops, so nothing in your process has to keep reading it. They stay small unless `RUST_LOG=trace` is set, which logs every request for as long as the server runs, at about 120 bytes each.

## Code that builds its own clients

If the code under test calls `boto3.client("dynamodb")` itself, export the environment instead:

```python
server = dynoxide.start()
server.export_env()
```

That sets `AWS_ENDPOINT_URL_DYNAMODB`, the dummy credentials, `AWS_REGION` and `AWS_DEFAULT_REGION`, sets `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=false`, and removes `AWS_SESSION_TOKEN`. It never sets `AWS_ENDPOINT_URL`, so other AWS services aren't sent to Dynoxide.

The variables apply to the whole process. A test that also talks to S3 picks up the dummy credentials as well, and endpoints configured for other services come back into use if `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` had switched them off. The credentials and endpoint variables take priority over an `AWS_PROFILE`, so a profile can't send requests elsewhere.

boto3 reads `AWS_ENDPOINT_URL_DYNAMODB` from 1.28.0 (botocore 1.31.0), and aiobotocore from 2.5.3. Older versions ignore it and call AWS, so pass `endpoint_url` yourself there.

## pytest

The plugin loads when the package is installed, and starts nothing until a test asks for one of its fixtures:

```python
def test_users(dynamodb_client):
    dynamodb_client.put_item(TableName="Users", Item={"pk": {"S": "alice"}})
    item = dynamodb_client.get_item(TableName="Users", Key={"pk": {"S": "alice"}})
    assert item["Item"]["pk"]["S"] == "alice"
```

| Fixture | Scope | What it gives |
|---|---|---|
| `dynoxide` | session | The `Server`, with its environment exported until the session ends |
| `dynamodb_client` | session | `boto3.client("dynamodb")` for it |
| `dynamodb_resource` | session | `boto3.resource("dynamodb")` for it |
| `dynoxide_clean` | test | Empties every table after the test, keeping the tables |

The client fixtures need boto3, which this package doesn't install for you.

Settings go in `pyproject.toml` (or `pytest.ini`), and each has a command-line option of the same name, such as `--dynoxide-schema`:

```toml
[tool.pytest.ini_options]
dynoxide_schema = "tests/schema.json"  # tables to create at startup
dynoxide_db_path = "tests/data.db"     # keep data between runs
dynoxide_binary = "/path/to/dynoxide"  # a different binary
```

Relative paths are relative to pytest's rootdir.

With pytest-xdist each worker starts its own server on its own port. Two servers can't share a database file, so with `dynoxide_db_path` set each worker gets its own, with the worker id appended: `tests/data.db-gw0`, `tests/data.db-gw1` and so on.

## Platforms

Wheels carry the binary for:

- Linux x86_64 and arm64, glibc or musl (Alpine)
- macOS on Intel (10.12 or later) and Apple silicon
- Windows x86_64

On anything else pip installs from source, without a binary. `start()` then raises `UNSUPPORTED_PLATFORM` until you point `DYNOXIDE_BINARY` at a dynoxide executable or pass `binary=`. Release binaries are on [GitHub](https://github.com/nubo-db/dynoxide/releases). A binary from `binary=`, `DYNOXIDE_BINARY` or `PATH` isn't checksum-verified, and it has to be Dynoxide 1.6.0 or later.

The `dynoxide` command runs the bundled binary, so `dynoxide serve --port 8000` works straight after `pip install`.

## Not for production

Dynoxide is for tests and local development. It binds to loopback, accepts any credentials and doesn't check request signatures.

The package's version matches the engine's: `dynoxide 1.8.0` on PyPI carries Dynoxide 1.8.0. A fix to the Python package alone ships as a post-release, such as `1.8.0.post1`.

Dual licensed under MIT or Apache-2.0.
