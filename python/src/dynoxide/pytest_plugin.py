"""pytest fixtures for Dynoxide. pytest loads this on its own once the package
is installed, and it starts nothing until a test asks for a fixture.

- ``dynoxide``: the Server, one per test process (one per pytest-xdist
  worker), with its environment variables exported for the session.
- ``dynamodb_client`` and ``dynamodb_resource``: boto3 objects for it.
- ``dynoxide_clean``: empties every table after the test that uses it.

Settings, in pytest.ini or ``[tool.pytest.ini_options]``, each also a
command-line option: ``dynoxide_schema``, ``dynoxide_db_path`` and
``dynoxide_binary``. Relative paths are relative to the rootdir.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from . import _env
from ._server import Server, start

_SETTINGS = {
    "dynoxide_schema": "Schema file Dynoxide creates tables from at startup.",
    "dynoxide_db_path": (
        "Database file for Dynoxide to keep data in between runs. Under pytest-xdist each "
        "worker gets its own, with the worker id appended (-gw0)."
    ),
    "dynoxide_binary": "A dynoxide binary to run instead of the one this package installed.",
}


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("dynoxide")
    for name, help_text in _SETTINGS.items():
        parser.addini(name, help_text, default="")
        group.addoption(f"--{name.replace('_', '-')}", dest=name, default=None, help=help_text)


def _setting(config: pytest.Config, name: str) -> str | None:
    value = config.getoption(name) or config.getini(name)
    if not value:
        return None
    path = Path(str(value))
    return str(path if path.is_absolute() else config.rootpath / path)


def worker_db_path(db_path: str, worker: str | None = None) -> str:
    """The database file for this process: the one given, or under
    pytest-xdist that file with the worker id appended, since two servers
    can't share one file."""
    worker = os.environ.get("PYTEST_XDIST_WORKER") if worker is None else worker
    return f"{db_path}-{worker}" if worker else db_path


@pytest.fixture(scope="session")
def dynoxide(pytestconfig: pytest.Config) -> Iterator[Server]:
    """A Dynoxide server for this test session, with AWS_ENDPOINT_URL_DYNAMODB
    and the dummy credentials exported until the session ends."""
    db_path = _setting(pytestconfig, "dynoxide_db_path")
    server = start(
        binary=_setting(pytestconfig, "dynoxide_binary"),
        schema=_setting(pytestconfig, "dynoxide_schema"),
        db_path=worker_db_path(db_path) if db_path else None,
    )
    saved = {name: os.environ.get(name) for name in _env.NAMES}
    try:
        server.export_env()
        yield server
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        server.stop()


def _boto3() -> Any:
    try:
        import boto3  # type: ignore[import-untyped]
    except ImportError:
        pytest.fail(
            "The dynamodb_client and dynamodb_resource fixtures need boto3: pip install boto3",
            pytrace=False,
        )
    return boto3


@pytest.fixture(scope="session")
def dynamodb_client(dynoxide: Server) -> Any:
    """A boto3 DynamoDB client for the session's server."""
    return _boto3().client("dynamodb", **dynoxide.client_kwargs())


@pytest.fixture(scope="session")
def dynamodb_resource(dynoxide: Server) -> Any:
    """A boto3 DynamoDB resource for the session's server."""
    return _boto3().resource("dynamodb", **dynoxide.client_kwargs())


@pytest.fixture
def dynoxide_clean(dynamodb_client: Any) -> Iterator[None]:
    """Deletes every item from every table once the test finishes, keeping the
    tables themselves."""
    yield
    for page in dynamodb_client.get_paginator("list_tables").paginate():
        for table in page["TableNames"]:
            keys = [k["AttributeName"] for k in dynamodb_client.describe_table(TableName=table)["Table"]["KeySchema"]]
            projection = ", ".join(f"#k{i}" for i in range(len(keys)))
            names = {f"#k{i}": key for i, key in enumerate(keys)}
            scan = dynamodb_client.get_paginator("scan").paginate(
                TableName=table, ProjectionExpression=projection, ExpressionAttributeNames=names
            )
            for items in scan:
                batch = items["Items"]
                for start_at in range(0, len(batch), 25):
                    _delete_batch(dynamodb_client, table, batch[start_at : start_at + 25])


def _delete_batch(client: Any, table: str, items: list[dict[str, Any]]) -> None:
    requests: dict[str, Any] = {table: [{"DeleteRequest": {"Key": item}} for item in items]}
    while requests:
        requests = client.batch_write_item(RequestItems=requests).get("UnprocessedItems") or {}
