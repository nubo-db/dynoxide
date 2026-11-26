from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

import dynoxide
from dynoxide import Server, _env

ENDPOINT = "http://127.0.0.1:4567"


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Puts every variable the export touches under monkeypatch, so each test's
    changes are undone."""
    for name in (*_env.NAMES, "AWS_ENDPOINT_URL", "AWS_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"):
        # Set first: deleting a variable that isn't there records nothing, so
        # one the test then sets would outlive it.
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)


def test_environment_lists_the_contracts_variables() -> None:
    assert _env.environment(ENDPOINT) == {
        "AWS_ENDPOINT_URL_DYNAMODB": ENDPOINT,
        "AWS_ACCESS_KEY_ID": "dynoxide",
        "AWS_SECRET_ACCESS_KEY": "dynoxide",
        "AWS_REGION": "us-east-1",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
    }


def test_export_sets_them_and_drops_the_session_token(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    monkeypatch.setenv("AWS_SESSION_TOKEN", "real-token")
    monkeypatch.setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true")
    _env.export(ENDPOINT)
    for name, value in _env.environment(ENDPOINT).items():
        assert os.environ[name] == value
    assert "AWS_SESSION_TOKEN" not in os.environ
    assert "AWS_ENDPOINT_URL" not in os.environ


def test_export_into_a_mapping() -> None:
    target = {"AWS_SESSION_TOKEN": "t", "OTHER": "kept"}
    _env.export(ENDPOINT, target)
    assert target["OTHER"] == "kept"
    assert "AWS_SESSION_TOKEN" not in target
    assert target["AWS_ENDPOINT_URL_DYNAMODB"] == ENDPOINT


def test_a_default_boto3_client_reaches_the_server_despite_a_profile(
    real_binary: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    boto3 = pytest.importorskip("boto3")
    config = tmp_path / "config"
    config.write_text(
        "[profile elsewhere]\n"
        "region = eu-west-2\n"
        "endpoint_url = http://127.0.0.1:9\n"
        "ignore_configured_endpoint_urls = true\n"
        "aws_access_key_id = AKIAREALLOOKING\n"
        "aws_secret_access_key = real-looking-secret\n"
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_PROFILE", "elsewhere")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "real-token")

    with dynoxide.start(binary=real_binary) as server:
        server.export_env()
        client = boto3.Session().client("dynamodb")
        assert client.meta.endpoint_url == server.endpoint
        assert client.list_tables()["TableNames"] == []


def test_server_environment_matches(start_fake: Callable[..., Server]) -> None:
    server = start_fake()
    assert server.environment() == _env.environment(server.endpoint)
    assert server.client_kwargs() == {
        "endpoint_url": server.endpoint,
        "region_name": "us-east-1",
        "aws_access_key_id": "dynoxide",
        "aws_secret_access_key": "dynoxide",
    }
