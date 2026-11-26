from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from dynoxide import Server
from dynoxide._server import _launch

FIXTURES = Path(__file__).parent / "fixtures"
FAKE = FIXTURES / "fake_server.py"


def real_binary_path() -> str | None:
    """A real dynoxide binary for the tests that need one: DYNOXIDE_TEST_BINARY,
    or DYNOXIDE_BINARY as CI sets it."""
    for name in ("DYNOXIDE_TEST_BINARY", "DYNOXIDE_BINARY"):
        value = os.environ.get(name)
        if value:
            return value
    return None


@pytest.fixture
def real_binary() -> str:
    path = real_binary_path()
    if path is None:
        pytest.skip("needs a real dynoxide binary in DYNOXIDE_TEST_BINARY or DYNOXIDE_BINARY")
    return path


@pytest.fixture
def fake_env(tmp_path: Path) -> dict[str, str]:
    """The environment a fake server runs with, minus anything that would
    point it at a real binary."""
    env = {k: v for k, v in os.environ.items() if k not in ("DYNOXIDE_BINARY", "FAKE_MODE")}
    env["FAKE_MARKER"] = str(tmp_path / "marker")
    return env


@pytest.fixture
def start_fake(fake_env: dict[str, str]) -> Iterator[Callable[..., Server]]:
    """Starts the fake server in a mode, with a short grace period."""
    started: list[Server] = []

    def run(mode: str = "ready", grace: float = 0.5, **options: Any) -> Server:
        env = {**fake_env, "FAKE_MODE": mode, **options.pop("env", {})}
        server = _launch(binary=str(FAKE), command=[sys.executable], env=env, grace=grace, **options)
        started.append(server)
        return server

    yield run
    for server in started:
        server.stop()
