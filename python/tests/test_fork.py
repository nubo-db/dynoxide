"""A forked child gets a copy of the server's stdin pipe. These check the
child closes it, so the parent can still stop the server by closing its own,
and that nothing the child does at exit stops the parent's server."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from conftest import FAKE

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="no fork on Windows")

PRELUDE = f"""
import json, os, sys, time, warnings
warnings.simplefilter("ignore", DeprecationWarning)  # fork() with threads running
from dynoxide._server import _launch
env = dict(os.environ, FAKE_MODE="ready")
server = _launch(binary={str(FAKE)!r}, command=[sys.executable], env=env, grace=5.0)
"""


def _run(body: str, tmp_path: Path) -> dict[str, object]:
    script = tmp_path / "scenario.py"
    script.write_text(PRELUDE + textwrap.dedent(body))
    marker = tmp_path / "marker"
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=60,
        env={**_clean_env(), "FAKE_MARKER": str(marker)},
    )
    assert result.returncode == 0, result.stderr
    outcome: dict[str, object] = json.loads(result.stdout.strip().splitlines()[-1])
    outcome["marker"] = marker.read_text() if marker.exists() else None
    return outcome


def _clean_env() -> dict[str, str]:
    import os

    return {k: v for k, v in os.environ.items() if k != "DYNOXIDE_BINARY"}


def test_parent_stops_promptly_while_a_forked_child_lives(tmp_path: Path) -> None:
    outcome = _run(
        """
        read, write = os.pipe()
        child = os.fork()
        if child == 0:
            os.close(read)
            os.write(write, json.dumps(server.stop()).encode())
            os.close(write)
            time.sleep(15)
            os._exit(0)
        os.close(write)
        from_child = json.loads(os.read(read, 100))
        began = time.monotonic()
        status = server.stop()
        took = time.monotonic() - began
        os.kill(child, 9)
        os.waitpid(child, 0)
        print(json.dumps({"child_stop": from_child, "status": status, "took": took}))
        """,
        tmp_path,
    )
    assert outcome["child_stop"] is None
    assert outcome["status"] == 0
    # Well inside the 5-second grace period: closing stdin was enough.
    assert isinstance(outcome["took"], float) and outcome["took"] < 2.0
    assert outcome["marker"] == "stdin"


def test_a_child_exiting_normally_leaves_the_server_running(tmp_path: Path) -> None:
    outcome = _run(
        """
        child = os.fork()
        if child == 0:
            sys.exit(0)  # runs atexit and the finaliser in the child
        os.waitpid(child, 0)
        time.sleep(0.3)
        running = server.is_running()
        server.stop()
        print(json.dumps({"running_after_child_exit": running}))
        """,
        tmp_path,
    )
    assert outcome["running_after_child_exit"] is True
