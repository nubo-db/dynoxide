"""The dynoxide command that pip puts on PATH."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest


def run(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "dynoxide", *args], capture_output=True, text=True, env=env, timeout=30
    )


def test_without_a_binary_it_says_so_and_exits_1() -> None:
    # The source tree has no bundled binary unless one was copied in.
    from dynoxide import DynoxideError, binary_path

    try:
        binary_path()
    except DynoxideError:
        pass
    else:
        pytest.skip("a binary is bundled in this install")
    result = run()
    assert result.returncode == 1
    assert result.stderr.startswith("dynoxide: ")
    assert "Traceback" not in result.stderr


def run_with(binary: str, *args: str) -> subprocess.CompletedProcess[str]:
    # Points binary_path() at the real binary, as a wheel's bundled copy would be.
    code = (
        "import sys, dynoxide.__main__ as m; "
        f"m.binary_path = lambda: {binary!r}; "
        "sys.argv = ['dynoxide', *sys.argv[1:]]; m.main()"
    )
    return subprocess.run(
        [sys.executable, "-c", code, *args], capture_output=True, text=True, env=dict(os.environ), timeout=30
    )


def test_it_runs_the_binary_with_the_arguments(real_binary: str) -> None:
    result = run_with(real_binary, "--version")
    expected = subprocess.run([real_binary, "--version"], capture_output=True, text=True).stdout
    assert result.returncode == 0
    assert result.stdout == expected


def test_the_binary_exit_status_comes_through(real_binary: str) -> None:
    result = run_with(real_binary, "--no-such-flag")
    direct = subprocess.run([real_binary, "--no-such-flag"], capture_output=True, text=True)
    assert direct.returncode != 0
    assert result.returncode == direct.returncode
