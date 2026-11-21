from __future__ import annotations

import gc
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path

import pytest
from conftest import FAKE

import dynoxide
from dynoxide import DynoxideError, PortInUseError, Server

POSIX = sys.platform != "win32"
StartFake = Callable[..., Server]


def _gone(pid: int, within: float = 5.0) -> bool:
    """Whether the process has exited, checked by connecting rather than by
    pid where we can't reap it ourselves."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return False


def _alive(pid: int) -> bool:
    if not POSIX:
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie still answers kill(0); ps knows better.
    state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout
    return bool(state.strip()) and not state.strip().startswith("Z")


# Fakes: the error paths.


def test_ready_line_gives_the_endpoint(start_fake: StartFake) -> None:
    server = start_fake()
    assert server.port > 0
    assert server.endpoint == f"http://127.0.0.1:{server.port}"
    assert server.binary_source == "option"
    assert server.version == "1.8.0"
    assert server.is_running()


def test_ready_line_with_unknown_fields_is_accepted(start_fake: StartFake) -> None:
    assert start_fake("extra-fields").port > 0


def test_the_arguments_are_the_contracts(start_fake: StartFake, tmp_path: Path) -> None:
    record = tmp_path / "record.json"
    start_fake(env={"FAKE_RECORD": str(record)}, db_path="-odd.db", schema=tmp_path / "s.json")
    args = json.loads(record.read_text())["args"]
    assert args == [
        "serve",
        "--port",
        "0",
        "--ready-json",
        "--exit-with-parent",
        "--db-path=-odd.db",
        f"--schema={tmp_path / 's.json'}",
    ]


def test_the_encryption_key_does_not_reach_the_server(start_fake: StartFake, tmp_path: Path) -> None:
    record = tmp_path / "record.json"
    start_fake(env={"FAKE_RECORD": str(record), "DYNOXIDE_ENCRYPTION_KEY": "secret"})
    assert json.loads(record.read_text())["keyed"] == []


def test_no_ready_line_times_out_and_stops_the_server(start_fake: StartFake, tmp_path: Path) -> None:
    record = tmp_path / "record.json"
    began = time.monotonic()
    with pytest.raises(DynoxideError) as caught:
        start_fake("never-ready", ready_timeout=1.0, env={"FAKE_PID_FILE": str(record)})
    assert caught.value.code == "TIMEOUT"
    assert time.monotonic() - began < 5
    assert "still thinking" in caught.value.stderr_tail
    assert "still thinking" in str(caught.value)
    assert _gone(int(record.read_text()))


def test_exit_before_ready(start_fake: StartFake) -> None:
    with pytest.raises(DynoxideError) as caught:
        start_fake("exit-3")
    assert caught.value.code == "EXITED"
    assert "status 3" in str(caught.value)
    assert caught.value.stderr_tail == ["something broke"]


def test_exit_127_says_it_could_not_be_run(start_fake: StartFake) -> None:
    with pytest.raises(DynoxideError) as caught:
        start_fake("exit-127")
    assert caught.value.code == "EXITED"
    assert "couldn't run it" in str(caught.value)


def test_partial_line_then_exit_is_an_exit(start_fake: StartFake) -> None:
    with pytest.raises(DynoxideError) as caught:
        start_fake("partial-then-exit")
    assert caught.value.code == "EXITED"


def test_port_in_use_on_a_fixed_port(start_fake: StartFake) -> None:
    with pytest.raises(PortInUseError) as caught:
        start_fake("port-in-use", port=8123)
    assert caught.value.code == "PORT_IN_USE"
    assert caught.value.port == 8123
    assert "8123" in str(caught.value)


def test_exit_4_with_port_0_is_just_an_exit(start_fake: StartFake) -> None:
    with pytest.raises(DynoxideError) as caught:
        start_fake("port-in-use")
    assert not isinstance(caught.value, PortInUseError)
    assert caught.value.code == "EXITED"


@pytest.mark.parametrize(("mode", "code"), [("garbage", "BAD_READY_LINE"), ("not-ready", "BAD_READY_LINE")])
def test_a_first_line_that_is_not_a_ready_line(start_fake: StartFake, mode: str, code: str) -> None:
    with pytest.raises(DynoxideError) as caught:
        start_fake(mode)
    assert caught.value.code == code


def test_port_zero_in_the_ready_line(start_fake: StartFake) -> None:
    with pytest.raises(DynoxideError) as caught:
        start_fake("port-zero")
    assert caught.value.code == "PORT_ZERO"


def test_legacy_binary_starts_through_the_stderr_line(start_fake: StartFake) -> None:
    server = start_fake("legacy", env={"FAKE_VERSION": "1.6.3"})
    assert server.version == "1.6.3"
    with urllib.request.urlopen(server.endpoint + "/") as response:
        assert response.status == 200
    server.stop()
    assert not server.is_running()


def test_legacy_binary_refuses_a_fixed_port(start_fake: StartFake) -> None:
    with pytest.raises(DynoxideError) as caught:
        start_fake("legacy", env={"FAKE_VERSION": "1.6.3"}, port=8123)
    assert caught.value.code == "TOO_OLD"


def test_stop_closes_stdin_and_the_server_exits(start_fake: StartFake, fake_env: dict[str, str]) -> None:
    server = start_fake()
    assert server.stop() == 0
    assert Path(fake_env["FAKE_MARKER"]).read_text() == "stdin"


def test_stop_twice_returns_the_same_status(start_fake: StartFake) -> None:
    server = start_fake()
    first = server.stop()
    assert server.stop() == first


@pytest.mark.skipif(not POSIX, reason="SIGTERM is a kill on Windows")
def test_a_server_ignoring_stdin_gets_sigterm(start_fake: StartFake, fake_env: dict[str, str]) -> None:
    server = start_fake("ignore-stdin", grace=0.3)
    server.stop()
    assert Path(fake_env["FAKE_MARKER"]).read_text() == "sigterm"


def test_a_server_ignoring_sigterm_is_killed(start_fake: StartFake) -> None:
    server = start_fake("ignore-term", grace=0.3)
    began = time.monotonic()
    status = server.stop()
    assert status is not None and status != 0
    assert time.monotonic() - began < 3
    assert not server.is_running()


def test_an_interrupted_stop_still_ends_the_server(start_fake: StartFake, monkeypatch: pytest.MonkeyPatch) -> None:
    server = start_fake("ignore-term", grace=5.0)
    process = server._process

    def interrupted(seconds: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(process, "_wait", interrupted)
    with pytest.raises(KeyboardInterrupt):
        server.stop()
    process.popen.wait(timeout=5)
    assert not server.is_running()
    # A later stop() finishes the job, which the fixture's teardown checks by
    # leaving no pipe open.
    monkeypatch.undo()
    assert server.stop() is not None


def test_a_chatty_server_never_stalls(start_fake: StartFake) -> None:
    server = start_fake("flood")
    deadline = time.monotonic() + 10
    while True:
        with urllib.request.urlopen(server.endpoint + "/", timeout=5) as response:
            flushed = json.load(response)
        # 4 MB of each, far more than a pipe holds, all written: nothing
        # stalled the server's writes.
        if flushed == {"out": 4 << 20, "err": 4 << 20} or time.monotonic() > deadline:
            break
        time.sleep(0.1)
    assert flushed == {"out": 4 << 20, "err": 4 << 20}
    assert server.stop() == 0


def test_the_output_files_go_when_the_server_stops(start_fake: StartFake) -> None:
    server = start_fake()
    folder = Path(server._process.output.dir)
    server.stop()
    assert not folder.exists()


def test_start_leaves_no_thread_running(start_fake: StartFake) -> None:
    # Python 3.12+ warns on fork() while threads run, which fails suites that
    # make warnings errors, so the launcher runs none.
    before = set(threading.enumerate())
    server = start_fake()
    assert set(threading.enumerate()) == before
    server.stop()


def test_context_manager_stops_on_exit_and_on_error(start_fake: StartFake) -> None:
    with start_fake() as server:
        assert server.is_running()
    assert not server.is_running()
    with pytest.raises(RuntimeError), start_fake() as other:
        raise RuntimeError("boom")
    assert not other.is_running()


def test_garbage_collected_server_is_stopped(start_fake: StartFake, fake_env: dict[str, str]) -> None:
    from dynoxide._server import _launch

    env = {**fake_env, "FAKE_MODE": "ready"}
    server = _launch(binary=str(FAKE), command=[sys.executable], env=env, grace=0.5)
    process = server._process
    del server
    gc.collect()
    assert process.popen.returncode is not None
    assert Path(fake_env["FAKE_MARKER"]).read_text() == "stdin"


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"port": -1}, ValueError),
        ({"port": 70000}, ValueError),
        ({"port": "8000"}, TypeError),
        ({"port": True}, TypeError),
        ({"ready_timeout": 0}, ValueError),
        ({"ready_timeout": float("nan")}, ValueError),
        ({"db_path": 5}, TypeError),
        ({"db_path": ""}, ValueError),
        ({"log": "print"}, TypeError),
    ],
)
def test_bad_arguments_raise_before_anything_starts(options: dict[str, object], error: type[Exception]) -> None:
    with pytest.raises(error):
        dynoxide.start(binary="/does/not/matter", **options)  # type: ignore[arg-type]


def test_log_gets_the_binary_and_endpoint(start_fake: StartFake) -> None:
    lines: list[str] = []
    server = start_fake(log=lines.append)
    assert len(lines) == 1
    assert str(FAKE) in lines[0] and "the binary argument" in lines[0] and server.endpoint in lines[0]


# The real binary.


def test_real_server_answers_boto3(real_binary: str) -> None:
    boto3 = pytest.importorskip("boto3")
    with dynoxide.start(binary=real_binary) as server:
        assert server.port > 0
        client = boto3.client("dynamodb", **server.client_kwargs())
        assert client.list_tables()["TableNames"] == []
    assert not server.is_running()


def test_real_server_persists_to_db_path(real_binary: str, tmp_path: Path) -> None:
    boto3 = pytest.importorskip("boto3")
    db = tmp_path / "data.db"
    with dynoxide.start(binary=real_binary, db_path=db) as server:
        client = boto3.client("dynamodb", **server.client_kwargs())
        client.create_table(
            TableName="kept",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
    with dynoxide.start(binary=real_binary, db_path=db) as server:
        client = boto3.client("dynamodb", **server.client_kwargs())
        assert client.list_tables()["TableNames"] == ["kept"]


def test_real_server_stops_cleanly(real_binary: str) -> None:
    server = dynoxide.start(binary=real_binary)
    assert server.stop() == 0


def test_real_server_on_a_taken_port(real_binary: str) -> None:
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        with pytest.raises(PortInUseError) as caught:
            dynoxide.start(binary=real_binary, port=port)
    assert caught.value.port == port


def test_real_server_on_a_fixed_free_port(real_binary: str) -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with dynoxide.start(binary=real_binary, port=port) as server:
        assert server.port == port


def test_real_server_outlives_nothing_when_the_parent_is_killed(real_binary: str, tmp_path: Path) -> None:
    script = tmp_path / "parent.py"
    script.write_text(
        "import sys, time, dynoxide\n"
        f"server = dynoxide.start(binary={real_binary!r})\n"
        "print(server.pid, flush=True)\n"
        "time.sleep(60)\n"
    )
    with subprocess.Popen([sys.executable, str(script)], stdout=subprocess.PIPE, text=True) as parent:
        assert parent.stdout is not None
        pid = int(parent.stdout.readline())
        assert _alive(pid)
        parent.kill()
    assert _gone(pid, within=10)
