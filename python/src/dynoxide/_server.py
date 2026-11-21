"""Starting a Dynoxide server, knowing when it's ready, and stopping it."""

from __future__ import annotations

import contextlib
import http.client
import ipaddress
import json
import logging
import math
import os
import queue
import re
import subprocess
import threading
import time
import weakref
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import IO, Any

from . import _env
from ._binary import WINDOWS, Binary, checked, child_env, resolve
from ._errors import (
    BAD_READY_LINE,
    EXITED,
    PORT_ZERO,
    SPAWN_FAILED,
    TIMEOUT,
    TOO_OLD,
    DynoxideError,
    PortInUseError,
)

GRACE = 5.0
STDERR_LINES = 20
_LINE_LIMIT = 64 * 1024
_EXIT_PORT_IN_USE = 4
_LISTENING = re.compile(r"Dynoxide listening on (http://(\[[0-9a-fA-F:.]+\]|[^\s:/]+):(\d{1,5}))\s*$")

_logger = logging.getLogger("dynoxide")

# Every running server, so a forked child can close its copies of their stdin
# pipes. A process can't unregister a fork hook, so there's one for them all.
_live: weakref.WeakSet[_Process] = weakref.WeakSet()


def _after_fork_in_child() -> None:
    for process in list(_live):
        process.forget()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


@dataclass(frozen=True)
class _Ready:
    endpoint: str
    host: str
    port: int
    version: str | None


def start(
    *,
    binary: str | os.PathLike[str] | None = None,
    binary_from_path: bool = False,
    db_path: str | os.PathLike[str] | None = None,
    schema: str | os.PathLike[str] | None = None,
    port: int = 0,
    ready_timeout: float = 10.0,
    log: Callable[[str], None] | None = None,
) -> Server:
    """Starts a Dynoxide server and returns once it's ready for requests.

    The server listens on 127.0.0.1, on a free port unless ``port`` names one.
    It stops when you call ``stop()``, leave a ``with`` block, or when this
    process exits, however it exits.

    Raises DynoxideError (or PortInUseError, a subclass) if it can't start.
    """
    return _launch(
        binary=binary,
        binary_from_path=binary_from_path,
        db_path=db_path,
        schema=schema,
        port=port,
        ready_timeout=ready_timeout,
        log=log,
    )


def _launch(
    *,
    binary: str | os.PathLike[str] | None = None,
    binary_from_path: bool = False,
    db_path: str | os.PathLike[str] | None = None,
    schema: str | os.PathLike[str] | None = None,
    port: int = 0,
    ready_timeout: float = 10.0,
    log: Callable[[str], None] | None = None,
    command: Sequence[str] = (),
    env: Mapping[str, str] | None = None,
    grace: float = GRACE,
) -> Server:
    """start(), plus seams for the tests: a command prefix that runs a fake
    binary through Python, an environment, and a shorter grace period."""
    port = _port(port)
    ready_timeout = _timeout(ready_timeout)
    db = _path_arg(db_path, "db_path")
    schema_file = _path_arg(schema, "schema")
    if log is not None and not callable(log):
        raise TypeError("log must be a callable taking one string, or None.")
    say: Callable[[str], None] = log if log is not None else _logger.info

    base = dict(os.environ) if env is None else dict(env)
    found = checked(resolve(binary, binary_from_path, base), command, base)
    # Without the launcher flags, readiness is a listening line and then GET /
    # answering, and a stranger's server already on a fixed port would pass
    # the second check.
    if port != 0 and not found.launcher_flags:
        raise DynoxideError(
            TOO_OLD,
            f"A fixed port needs Dynoxide 1.7.0 or later, and {found.describe()} is older. "
            "Leave port at 0 to have the operating system pick one.",
        )

    args = [*command, found.path, "serve", "--port", str(port)]
    if found.launcher_flags:
        args += ["--ready-json", "--exit-with-parent"]
    # One argument each, so a path starting with "-" can't be read as a flag.
    if db is not None:
        args.append(f"--db-path={db}")
    if schema_file is not None:
        args.append(f"--schema={schema_file}")

    try:
        popen = subprocess.Popen(
            args,
            stdin=subprocess.PIPE if found.launcher_flags else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=child_env(base),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if WINDOWS else 0,
        )
    except (OSError, ValueError) as err:
        raise DynoxideError(SPAWN_FAILED, f"Couldn't run {found.describe()}: {err}.") from None

    process = _Process(popen, found.launcher_flags, grace)
    # Until the Server exists nobody else can stop it, so anything that goes
    # wrong first, the log callback included, stops it here.
    try:
        ready = process.wait_ready(found, ready_timeout, port)
        say(f"Dynoxide: {found.describe()}, listening on {ready.endpoint}")
        if not _is_loopback(ready.host):
            say(
                f"Dynoxide: warning, the server bound {ready.host}, which isn't a loopback address, "
                "so other machines may be able to reach it. Dynoxide accepts any credentials."
            )
    except BaseException:
        process.stop()
        raise
    return Server(ready, found, process)


class Server:
    """A running Dynoxide server.

    Use it as a context manager, or call ``stop()`` when you're done. A server
    nobody stops is stopped when it's garbage-collected or the process exits.
    """

    region = _env.REGION
    access_key_id = _env.ACCESS_KEY_ID
    secret_access_key = _env.SECRET_ACCESS_KEY

    def __init__(self, ready: _Ready, found: Binary, process: _Process) -> None:
        self.endpoint = ready.endpoint
        self.host = ready.host
        self.port = ready.port
        self.version = ready.version or found.version
        self.binary = found.path
        self.binary_source = found.source
        self.pid = process.popen.pid
        self._process = process
        # Holds the process, never the Server, so the Server can be collected.
        self._finalizer = weakref.finalize(self, process.stop)

    def client_kwargs(self) -> dict[str, str]:
        """Arguments for ``boto3.client("dynamodb", **...)`` or ``resource(...)``."""
        return {
            "endpoint_url": self.endpoint,
            "region_name": self.region,
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
        }

    def environment(self) -> dict[str, str]:
        """The environment variables that point AWS SDKs at this server."""
        return _env.environment(self.endpoint)

    def export_env(self) -> None:
        """Sets those variables in ``os.environ``, and removes AWS_SESSION_TOKEN.

        They apply to every AWS client in the process, not just DynamoDB ones,
        so code that also talks to S3 picks up the dummy credentials too.
        """
        _env.export(self.endpoint)

    def is_running(self) -> bool:
        return self._process.popen.poll() is None

    def stop(self) -> int | None:
        """Stops the server and returns its exit status (negative for a signal).

        Safe to call more than once. In a process forked after start() it does
        nothing and returns None: the server belongs to the parent.
        """
        return self._process.stop()

    def __enter__(self) -> Server:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()

    def __repr__(self) -> str:
        state = "running" if self.is_running() else "stopped"
        return f"<dynoxide.Server {self.endpoint} pid={self.pid} {state}>"


class _Process:
    """One child process: its output readers, and the contract's stop
    procedure (close stdin, SIGTERM, kill), run once."""

    def __init__(self, popen: subprocess.Popen[bytes], launcher_flags: bool, grace: float) -> None:
        self.popen = popen
        self.launcher_flags = launcher_flags
        self.grace = grace
        self.owner = os.getpid()
        self.stderr_tail: deque[str] = deque(maxlen=STDERR_LINES)
        self.events: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._lock = threading.Lock()
        self._stopped = False
        assert popen.stdout is not None and popen.stderr is not None
        self._readers = [
            threading.Thread(target=self._read_stdout, args=(popen.stdout,), daemon=True, name="dynoxide-stdout"),
            threading.Thread(target=self._read_stderr, args=(popen.stderr,), daemon=True, name="dynoxide-stderr"),
        ]
        for reader in self._readers:
            reader.start()
        _live.add(self)

    # Reading output. Both pipes are read for the life of the process, so a
    # chatty server never fills one and stalls.

    def _read_stdout(self, stream: IO[bytes]) -> None:
        try:
            first = stream.readline(_LINE_LIMIT)
            # Only a whole line counts: a partial one means the server died
            # part-way through writing it.
            if first.endswith(b"\n"):
                self.events.put(("stdout", first.decode("utf-8", "replace").strip()))
                while stream.read(65536):
                    pass
        except (OSError, ValueError):
            pass
        self.events.put(("eof", "stdout"))

    def _read_stderr(self, stream: IO[bytes]) -> None:
        try:
            for raw in iter(lambda: stream.readline(_LINE_LIMIT), b""):
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line.strip():
                    self.stderr_tail.append(line)
                if not self.launcher_flags:
                    match = _LISTENING.search(line)
                    if match:
                        self.events.put(("listening", match))
        except (OSError, ValueError):
            pass
        self.events.put(("eof", "stderr"))

    # Starting.

    def wait_ready(self, found: Binary, timeout: float, port: int) -> _Ready:
        deadline = time.monotonic() + timeout
        listening: _Ready | None = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if listening is None and not self.launcher_flags:
                    what = f"didn't say it was listening within {timeout:g} seconds"
                elif listening is not None:
                    what = f"didn't answer GET {listening.endpoint}/ within {timeout:g} seconds"
                else:
                    what = f"didn't print its ready line within {timeout:g} seconds"
                raise self._failure(TIMEOUT, found, what)
            try:
                kind, value = self.events.get(timeout=min(remaining, 0.05))
            except queue.Empty:
                kind, value = "", None

            if kind == "stdout" and self.launcher_flags:
                return self._parse_ready(value, found)
            if kind == "listening" and listening is None:
                listening = _Ready(value.group(1), value.group(2).strip("[]"), int(value.group(3)), found.version)
            if listening is not None and _answers_get(listening.host, listening.port):
                return listening
            if (kind == "eof" and value == "stdout" and self.launcher_flags) or self.popen.poll() is not None:
                self._raise_exited(found, port, deadline)

    def _parse_ready(self, line: str, found: Binary) -> _Ready:
        try:
            ready = json.loads(line)
        except ValueError:
            ready = None
        if (
            not isinstance(ready, dict)
            or ready.get("event") != "ready"
            or not isinstance(ready.get("endpoint"), str)
            or not isinstance(ready.get("port"), int)
            or isinstance(ready.get("port"), bool)
        ):
            raise self._failure(BAD_READY_LINE, found, f"printed a first line that isn't a ready line: {line[:200]}")
        if ready["port"] == 0:
            raise self._failure(PORT_ZERO, found, "reported port 0 in its ready line, which can't be reached")
        host = ready.get("host")
        version = ready.get("version")
        return _Ready(
            ready["endpoint"],
            host if isinstance(host, str) else "127.0.0.1",
            ready["port"],
            version if isinstance(version, str) else None,
        )

    def _raise_exited(self, found: Binary, port: int, deadline: float) -> None:
        # Its stdout has closed, or it has exited. Give it a moment to do the
        # other, and the stderr reader time to catch up.
        try:
            status = self.popen.wait(timeout=max(0.0, min(self.grace, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            raise self._failure(BAD_READY_LINE, found, "closed its output without printing a ready line") from None
        self._readers[1].join(timeout=1.0)
        if status == _EXIT_PORT_IN_USE and port != 0:
            raise PortInUseError(
                port,
                f"Port {port} is already in use, so the Dynoxide server ({found.describe()}) couldn't start on it.",
                self.stderr_tail,
            )
        if status < 0:
            how = f"was killed by signal {-status}"
        elif status in (126, 127):
            how = (
                f"exited with status {status}, which usually means the system couldn't run it: "
                "check it's a Dynoxide executable for this platform"
            )
        else:
            how = f"exited with status {status}"
        raise self._failure(EXITED, found, f"{how} before it was ready")

    def _failure(self, code: str, found: Binary, what: str) -> DynoxideError:
        # The server is stopped first, so the stderr tail is complete.
        self.stop()
        tail = list(self.stderr_tail)
        quiet = "" if tail else " It wrote nothing to stderr."
        return DynoxideError(code, f"The Dynoxide server ({found.describe()}) {what}.{quiet}", tail)

    # Stopping.

    def stop(self) -> int | None:
        if os.getpid() != self.owner:
            return None
        with self._lock:
            if not self._stopped:
                self._stopped = True
                _live.discard(self)
                try:
                    self._stop()
                except BaseException:
                    # Interrupted part way, by Ctrl+C in a grace wait say.
                    # Don't leave the server running, and let a later stop()
                    # or the finaliser finish closing the pipes.
                    if self.popen.poll() is None:
                        with contextlib.suppress(OSError):
                            self.popen.kill()
                    self._stopped = False
                    _live.add(self)
                    raise
        return self.popen.returncode

    def _stop(self) -> None:
        popen = self.popen
        if popen.poll() is None and self.launcher_flags:
            self._close_stdin()
            self._wait(self.grace)
        if popen.poll() is None and not WINDOWS:
            popen.terminate()
            self._wait(self.grace)
        if popen.poll() is None:
            popen.kill()
            popen.wait()
        self._close_stdin()
        # Each pipe is closed once its reader has seen the end of it. A reader
        # still going means something the server started holds the pipe open;
        # closing it under the reader would race, so it's left to the reader.
        for reader, stream in zip(self._readers, (popen.stdout, popen.stderr), strict=True):
            if reader is not threading.current_thread():
                reader.join(timeout=1.0)
            if not reader.is_alive() and stream is not None:
                stream.close()

    def _wait(self, seconds: float) -> None:
        with contextlib.suppress(subprocess.TimeoutExpired):
            self.popen.wait(timeout=seconds)

    def _close_stdin(self) -> None:
        if self.popen.stdin is not None:
            with contextlib.suppress(OSError):
                self.popen.stdin.close()

    def forget(self) -> None:
        """In a forked child: close this copy of the stdin pipe, so the parent
        can still stop the server by closing its own."""
        self._close_stdin()


def _answers_get(host: str, port: int) -> bool:
    connection = http.client.HTTPConnection(host, port, timeout=0.5)
    try:
        connection.request("GET", "/")
        return connection.getresponse().status == 200
    except (OSError, http.client.HTTPException):
        return False
    finally:
        connection.close()


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False


def _port(port: object) -> int:
    if isinstance(port, bool) or not isinstance(port, int):
        raise TypeError(f"port must be an int, not {type(port).__name__}.")
    if not 0 <= port <= 65535:
        raise ValueError(f"port must be from 0 to 65535, not {port}.")
    return port


def _timeout(seconds: object) -> float:
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise TypeError(f"ready_timeout must be a number of seconds, not {type(seconds).__name__}.")
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError(f"ready_timeout must be a positive number of seconds, not {seconds}.")
    return float(seconds)


def _path_arg(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, (str, os.PathLike)):
        raise TypeError(f"{name} must be a path, not {type(value).__name__}.")
    path = os.fspath(value)
    if not isinstance(path, str) or not path:
        raise ValueError(f"{name} must be a non-empty path.")
    return path
