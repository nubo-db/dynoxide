"""Starting a Dynoxide server, knowing when it's ready, and stopping it."""

from __future__ import annotations

import contextlib
import http.client
import ipaddress
import json
import logging
import math
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import weakref
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import BinaryIO

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
_POLL = 0.02
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
        output = _Output()
    except OSError as err:
        raise DynoxideError(
            SPAWN_FAILED, f"Couldn't create the files for the server's output in {tempfile.gettempdir()}: {err}."
        ) from None
    try:
        popen = subprocess.Popen(
            args,
            stdin=subprocess.PIPE if found.launcher_flags else subprocess.DEVNULL,
            stdout=output.child_stdout,
            stderr=output.child_stderr,
            env=child_env(base),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if WINDOWS else 0,
        )
    except BaseException as err:
        output.close()
        if isinstance(err, (OSError, ValueError)):
            raise DynoxideError(SPAWN_FAILED, f"Couldn't run {found.describe()}: {err}.") from None
        raise
    output.spawned()

    process = _Process(popen, output, found.launcher_flags, grace)
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


class _Output:
    """Where the server's stdout and stderr go: a file each, in a directory
    only this process can read. A file never fills the way a pipe does, so
    nothing has to keep reading it, and with no reader threads running a later
    fork() is clean.

    The child writes through its own handles and this process reads through
    separate ones, so reading never moves where the child writes. On POSIX
    the files are removed as soon as both sides have them open; Windows can't
    remove an open file, so there they go when the server has stopped.
    """

    def __init__(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="dynoxide-")
        out, err = os.path.join(self.dir, "stdout"), os.path.join(self.dir, "stderr")
        opened: list[BinaryIO] = []
        try:
            for path, append in ((out, True), (err, True), (out, False), (err, False)):
                opened.append(open(path, "ab" if append else "rb"))  # noqa: SIM115
        except BaseException:
            for handle in opened:
                handle.close()
            self._remove()
            raise
        self.child_stdout: BinaryIO | None = opened[0]
        self.child_stderr: BinaryIO | None = opened[1]
        self.stdout: BinaryIO = opened[2]
        self.stderr: BinaryIO = opened[3]
        self._closed = False
        if not WINDOWS:
            self._remove()

    def spawned(self) -> None:
        """The child has its copies of the write handles; close ours."""
        for handle in (self.child_stdout, self.child_stderr):
            if handle is not None:
                handle.close()
        self.child_stdout = self.child_stderr = None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.spawned()
        self.stdout.close()
        self.stderr.close()
        self._remove()

    def _remove(self) -> None:
        # On Windows a virus scanner or the indexer can hold a file that was
        # just closed for a moment, so try again briefly before giving up.
        for attempt in range(5):
            shutil.rmtree(self.dir, ignore_errors=True)
            if not os.path.exists(self.dir):
                return
            time.sleep(0.05 * (attempt + 1))


class _Process:
    """One child process: its output, and the contract's stop procedure
    (close stdin, SIGTERM, kill), run once."""

    def __init__(self, popen: subprocess.Popen[bytes], output: _Output, launcher_flags: bool, grace: float) -> None:
        self.popen = popen
        self.output = output
        self.launcher_flags = launcher_flags
        self.grace = grace
        self.owner = os.getpid()
        self._stdout = b""
        self._stderr = b""
        self._tail: list[str] = []
        self._lock = threading.Lock()
        self._stopped = False
        _live.add(self)

    # Reading output.

    def _ready_line(self) -> str | None:
        """The first stdout line, once it's whole. A partial one means the
        server is still writing it, or died part way through. One that runs
        past the limit with no end is returned as it stands, and fails as a
        bad ready line."""
        if len(self._stdout) < _LINE_LIMIT:
            self._stdout += self.output.stdout.read(_LINE_LIMIT - len(self._stdout))
        line, newline, _ = self._stdout.partition(b"\n")
        if not newline and len(self._stdout) < _LINE_LIMIT:
            return None
        return line.decode("utf-8", "replace").strip()

    def _new_stderr_lines(self) -> list[str]:
        self._stderr += self.output.stderr.read()
        *lines, self._stderr = self._stderr.split(b"\n")
        return [line.decode("utf-8", "replace").rstrip("\r") for line in lines]

    @property
    def stderr_tail(self) -> list[str]:
        """The last lines the server wrote to stderr, read from the end of the
        file so a long log costs no more than a short one."""
        if not self.output._closed:
            stream = self.output.stderr
            # Put back where the legacy start's scan has read to.
            position = stream.tell()
            end = stream.seek(0, os.SEEK_END)
            stream.seek(max(0, end - STDERR_LINES * 1024))
            text = stream.read().decode("utf-8", "replace")
            stream.seek(position)
            lines = [line.rstrip("\r") for line in text.split("\n") if line.strip()]
            # The first may be cut short, but a cut line beats none at all.
            if end > STDERR_LINES * 1024 and len(lines) > 1:
                lines = lines[1:]
            self._tail = lines[-STDERR_LINES:]
        return list(self._tail)

    # Starting.

    def wait_ready(self, found: Binary, timeout: float, port: int) -> _Ready:
        deadline = time.monotonic() + timeout
        listening: _Ready | None = None
        while True:
            # Checked before the output is read, so anything written just
            # before the exit still counts.
            exited = self.popen.poll() is not None
            if self.launcher_flags:
                line = self._ready_line()
                if line is not None:
                    return self._parse_ready(line, found)
            elif listening is None:
                for line in self._new_stderr_lines():
                    match = _LISTENING.search(line)
                    if match:
                        endpoint, host, bound = match.group(1), match.group(2).strip("[]"), int(match.group(3))
                        listening = _Ready(endpoint, host, bound, found.version)
                        break
            if listening is not None and _answers_get(listening.host, listening.port):
                return listening
            if exited:
                self._raise_exited(found, port)
            if time.monotonic() >= deadline:
                if listening is None and not self.launcher_flags:
                    what = f"didn't say it was listening within {timeout:g} seconds"
                elif listening is not None:
                    what = f"didn't answer GET {listening.endpoint}/ within {timeout:g} seconds"
                else:
                    what = f"didn't print its ready line within {timeout:g} seconds"
                raise self._failure(TIMEOUT, found, what)
            time.sleep(_POLL)

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

    def _raise_exited(self, found: Binary, port: int) -> None:
        status = self.popen.returncode
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
        tail = self.stderr_tail
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
                    # or the finaliser finish tidying up.
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
        # Read before the files go, so a failure can still report it.
        self._tail = self.stderr_tail
        self.output.close()

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
