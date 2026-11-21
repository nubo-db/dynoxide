"""The errors start() raises."""

from __future__ import annotations

from collections.abc import Iterable

# Error codes, the same as the npm package's without its DYNOXIDE_ prefix.
BINARY_MISSING = "BINARY_MISSING"
UNSUPPORTED_PLATFORM = "UNSUPPORTED_PLATFORM"
TOO_OLD = "TOO_OLD"
VERSION_CHECK_FAILED = "VERSION_CHECK_FAILED"
SPAWN_FAILED = "SPAWN_FAILED"
EXITED = "EXITED"
PORT_IN_USE = "PORT_IN_USE"
BAD_READY_LINE = "BAD_READY_LINE"
PORT_ZERO = "PORT_ZERO"
TIMEOUT = "TIMEOUT"


class DynoxideError(Exception):
    """Dynoxide couldn't be found or started.

    ``code`` says which of the failures it was, and ``stderr_tail`` holds the
    server's last lines of stderr when there were any.
    """

    def __init__(self, code: str, message: str, stderr_tail: Iterable[str] = ()) -> None:
        self.code = code
        self.message = message
        self.stderr_tail = list(stderr_tail)
        if self.stderr_tail:
            message += "\n\nLast lines of stderr:\n" + "\n".join(self.stderr_tail)
        super().__init__(message)


class PortInUseError(DynoxideError):
    """The fixed port start() was given is already taken."""

    def __init__(self, port: int, message: str, stderr_tail: Iterable[str] = ()) -> None:
        self.port = port
        super().__init__(PORT_IN_USE, message, stderr_tail)
