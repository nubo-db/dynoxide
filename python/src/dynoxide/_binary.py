"""Finding the dynoxide binary, and checking the version of one we didn't ship."""

from __future__ import annotations

import os
import platform
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from ._errors import (
    BINARY_MISSING,
    TOO_OLD,
    UNSUPPORTED_PLATFORM,
    VERSION_CHECK_FAILED,
    DynoxideError,
)

WINDOWS = sys.platform == "win32"
EXE = "dynoxide.exe" if WINDOWS else "dynoxide"

# The first release with --ready-json and --exit-with-parent.
LAUNCHER_FLAGS_SINCE = (1, 7, 0)
# The oldest that can be started at all: earlier ones print the port asked for
# rather than the one bound, so --port 0 can't work.
OLDEST_SUPPORTED = (1, 6, 0)

VERSION_TIMEOUT = 10.0
_VERSION_LINE = re.compile(r"^dynoxide ((\d+)\.(\d+)\.(\d+)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?)\s*$")
# A console script that runs this package is small; the real binary is
# megabytes. Anything bigger isn't read.
_SHIM_MAX_BYTES = 1 << 20


@dataclass(frozen=True)
class Binary:
    """A binary to run, where it came from, and how to start it."""

    path: str
    source: str  # "option", "env", "path" or "package"
    version: str | None = None  # None for the bundled binary, which isn't asked
    launcher_flags: bool = True

    def describe(self) -> str:
        found = {
            "option": "the binary argument",
            "env": "DYNOXIDE_BINARY",
            "path": "PATH",
            "package": "this package",
        }[self.source]
        version = f" {self.version}" if self.version else ""
        return f"dynoxide{version} at {self.path}, from {found}"


def binary_path() -> str:
    """The binary that came with this package.

    Raises DynoxideError with code UNSUPPORTED_PLATFORM when there isn't one:
    pip installed the source distribution because no wheel matched.
    """
    bundled = Path(__file__).resolve().parent / "_bin" / EXE
    if not bundled.is_file():
        raise DynoxideError(
            UNSUPPORTED_PLATFORM,
            f"This install of the dynoxide package has no binary for {_platform_name()}: "
            "pip installed it from source because no wheel matches this platform. "
            "Set DYNOXIDE_BINARY to a dynoxide executable for this platform "
            "(https://github.com/nubo-db/dynoxide/releases), or pass binary= to start().",
        )
    return str(bundled)


def resolve(
    binary: str | os.PathLike[str] | None,
    binary_from_path: bool,
    env: Mapping[str, str],
) -> Binary:
    """Finds the binary in the launcher contract's order: the binary argument
    (or PATH, when asked for), then DYNOXIDE_BINARY, then the bundled one. A
    path that was asked for but isn't there is an error, not a reason to try
    the next step."""
    if binary is not None:
        return Binary(_existing(binary, "the binary argument"), "option")
    if binary_from_path:
        return Binary(_on_path(env), "path")
    from_env = env.get("DYNOXIDE_BINARY", "")
    if from_env:
        return Binary(_existing(from_env, "DYNOXIDE_BINARY"), "env")
    return Binary(binary_path(), "package")


def checked(found: Binary, command: Sequence[str] = (), env: Mapping[str, str] | None = None) -> Binary:
    """Asks a binary we didn't ship which version it is, and decides how to
    start it. `command` is a prefix for running it through an interpreter, which
    is how the tests run their fake servers."""
    if found.source == "package":
        return found
    version = _ask_version(found.path, command, env)
    numbers = _numbers(version)
    if numbers < OLDEST_SUPPORTED:
        raise DynoxideError(
            TOO_OLD,
            f"The dynoxide binary at {found.path} is {version}, and this package needs "
            f"{_dotted(OLDEST_SUPPORTED)} or later. Leave the binary unset to use the one this "
            "package installed.",
        )
    return Binary(found.path, found.source, version, numbers >= LAUNCHER_FLAGS_SINCE)


def parse_version(output: str) -> str | None:
    """The version from `dynoxide 1.7.0` or `dynoxide 1.7.0-rc.1`, or None."""
    first = output.lstrip().split("\n", 1)[0] if output.strip() else ""
    match = _VERSION_LINE.match(first)
    return match.group(1) if match else None


def child_env(env: Mapping[str, str]) -> dict[str, str]:
    """The environment without DYNOXIDE_ENCRYPTION_KEY: a test server never
    wants encryption, and a release binary refuses to start with it set.
    Windows names ignore case."""

    def dropped(name: str) -> bool:
        if WINDOWS:
            return name.upper() == "DYNOXIDE_ENCRYPTION_KEY"
        return name == "DYNOXIDE_ENCRYPTION_KEY"

    return {name: value for name, value in env.items() if not dropped(name)}


def _existing(file: str | os.PathLike[str], source: str) -> str:
    if not isinstance(file, (str, os.PathLike)):
        raise TypeError(f"{source} must be a path to a dynoxide executable, not {type(file).__name__}.")
    path = Path(file)
    absolute = path.absolute()
    if not absolute.is_file():
        name = os.fspath(file)
        bare = (
            " It takes a path, not a command name to look up on PATH (pass binary_from_path=True for that)."
            if os.sep not in name and "/" not in name
            else ""
        )
        raise DynoxideError(
            BINARY_MISSING,
            f"The dynoxide binary at {absolute}, from {source}, isn't a file.{bare} "
            "Point it at a dynoxide executable, or leave it unset to use the one this package installed.",
        )
    return str(absolute)


def _on_path(env: Mapping[str, str]) -> str:
    """The first dynoxide on PATH that isn't this package's own command, which
    would only start the bundled binary as its child."""
    exts = [""]
    if WINDOWS:
        exts = [e.lower() for e in env.get("PATHEXT", ".EXE").split(os.pathsep) if e]
    for folder in env.get("PATH", "").split(os.pathsep):
        if not folder:
            continue
        for ext in exts:
            candidate = Path(folder) / f"dynoxide{ext}"
            if candidate.is_file() and os.access(candidate, os.X_OK) and not _is_our_shim(candidate):
                return str(candidate.absolute())
    raise DynoxideError(
        BINARY_MISSING,
        "binary_from_path=True, but there's no dynoxide executable on PATH other than this "
        "package's own command. Install Dynoxide (for example with Homebrew), or leave "
        "binary_from_path unset to use the binary this package installed.",
    )


def _is_our_shim(candidate: Path) -> bool:
    try:
        if candidate.stat().st_size > _SHIM_MAX_BYTES:
            return False
        return b"dynoxide.__main__" in candidate.read_bytes()
    except OSError:
        return False


def _ask_version(path: str, command: Sequence[str], env: Mapping[str, str] | None) -> str:
    base = dict(os.environ) if env is None else dict(env)
    try:
        result = subprocess.run(
            [*command, path, "--version"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            env=child_env(base),
            timeout=VERSION_TIMEOUT,
            creationflags=_no_window(),
        )
    except subprocess.TimeoutExpired:
        raise DynoxideError(
            VERSION_CHECK_FAILED,
            f"{path} --version didn't finish within {VERSION_TIMEOUT:g} seconds. Is it a Dynoxide binary?",
        ) from None
    except OSError as err:
        raise DynoxideError(VERSION_CHECK_FAILED, f"Couldn't run {path}: {err}.") from None

    stdout = result.stdout.decode("utf-8", "replace")
    stderr = result.stderr.decode("utf-8", "replace").strip()
    if result.returncode != 0:
        how = (
            f"was killed by signal {-result.returncode}"
            if result.returncode < 0
            else f"failed with exit status {result.returncode}"
        )
        if result.returncode in (126, 127):
            how += ", which usually means it couldn't be executed at all"
        said = f" It said: {_shown(stderr)}" if stderr else ""
        raise DynoxideError(VERSION_CHECK_FAILED, f"{path} --version {how}.{said}")
    version = parse_version(stdout)
    if version is None:
        raise DynoxideError(
            VERSION_CHECK_FAILED,
            f'{path} --version printed {_shown(stdout)}, not "dynoxide <version>". Is it a Dynoxide binary?',
        )
    return version


def _numbers(version: str) -> tuple[int, int, int]:
    """X.Y.Z only, so a pre-release counts as its release."""
    major, minor, patch = re.split(r"[-+]", version, maxsplit=1)[0].split(".")
    return int(major), int(minor), int(patch)


def _dotted(version: tuple[int, int, int]) -> str:
    return ".".join(str(part) for part in version)


def _shown(text: str) -> str:
    text = re.sub(r"[^\x20-\x7e]+", " ", text.strip())
    if not text:
        return "nothing"
    return '"' + (text[:200] + "..." if len(text) > 200 else text) + '"'


def _platform_name() -> str:
    return f"{platform.system() or sys.platform} {platform.machine() or 'unknown'}"


def _no_window() -> int:
    return getattr(subprocess, "CREATE_NO_WINDOW", 0) if WINDOWS else 0
