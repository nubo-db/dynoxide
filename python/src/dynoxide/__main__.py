"""The ``dynoxide`` command: runs the binary that came with this package."""

from __future__ import annotations

import os
import subprocess
import sys

from ._binary import WINDOWS, binary_path
from ._errors import DynoxideError


def main() -> None:
    try:
        path = binary_path()
    except DynoxideError as err:
        print(f"dynoxide: {err.message}", file=sys.stderr)
        sys.exit(1)
    args = [path, *sys.argv[1:]]
    if not WINDOWS:
        # Replaces this process, so signals and the exit status are the
        # binary's own.
        os.execv(path, args)
    # Windows has no exec that keeps the process, so run it and pass its
    # status on. Ctrl+C reaches the binary too, and it decides what to do.
    process = subprocess.Popen(args)
    while True:
        try:
            sys.exit(process.wait())
        except KeyboardInterrupt:
            continue


if __name__ == "__main__":
    main()
