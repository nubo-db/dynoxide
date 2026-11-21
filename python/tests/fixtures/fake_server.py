"""A stand-in for the dynoxide binary, run through Python so it works on
every platform.

FAKE_MODE picks the behaviour. FAKE_RECORD, if set, is a file that gets the
arguments and whether DYNOXIDE_ENCRYPTION_KEY reached it. FAKE_MARKER, if
set, is written with how the fake was stopped. FAKE_VERSION is what
--version reports.
"""

from __future__ import annotations

import http.server
import json
import os
import signal
import sys
import threading
import time

args = sys.argv[1:]
mode = os.environ.get("FAKE_MODE", "ready")
marker = os.environ.get("FAKE_MARKER")


def mark(how: str) -> None:
    if marker:
        with open(marker, "w") as f:
            f.write(how)


def forever() -> None:
    while True:
        time.sleep(1)


def out(text: str) -> None:
    sys.stdout.write(text)
    sys.stdout.flush()


def err(text: str) -> None:
    sys.stderr.write(text)
    sys.stderr.flush()


if os.environ.get("FAKE_PID_FILE") and args[:1] != ["--version"]:
    with open(os.environ["FAKE_PID_FILE"], "w") as f:
        f.write(str(os.getpid()))

if os.environ.get("FAKE_RECORD"):
    keyed = [k for k in os.environ if k.upper() == "DYNOXIDE_ENCRYPTION_KEY"]
    with open(os.environ["FAKE_RECORD"], "w") as f:
        json.dump({"args": args, "keyed": keyed}, f)

if args[:1] == ["--version"]:
    if mode == "version-hang":
        forever()
    if mode == "version-fail":
        err("broken\n")
        sys.exit(3)
    if mode == "version-garbage":
        out("something else\n")
        sys.exit(0)
    out(f"dynoxide {os.environ.get('FAKE_VERSION', '1.8.0')}\n")
    sys.exit(0)


def port_arg() -> int:
    return int(args[args.index("--port") + 1])


def ready(port: int, **extra: object) -> None:
    line = {
        "event": "ready",
        "schema": 1,
        "version": "1.8.0",
        "endpoint": f"http://127.0.0.1:{port}",
        "host": "127.0.0.1",
        "port": port,
        "pid": os.getpid(),
        **extra,
    }
    out(json.dumps(line) + "\n")


FLOOD = 4 << 20

# In flood mode, how many bytes of each stream the reader has taken. A reader
# that stops reading stops the count, as it would stop a real server's writes.
flushed = {"out": 0, "err": 0}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = json.dumps(flushed).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


if mode == "port-in-use":
    port = port_arg()
    err(f"error: port {port} is already in use (detected listener on 127.0.0.1:{port})\n")
    sys.exit(4)
if mode == "exit-3":
    err("something broke\n")
    sys.exit(3)
if mode == "exit-127":
    err("cannot execute\n")
    sys.exit(127)
if mode == "garbage":
    out("not json\n")
    forever()
if mode == "not-ready":
    out(json.dumps({"event": "starting"}) + "\n")
    forever()
if mode == "port-zero":
    ready(0)
    forever()
if mode == "partial-then-exit":
    out('{"event":"rea')
    sys.exit(1)
if mode == "never-ready":
    err("still thinking\n")
    forever()

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
port = server.server_address[1]


def on_term(signum: int, frame: object) -> None:
    if mode == "ignore-term":
        return
    mark("sigterm")
    os._exit(0)


if sys.platform != "win32":
    signal.signal(signal.SIGTERM, on_term)

if mode == "legacy":
    # A 1.6.x binary: no --ready-json, so it says it's listening on stderr.
    assert "--ready-json" not in args and "--exit-with-parent" not in args, args
    err(f"Dynoxide listening on http://127.0.0.1:{port}\n")
    forever()

if mode == "extra-fields":
    ready(port, shiny="new")
else:
    ready(port)

if mode == "flood":
    # Far more than a pipe holds, then stops, so a launcher that sent output
    # to files doesn't fill the disk.
    line = "x" * (16 * 1024 - 1) + "\n"

    def pump(stream: object, key: str) -> None:
        while flushed[key] < FLOOD:
            stream.write(line)  # type: ignore[attr-defined]
            stream.flush()  # type: ignore[attr-defined]
            flushed[key] += len(line)

    threading.Thread(target=pump, args=(sys.stdout, "out"), daemon=True).start()
    threading.Thread(target=pump, args=(sys.stderr, "err"), daemon=True).start()

if mode in ("ignore-stdin", "ignore-term"):
    forever()

# Until end of input, as --exit-with-parent does.
while sys.stdin.buffer.read(65536):
    pass
mark("stdin")
os._exit(0)
