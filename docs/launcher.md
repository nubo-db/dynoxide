# Writing a launcher

**Status: draft, launcher contract 1. Needs Dynoxide 1.7.0 or later.**

This is for anyone wrapping Dynoxide for another language: an npm `start()`,
a Composer package for PHPUnit, a pytest fixture, a JVM test helper. Each of
those has to find a binary, start it, know when it's ready, hand back an
endpoint and stop it again, including when the test runner is killed. This
page says how, so every wrapper gets those details right in the same way and a
developer moving between languages sees the same behaviour.

It is a draft until a published wrapper implements it and the behaviour below
has been checked in practice. Until then the rules can still change, and
feedback is welcome as an issue on the repository.
`docs/versioning.md` covers what the engine itself promises.

## The short version

1. Find the binary: an explicit path, then `DYNOXIDE_BINARY`, then one your
   package manager installed, then the shared cache, then a verified download.
2. Spawn `dynoxide serve --port 0 --ready-json --exit-with-parent`, with an
   argument array and no shell, and stdin on a pipe you hold.
3. Keep reading stdout and stderr for as long as the server runs.
4. The first stdout line is the ready line. Parse it and return the endpoint,
   region and credentials.
5. To stop it, close stdin, wait, and kill it if it's still there.

The rest of this page is the detail behind each step.

## Finding the binary

Look in this order and use the first that exists:

1. a path the user passed to the wrapper
2. the `DYNOXIDE_BINARY` environment variable
3. a binary the language's package manager installed, where one exists. For
   npm that's the `@dynoxide/<platform>` optional dependency.
4. the shared cache (below)
5. a download into the shared cache

A `dynoxide` on `PATH`, from Homebrew say, isn't a step, because its version
may not be the one the wrapper expects. A wrapper may offer it to its users as
an explicit option.

Log the path you settled on and which step found it, once, when the server
starts, so a test log shows which binary ran.

### Which version

- A wrapper released in step with Dynoxide uses its own version.
- Any other wrapper pins a default version, kept current by something like
  Renovate or Dependabot, and lets its users override it.
- Never resolve "latest" when the wrapper runs. It makes builds
  unreproducible, and it needs a network call before every cache lookup.

### A binary of unknown version

A binary from step 1 or 2 could be any version, and it isn't checksum-verified
(say so in your docs). Run `<binary> --version` first. It prints
`dynoxide <version>` on one line. Compare only the `X.Y.Z` part: a
pre-release counts as its release. That holds for the flags this contract
needs: every 1.7.0 pre-release has `--ready-json` and `--exit-with-parent`, and
a flag the contract comes to depend on ships in the first pre-release of the
version that adds it.
If that's older than the version this
contract needs, the binary doesn't know `--ready-json` or `--exit-with-parent`.
Start it without both, wait for the stderr line `Dynoxide listening on
http://<host>:<port>` and then for `GET /` to answer `200`, and stop it with
the procedure for servers started without `--exit-with-parent`.

That fallback needs 1.6.0 or later. Earlier versions print the port you asked
for rather than the one they bound, so under `--port 0` the line says `:0`.
Refuse anything older with an error that names the version found and the
version needed.

## The cache

The cache directory is, in order of preference:

- `DYNOXIDE_CACHE_DIR`, if set
- `$XDG_CACHE_HOME/dynoxide`, if `XDG_CACHE_HOME` is set
- `~/.cache/dynoxide` on Linux and macOS
- `%LOCALAPPDATA%\dynoxide` on Windows

Inside it, the binary lives at:

```text
<cache>/<version>/<target>/dynoxide        (dynoxide.exe on Windows)
<cache>/<version>/<target>/dynoxide.sha256 (the binary's SHA-256)
```

`<version>` is the full version, including any pre-release part, so
`1.7.0-rc.1` never shares a directory with `1.7.0`. `dynoxide.sha256` holds the
SHA-256 of the binary itself (not of the archive it came in) as lowercase hex,
followed by a newline. Read it leniently, taking the first word in any case,
so an install another wrapper wrote as `<hash>  dynoxide` still counts.

The layout is fixed, so a PHP wrapper and a Python wrapper find each other's
installs and a machine downloads each version once. The target is in the path
because a home directory can be shared between architectures: an NFS home on
mixed hardware, or a CI cache restored on a different runner.

If the cache directory can't be created or written, fail with an error that
names the path and `DYNOXIDE_CACHE_DIR`. Don't quietly fall back to a
temporary directory.

On CI, an ephemeral runner starts with an empty cache and downloads on every
job. Caching `DYNOXIDE_CACHE_DIR`, keyed on the version and target, avoids
that.

### Keeping the cache to yourself

The cache holds executables, so another user who can write to it can choose
what your tests run.

- Create cache directories with mode `0700`.
- On Linux and macOS, check the `dynoxide` cache directory and everything
  below it before use: the current user owns it, and it isn't writable by group
  or other. Don't check the parents. `~/.cache` itself is group-writable under
  some default umasks, and refusing it would break ordinary setups.
- Refuse a directory that fails, with an error naming it, what's wrong and the
  fix: `chmod 700`, `chown`, or point `DYNOXIDE_CACHE_DIR` somewhere else. A
  container running as root against a cache volume owned by another user hits
  this, and the error should make the fix obvious.
- Create temporary files with random names, exclusively.
- Use an existing install only if the binary's SHA-256 matches its
  `dynoxide.sha256` and, when you embed one, the expected binary hash for that
  version and target. Another wrapper may have written the install, so
  `dynoxide.sha256` alone catches a damaged file, not a replaced one. Without
  embedded binary hashes, say that in your docs.
- On Windows, rely on `%LOCALAPPDATA%` being per-user.

### Installing when several processes race

ParaTest, pytest-xdist and parallel Gradle workers will all try to install the
same version at the same moment. Install like this, and nobody ever runs a
half-written binary:

1. Download to a temporary file in the cache directory.
2. Verify it.
3. Extract it into a temporary directory alongside.
4. Write `dynoxide.sha256` next to the binary.
5. Rename the directory into place.
6. If the rename fails because the target already exists, another process won.
   That's success: use theirs, after the checksum check above.

## Downloading and verifying

Build the URL from the version, with no API call (which also avoids GitHub's
rate limits):

```text
<base>/v<version>/dynoxide-<target>.tar.gz   (.zip for Windows)
<base>/v<version>/sha256sums.txt
```

The base is `https://github.com/nubo-db/dynoxide/releases/download`. The
targets are:

| Platform | Target |
|---|---|
| Linux x86_64 | `x86_64-unknown-linux-musl` |
| Linux arm64 | `aarch64-unknown-linux-musl` |
| macOS Intel | `x86_64-apple-darwin` |
| macOS Apple silicon | `aarch64-apple-darwin` |
| Windows x86_64 | `x86_64-pc-windows-msvc` (`.zip`) |

The Linux builds are static musl binaries, so they also run on glibc systems
and there's no libc choice to make.

Fetch `sha256sums.txt` from the same base and version as the archive, and
check the archive's SHA-256 against its line before unpacking anything. A
mismatch is an error, never a warning.

What that check catches is a corrupt or truncated download. It doesn't defend
against a compromised release, because the checksums ship in the same release.
A wrapper released in step with Dynoxide can do better by publishing the
expected checksums in its own package and checking against those.

### Mirrors

`DYNOXIDE_DOWNLOAD_BASE_URL` replaces the base, for networks that can't reach
GitHub. It must be `https`; plain `http` is allowed only for a loopback host.
A mirror serves both the archive and its checksum, so it is trusted exactly as
much as the binary it serves. Document that for your users.

### Extracting

Each archive holds one file, `dynoxide` or `dynoxide.exe`. Extract that member
by exact name and nothing else, and fail if the archive has any other entry, a
path with a separator or `..`, an absolute path, or a symlink or hardlink. Set
the executable bit yourself (`0755`) rather than trusting the archive.

## Starting the server

Spawn the native binary with an argument array:

```text
dynoxide serve --port 0 --ready-json --exit-with-parent
```

Add `--db-path <file>` for a database that survives a restart, and
`--schema <file>` to create tables at startup. Everything else stays at its
default.

Give each server its own `--db-path`. Two servers on one file aren't
supported: if they start together, one can fail with `database is locked`, and
if both start, a table one of them deletes shows up in the other as an internal
error. Parallel test workers each need a file of their own, or no `--db-path`
at all.

- **No shell.** Spawning through `sh -c`, `cmd /c` or PHP's string form of
  `proc_open` leaves your process handle pointing at the shell, so stopping it
  can leave Dynoxide running.
- **No package-manager shim.** Spawn the native binary, never the npm
  `dynoxide` launcher script, which manages its own child and would race the
  stdin shutdown.
- **Loopback only.** Don't pass `--host`, so the server binds to `127.0.0.1`.
  Dynoxide accepts any credentials, so a wider bind is an unauthenticated
  database on the network. If a wrapper offers a way to bind wider, make it an
  explicit option that says so, and warn if the ready line's `host` isn't a
  loopback address.
- **One server per caller.** `--port 0` lets the operating system pick a free
  port, so parallel test processes never collide. Don't reuse a server you
  find already listening on a fixed port: you can't tell it's yours, or even
  that it's Dynoxide.
- **Check the binary exists first.** Some runtimes report a successful spawn
  for a missing file and let the child exit `127` (PHP 8.1 and 8.2 do). Check
  the file before spawning, and treat an exit of `126` or `127` before the
  ready line as "couldn't run it", not as a crash.
- **Hold on to the process.** Keep the process handle and your end of the
  stdin pipe referenced for as long as the server should run, somewhere that
  outlives the call that started it. A runtime that closes pipes when their
  object is garbage-collected (PHP does) will otherwise close stdin, and with
  `--exit-with-parent` the server stops.

### Keep reading its output

For as long as the server runs, keep reading its stdout and stderr on a
background reader, or send both to files. If nobody reads a pipe, it fills,
the server's next write blocks and requests stall with no error.

A single-threaded runtime such as PHP can't read in the background while the
tests run, so it sends both streams to files and polls the stdout file for the
ready line. Keep the last few lines of stderr either way: they belong in any
startup error.

## Knowing it's ready

With `--ready-json`, the first line on stdout is a single JSON object, printed
once the server is listening:

```json
{"event":"ready","schema":1,"version":"<version>","endpoint":"http://127.0.0.1:54321","host":"127.0.0.1","port":54321,"pid":4242}
```

| Field | Meaning |
|---|---|
| `event` | Always `"ready"` |
| `schema` | The shape of this line. `1` today. |
| `version` | Dynoxide's product version |
| `endpoint` | The URL to give an SDK |
| `host` | The address the server bound |
| `port` | The port it bound, never `0` |
| `pid` | The server's process ID, for logs only |

Read the fields you need and ignore the rest. New fields can arrive in any
minor release, and anything that removes or changes a field bumps `schema`.
Parse the line as JSON rather than matching its text: field order isn't part
of the contract.

The line means requests will be served: the database is open and any
`--schema` tables exist before it's printed, so send the first request
straight away. With the flag, stdout carries nothing but this line. Logging,
however `RUST_LOG` is set, goes to stderr. Anything else on stdout is a bug to
report.

The line covers the DynamoDB listener only. The arguments above never include
`--mcp`; if you add it, the MCP listener binds after the line is printed, and a
failure to bind it shows up later rather than as a startup error.

With `--exit-with-parent`, a server whose stdin reaches end-of-file at once
(already closed, `/dev/null`, or a file) prints the ready line and then shuts
down straight away, exiting `0`. If the process exits soon after a ready line, check that its stdin is a
pipe you're still holding open.

Treat as errors, never warnings:

- no ready line within the timeout, which defaults to 10 seconds and should be
  configurable
- the process exiting before the ready line
- a line that doesn't parse, or has `port` 0

Include the last lines of stderr in the error. A server that failed to start
should fail the test run, not show up later as a pile of confusing test
failures.

## What start returns

| Value | |
|---|---|
| endpoint | `endpoint` from the ready line |
| host, port | from the ready line |
| region | `us-east-1` |
| access key ID | `dynoxide` |
| secret access key | `dynoxide` |
| stop | a handle that stops the server |

Dynoxide checks that the authorisation header is shaped correctly and doesn't
check the signature, so any non-empty credentials work. Every wrapper returns
the same pair so examples carry across languages.

## Exporting to the environment

Offer a separate call that exports the values to the environment, and don't do
it as part of starting. Credentials in `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` apply to every AWS client in the process, so a test
that also talks to S3 or LocalStack would quietly pick up the dummy pair.

The export call sets:

- `AWS_ENDPOINT_URL_DYNAMODB` to the endpoint
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to `dynoxide`
- `AWS_REGION` and `AWS_DEFAULT_REGION` to `us-east-1`. Some SDKs read one and
  some the other, so set both.

It removes `AWS_SESSION_TOKEN`. Left in place, a real session token from CI
would be sent with the dummy pair, to Dynoxide and to any other AWS client in
the process.

Set them everywhere your runtime reads the environment from. In PHP that's
`putenv()`, `$_ENV` and `$_SERVER`, because PHPUnit builds some child
processes' environments from `$_SERVER`.

It never sets `AWS_ENDPOINT_URL`, which would send every AWS service to
Dynoxide. It may set framework variables too, such as Laravel's
`DYNAMODB_ENDPOINT`, as long as it documents which.

Older SDKs ignore `AWS_ENDPOINT_URL_DYNAMODB`. These are the first versions
that read it:

| Language | Package | First version |
|---|---|---|
| JavaScript | `@aws-sdk/client-dynamodb` | 3.428.0 |
| PHP | `aws/aws-sdk-php` | 3.298.9 |
| Python | `boto3` / `botocore` | 1.28.0 / 1.31.0 |
| Java | AWS SDK for Java 2.x | 2.28.1 |
| Go | `github.com/aws/aws-sdk-go-v2/config` | 1.21.0 |
| .NET | `AWSSDK.Core` | 3.7.108.0 |
| Ruby | `aws-sdk-core` | 3.178.0 |
| Rust | `aws-config` | 1.1.10 |

PHP's SDK reads the variable from 3.281.0, but before 3.298.9 its requests
still go to AWS, with only the scheme taken from the variable. The client's
reported endpoint looks right, so check where a request actually lands.

The AWS SDK for Java 1.x, JavaScript v2 and Go v1 never read it. With those,
pass the endpoint to the client yourself.

## Stopping the server

Signal the server only through the process handle your spawn call returned.
Keep the first exit status you're given: some runtimes report a status only
once (PHP's `proc_get_status()` reports a signal death, and on 8.1 and 8.2 any
exit, on one call only).
Never use the `pid` from the ready line: process IDs get reused, and a stale
one can belong to something else.

Started with `--exit-with-parent`:

1. Close the server's stdin. It shuts down cleanly and exits `0`, on Windows
   too.
2. Wait up to 5 seconds.
3. On Linux and macOS, send SIGTERM and wait up to another 5 seconds. A
   shutdown that closing stdin has already started, and that is waiting on a
   request that never finishes, won't be hurried by SIGTERM. That's what step
   4 is for.
4. Kill it.

Started without it, for a binary older than this contract:

- On Linux and macOS: SIGTERM, wait up to 5 seconds, then kill.
- On Windows: kill. There's no graceful option there. A `--db-path` file
  keeps every write that completed.

## When the test runner dies

If the runner is killed outright, none of its shutdown hooks run. With
`--exit-with-parent`, that's fine: the operating system closes the runner's
end of the stdin pipe, the server sees end-of-file and shuts down cleanly.
That works on every platform, which is why the contract uses stdin rather than
watching the parent's process ID.

Two things break it.

**Stdin not on a pipe.** With stdin closed or at `/dev/null`, the server sees
end-of-file straight away and stops as soon as it starts. Only pass
`--exit-with-parent` with stdin on a pipe you hold open.

**Another process holding the pipe.** End-of-file arrives only when every copy
of the pipe's write end is closed. If another process holds a copy, stopping
waits out the grace period and a dead runner leaves the server running until
that process exits too.

- Spawned children don't inherit the pipe by default in Node, Python, Java and
  PHP.
  Check your runtime's behaviour if it isn't one of those.
- Forking copies every descriptor: `os.fork`, `multiprocessing` with the fork
  start method, PHP's `pcntl_fork`. Where the runtime has a fork hook, register
  one that closes the write end in the child (in Python,
  `os.register_at_fork(after_in_child=...)`). Where it doesn't, document that
  forking after start delays stopping until the kill fallback.

## What a wrapper may change

Free to vary:

- the API's names and idioms
- extra framework environment variables
- an install-ahead-of-time command, for offline machines
- an explicit option to use a binary from `PATH` or elsewhere
- an explicit, documented option to bind beyond loopback

Fixed, so wrappers behave alike:

- the discovery order
- the cache location and layout
- verification and extraction
- the argument list
- the exported variable names
- the stop procedure

## Checklist

- [ ] Discovery follows the order above, and logs the path and source.
- [ ] The default version is pinned, never resolved at run time.
- [ ] A binary from a path or `DYNOXIDE_BINARY` gets a `--version` check, and
      an older one gets the fallback.
- [ ] The cache uses the shared location and layout, and fails loudly when it
      can't be written.
- [ ] Cache directories are `0700`, checked before use, and an existing
      install is re-verified against its `dynoxide.sha256` and any embedded
      binary hash.
- [ ] Concurrent installs go through a temporary directory and a rename.
- [ ] Downloads are checked against `sha256sums.txt` before unpacking.
- [ ] A mirror must be `https`, except on loopback.
- [ ] Extraction takes the one expected file and rejects anything else.
- [ ] The server is spawned with an argument array, no shell, no shim, and no
      `--host`.
- [ ] stdout and stderr are read for the life of the process.
- [ ] Readiness comes from the first stdout line, with a timeout that fails.
- [ ] Start returns the endpoint, region and the `dynoxide` credentials.
- [ ] Exporting is a separate call, sets both region variables and never
      `AWS_ENDPOINT_URL`.
- [ ] Stopping closes stdin, waits, then signals through the process handle.
- [ ] No other process holds the stdin pipe, forks included.

## Changes to this contract

A flag this contract depends on ships in the first pre-release of the engine
version that adds it, and a later pre-release of that version never adds one.
Wrappers compare only `X.Y.Z`, so a flag that arrived in `rc.2` would crash
them on `rc.1`.

- **Launcher contract 1** (Dynoxide 1.7.0): first draft. Corrected after the
  first wrapper, for PHP: what `dynoxide.sha256` holds, the version in the
  cache path, pre-releases in the version check, spawns that report success
  for a missing binary, holding the process, exit statuses reported once,
  where to export the environment, and PHP's first working SDK version.
