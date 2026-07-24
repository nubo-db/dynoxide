# Installation

## npm

```sh
npm install --save-dev dynoxide
```

Or run directly without installing:

```sh
npx dynoxide --port 8000
```

## Homebrew (macOS and Linux)

```sh
brew install nubo-db/tap/dynoxide
```

Homebrew 6.0.0 added [tap trust](https://docs.brew.sh/Tap-Trust). The fully-qualified command above still installs fine (Homebrew trusts a named formula at install time), but the tap stays untrusted after that, so `brew upgrade` will skip Dynoxide and `brew doctor` will flag the tap. Trust it once:

```sh
# Trust just Dynoxide - lets brew upgrade see it again
brew trust --formula nubo-db/tap/dynoxide

# Or trust the whole tap - also clears the brew doctor warning
brew trust nubo-db/tap
```

## Pre-built binaries

Download from [GitHub Releases](https://github.com/nubo-db/dynoxide/releases) for Linux (x86_64, aarch64), macOS (Intel, Apple Silicon), and Windows.

```sh
# Example: Linux x86_64
curl -fsSL https://github.com/nubo-db/dynoxide/releases/latest/download/dynoxide-x86_64-unknown-linux-musl.tar.gz | tar xz
sudo mv dynoxide /usr/local/bin/
```

## Cargo

```sh
cargo install dynoxide-rs

# With encryption support (SQLCipher + vendored OpenSSL)
cargo install dynoxide-rs --no-default-features --features encrypted-full
```

## As a library (Rust)

```toml
[dependencies]
# Minimal - just the embedded database, no server or CLI dependencies
dynoxide-rs = { version = "0.12", default-features = false, features = ["native-sqlite"] }

# Or with encryption:
# dynoxide-rs = { version = "0.12", default-features = false, features = ["encryption"] }
```

## Upgrading to 0.12.x

Source-breaking for library consumers only. The DynamoDB wire API and the
CLI, server and MCP surfaces are unchanged, so anyone running the binary can
upgrade without reading further.

**Depending on the `dynoxide-rs` crate?** Two public types gained fields and
became `#[non_exhaustive]`:

- **`partiql::parser::Statement`** - the `Update` and `Delete` variants carry a
  `returning` field. Code that constructs or exhaustively matches them needs `..`.
- **`actions::batch_execute_statement::BatchStatementResponse`** - gained a
  `table_name` field, same treatment.

## Upgrading from 0.9.x

0.10.0 is a breaking release, but most of the breaks are library-only. The [CHANGELOG](../CHANGELOG.md) has the full list.

**Running the binary** (Homebrew, npm, the release archives, or the Docker image)? One change affects you:

- **MCP over HTTP now requires a bearer token.** Existing HTTP-transport clients break until they send an `Authorization: Bearer <token>` header. A loopback bind generates and persists a token on first run; a non-loopback bind will not start without one (`--mcp-token` or `DYNOXIDE_MCP_AUTH_TOKEN`). The stdio transport is unaffected, and plain `dynoxide serve` (DynamoDB only, no MCP) is unchanged. See [MCP Server](#mcp-server).

**Depending on the `dynoxide-rs` crate?** Also note:

- **`DynoxideError` is now `#[non_exhaustive]`.** Code that matches it exhaustively needs a `_ =>` arm.
- **`Database` is now generic, `Database<S>`.** The parameter defaults to the native backend, so code that names `Database` keeps compiling; a new `NativeDatabase` alias names that default explicitly.
- **Embedding the MCP HTTP server:** `dynoxide::mcp::serve_http` and `serve_http_with_shutdown` take an `HttpOptions` struct (bind host, auth mode, allowed hosts) in place of a bare port.

## GitHub Actions

```yaml
- uses: nubo-db/dynoxide/action@v0.12.0
  with:
    snapshot-url: https://example.com/test-data.db.zst  # optional
    port: 8000
```

See [action/action.yml](../action/action.yml) for all inputs and outputs.

## Docker

A 5 MB drop-in for `amazon/dynamodb-local` in containerised test suites. Same DynamoDB-compatible API, faster startup, smaller image. Note that this is a packaging convenience for test fixtures, not a containerised database product; production-database-on-Kubernetes patterns are out of scope.

```sh
docker run --rm -p 8000:8000 ghcr.io/nubo-db/dynoxide
```

With persistent storage:

```sh
docker run --rm -p 8000:8000 \
  -v "$(pwd)/data:/data" \
  ghcr.io/nubo-db/dynoxide \
  serve --host 0.0.0.0 --port 8000 --db-path /data/dynoxide.sqlite
```

The image runs as root by default, matching `amazon/dynamodb-local`, so bind mounts on Linux Just Work without `--user`. The canonical image lives at `ghcr.io/nubo-db/dynoxide`. Mirrors are pushed to `docker.io/nubodb/dynoxide` and `public.ecr.aws/h4s0n6a2/dynoxide` on a best-effort basis. SLSA provenance and SBOM attestations are published to GHCR only; if you want to verify provenance, pull from the GHCR canonical.

If you override `CMD` to bind to a different port, set the healthcheck target with environment variables so the container's `HEALTHCHECK` follows:

```sh
docker run -e DYNOXIDE_HEALTHCHECK_PORT=9000 ghcr.io/nubo-db/dynoxide serve --port 9000
```

`DYNOXIDE_HEALTHCHECK_HOST` and `DYNOXIDE_HEALTHCHECK_PORT` are documented public surface and will not be renamed in a patch or minor release.

### Running as nonroot

For security-conscious operators, opt into a nonroot uid:

```sh
docker run --rm -p 8000:8000 --user 65532:65532 ghcr.io/nubo-db/dynoxide
```

Persistent mode under nonroot needs a host-owned bind mount, since the in-image `/data` is owned by root:

```sh
docker run --rm -p 8000:8000 \
  --user "$(id -u):$(id -g)" \
  -v "$(pwd)/data:/data" \
  ghcr.io/nubo-db/dynoxide \
  serve --host 0.0.0.0 --port 8000 --db-path /data/dynoxide.sqlite
```

The default in-memory mode needs no flags whether root or nonroot. The uid 65532 is the well-known nonroot uid used by Google's distroless images; pick any uid you prefer with `--user <uid>:<gid>`.

### MCP over HTTP in Docker

The default image serves DynamoDB only. To also expose the [MCP](#mcp-server) Streamable HTTP transport, override the command to start it on `0.0.0.0` and supply a bearer token. The token is **mandatory** for any non-loopback bind. Pass it via the `DYNOXIDE_MCP_AUTH_TOKEN` environment variable (which keeps it out of shell history and `ps`), not a `--mcp-token` flag:

```sh
TOKEN=$(openssl rand -base64 24)

docker run --rm -p 8000:8000 -p 19280:19280 \
  -e DYNOXIDE_MCP_AUTH_TOKEN="$TOKEN" \
  ghcr.io/nubo-db/dynoxide \
  serve --host 0.0.0.0 --port 8000 \
        --mcp --mcp-host 0.0.0.0 --mcp-port 19280
```

DynamoDB is then reachable on `http://localhost:8000` and MCP on `http://localhost:19280/mcp`. Point an HTTP-transport MCP client at the latter with an `Authorization: Bearer <token>` header. See [MCP Server](#mcp-server) for the client config shape.

A few things to know:

- **The token is not optional.** Omit it and the container exits immediately with `a non-loopback MCP bind requires an explicit token`. The default `docker run ghcr.io/nubo-db/dynoxide` stays DynamoDB-only precisely because a token-less `0.0.0.0` MCP bind cannot boot.
- **Reaching MCP from another container** by service name (rather than `localhost`) needs that name added to the Host allowlist: `--mcp-allowed-host <name>` (e.g. `--mcp-allowed-host dynoxide`). The `-p`-mapped `localhost` access above needs nothing extra.
- **`--network host`** (Linux only) is an alternative to `-p`, but it bypasses Docker network isolation and binds MCP directly on the host's network interface, reachable from the LAN, not just the host. Prefer `-p` unless you specifically need host networking.

