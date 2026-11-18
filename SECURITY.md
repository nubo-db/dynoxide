# Security Policy

## Reporting a vulnerability

If you believe you've found a security issue in dynoxide, please report it privately rather than opening a public issue.

Use GitHub's private vulnerability reporting: https://github.com/nubo-db/dynoxide/security/advisories/new

I aim to acknowledge reports within 5 business days. How quickly a fix follows depends on severity:

- Critical issues affecting the loopback-only boundary: within 30 days.
- Other confirmed issues: best effort, typically within 90 days.

## Threat model

dynoxide's MCP HTTP transport is meant for local, single-user development. Exposing it more widely takes an explicit opt-in.

The HTTP transport is itself opt-in. The default `dynoxide mcp` uses stdio and has no network exposure. Only `dynoxide mcp --http` (or `dynoxide serve --mcp`) opens an HTTP listener.

The HTTP transport defends against two threats: direct-from-tooling callers (anyone who can reach the port) and browser-based cross-origin attacks (DNS rebinding, cross-origin CSRF).

dynoxide:

- **Requires a bearer token on every request.** Each `/mcp` request must carry `Authorization: Bearer <token>`. A missing token and a wrong token both return the same `401`, so a caller can't tell the two apart. On a loopback bind with no token supplied, dynoxide generates one on first run, saves it to a per-user config file and prints guidance for configuring your client. On Unix that file has mode `0600`; on Windows it lives under your per-user `%APPDATA%`, which other standard accounts cannot read. To supply your own token, use `--token` / `--mcp-token` or `DYNOXIDE_MCP_AUTH_TOKEN`. There is no rotation mechanism: to rotate, delete the saved file (or change the env var) and restart.
- **Defaults to a loopback bind.** `--host` / `--mcp-host` can widen it, but a non-loopback bind will not start without an explicit token. Tokens are only generated automatically on loopback.
- **Validates the `Host` header** against a loopback allowlist (`localhost`, `127.0.0.1`, `::1`); anything else returns `403`. `--allowed-host` / `--mcp-allowed-host` adds names for non-loopback by-name access. The auth check runs first, and a valid token holder who then spoofs `Host` still gets `403`. The allowlist is defence in depth; the token is the primary control.
- **Validates the `Origin` header** when present, against `http://localhost`, `http://127.0.0.1`, and any added hosts; anything else returns `403`. Native MCP clients (Claude Code, Cursor, the dynoxide CLI) don't send an Origin header and pass through unaffected.

With authentication in place, dynoxide is suitable for:

- Local development on a personal machine
- CI environments
- Dockerised and network-exposed deployments, **provided a token is set** and treated as a secret (passed via `DYNOXIDE_MCP_AUTH_TOKEN`, not a flag, to keep it out of process listings and shell history)

A loopback-only escape hatch, `--no-auth` / `--mcp-no-auth`, disables authentication entirely; it refuses to start on a non-loopback bind. Use it only on a trusted single-user machine where no other local process is a concern.

None of the above applies to the stdio transport. It is scoped to the process and has no network surface.

## Known advisories

See the [Security Advisories](https://github.com/nubo-db/dynoxide/security/advisories) tab.
