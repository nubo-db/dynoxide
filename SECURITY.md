# Security Policy

## Reporting a vulnerability

If you believe you've found a security issue in dynoxide, please report it privately rather than opening a public issue.

Use GitHub's private vulnerability reporting: https://github.com/nubo-db/dynoxide/security/advisories/new

I aim to acknowledge reports within 5 business days. Fix timelines depend on severity: critical issues affecting the loopback-only boundary, within 30 days; other confirmed issues, best effort, typically within 90 days.

## Threat model

dynoxide's MCP HTTP transport is intended for local single-user development, with an explicit opt-in for wider exposure. It is opt-in to begin with: the default `dynoxide mcp` uses stdio and has no network exposure. Only `dynoxide mcp --http` (or `dynoxide serve --mcp`) opens an HTTP listener.

The HTTP transport defends against two threats: direct-from-tooling callers (anyone who can reach the port) and browser-based cross-origin attacks (DNS rebinding, cross-origin CSRF).

dynoxide:

- **Requires a bearer token on every request.** Each `/mcp` request must carry `Authorization: Bearer <token>`; a missing or wrong token returns an identical `401` (no oracle distinguishes the two). On a loopback bind with no token supplied, dynoxide generates one on first run, persists it to a per-user config file (mode `0600` on Unix), and prints client-config guidance. Supply your own with `--token` / `--mcp-token` or `DYNOXIDE_MCP_AUTH_TOKEN`. There is no rotation mechanism: to rotate, delete the persisted file (or change the env var) and restart.
- **Defaults to a loopback bind.** `--host` / `--mcp-host` can widen it, but a non-loopback bind will not start without an explicit token — auto-generation is loopback-only.
- **Validates the `Host` header** against a loopback allowlist (`localhost`, `127.0.0.1`, `::1`); anything else returns `403`. `--allowed-host` / `--mcp-allowed-host` adds names for non-loopback by-name access. The auth check runs first, so a valid token holder who then spoofs `Host` still gets `403` — the allowlist is defense-in-depth, not the primary control.
- **Validates the `Origin` header** when present, against `http://localhost`, `http://127.0.0.1`, and any added hosts; anything else returns `403`. Native MCP clients (Claude Code, Cursor, the dynoxide CLI) don't send an Origin header and pass through unaffected.

With authentication in place, dynoxide is suitable for:

- Local development on a personal machine
- CI environments
- Dockerised and network-exposed deployments, **provided a token is set** and treated as a secret (passed via `DYNOXIDE_MCP_AUTH_TOKEN`, not a flag, to keep it out of process listings and shell history)

A loopback-only escape hatch, `--no-auth` / `--mcp-no-auth`, disables authentication entirely; it refuses to start on a non-loopback bind. Use it only on a trusted single-user machine where no other local process is a concern.

The stdio transport is unaffected by all of the above — it is process-scoped and has no network surface.

## Known advisories

See the [Security Advisories](https://github.com/nubo-db/dynoxide/security/advisories) tab.
