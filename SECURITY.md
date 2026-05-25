# Security Policy

## Reporting a vulnerability

If you believe you've found a security issue in dynoxide, please report it privately rather than opening a public issue.

Use GitHub's private vulnerability reporting: https://github.com/nubo-db/dynoxide/security/advisories/new

I aim to acknowledge reports within 5 business days. Fix timelines depend on severity: critical issues affecting the loopback-only boundary, within 30 days; other confirmed issues, best effort, typically within 90 days.

## Threat model

dynoxide's MCP HTTP transport is intended for local single-user development. It is opt-in: the default `dynoxide mcp` uses stdio and has no network exposure. Only `dynoxide mcp --http` opens a localhost HTTP listener.

The threat model for the HTTP transport is browser-based cross-origin attacks (DNS rebinding, cross-origin CSRF), not multi-user systems or network-adjacent attackers.

In that scope, dynoxide:

- Binds to `127.0.0.1` only. There is no flag to override this.
- Validates the `Host` header against a loopback allowlist (`localhost`, `127.0.0.1`, `::1`). Anything else returns 403.
- Validates the `Origin` header when present, against `http://localhost` and `http://127.0.0.1`. Anything else returns 403. Native MCP clients (Claude Code, Cursor, the dynoxide CLI) don't send an Origin header and pass through unaffected.

dynoxide does not yet implement client authentication on the MCP HTTP transport. The Host and Origin controls work against browser-based attackers but don't help against direct-from-tooling callers who can spoof Host and send no Origin. This means dynoxide is currently suitable for:

- Local development on a personal machine
- CI environments where the runner is dedicated to a single job (e.g. GitHub Actions hosted runners)

It is not currently suitable for:

- Multi-tenant CI infrastructure
- Dockerised deployments exposed beyond loopback
- Any environment where untrusted code or users share network access with dynoxide

Authentication is the next priority; see [#27](https://github.com/nubo-db/dynoxide/issues/27). Until it lands, restrict dynoxide MCP HTTP usage to environments matching the suitable-for list above, or use the stdio transport instead (`dynoxide mcp` without `--http`).

## Known advisories

See the [Security Advisories](https://github.com/nubo-db/dynoxide/security/advisories) tab.
