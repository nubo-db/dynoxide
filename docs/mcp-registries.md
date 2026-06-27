# MCP registries

Dynoxide's [MCP server](mcp.md) is listed in the public MCP registries so
coding agents can discover it. This is the maintainer runbook for each listing:
what is automated, what is a one-off, and what needs an account you control.

The canonical server identifier across every registry is
`io.github.nubo-db/dynoxide`.

## Official registry (registry.modelcontextprotocol.io)

Automated. `server.json` at the repo root describes the server, and the
`publish-mcp-registry` job in `.github/workflows/release.yml` publishes it on
every `v*` release tag, authenticating with GitHub OIDC (no token). Cutting a
release as normal is all that is needed; the job waits for npm to go live,
stamps the release version into `server.json`, and runs `mcp-publisher publish`.

Ownership is proved against the live release artefacts, so both markers must
stay in place:

- the npm package carries `"mcpName": "io.github.nubo-db/dynoxide"` in
  `npm/dynoxide/package.json`
- the GHCR image carries `LABEL io.modelcontextprotocol.server.name="io.github.nubo-db/dynoxide"`
  in the `Dockerfile`

The **GitHub MCP Registry** (github.com/mcp) ingests from the official
registry, so a successful publish covers it too. If the server has not appeared
there a day or two after a release, email partnerships@github.com with the
official-registry URL.

To publish out of band (for example to test a `server.json` change):

```sh
brew install mcp-publisher    # or download from the registry's GitHub releases
mcp-publisher login github    # device-code OAuth; grants the io.github.nubo-db/* namespace
mcp-publisher publish         # run from the repo root, where server.json lives
```

A manual publish still validates against the live npm package and GHCR image,
so it only works for a version that has already been released with the markers
above.

## Smithery (smithery.ai)

One-off, and it needs a Smithery account. `smithery.yaml` at the repo root
already tells Smithery to launch the server over stdio via `npx -y dynoxide mcp`,
so the repo side is done. To list it:

```sh
npm install -g @smithery/cli
smithery auth login
```

Then publish following <https://smithery.ai/docs/build/publish> (the publish
subcommand changed in CLI v1.1.0, so follow the current docs rather than a
pinned command here). Optionally install the Smithery GitHub App on the repo
for push-triggered redeploys.

## Glama (glama.ai)

One-off, and it needs your GitHub OAuth. `glama.json` at the repo root names the
maintainer (`hicksy`) for the ownership claim. To list it:

1. Go to <https://glama.ai/mcp/servers> and choose "Add MCP Server".
2. Sign in with GitHub and enter the repo URL.
3. Glama build-checks the server in a sandbox; it usually appears within minutes.
4. Because this is an org-owned repo, complete the "Claim ownership" flow, which
   reads `glama.json`.

A directory listing needs no Dockerfile. To later have Glama host and run it
through their gateway, point them at the `dynoxide` npm package in the admin
panel.

## mcp.so

One-off, and it needs your GitHub account. No repo file. Comment on
<https://github.com/chatmcp/mcpso/issues/1> with:

- name: `dynoxide`
- repo: <https://github.com/nubo-db/dynoxide>
- run: `npx -y dynoxide mcp`
- transport: stdio (also streamable HTTP via `dynoxide mcp --http`)
- 34 tools

The mcp.so team adds it to the directory from there.
