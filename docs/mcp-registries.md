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

One-off, and it needs a Smithery account. Smithery distributes a local stdio
server as an [MCPB bundle](https://github.com/anthropics/mcpb) that clients
download and run. The bundle source is `mcpb/manifest.json`; it launches the
server with `npx -y dynoxide mcp`, so the runtime always pulls the current npm
release regardless of the bundle's own version.

Build the bundle, sign in, and publish:

```sh
npx -y @anthropic-ai/mcpb pack mcpb mcpb/dynoxide.mcpb
npx -y @smithery/cli auth login
npx -y @smithery/cli mcp publish mcpb/dynoxide.mcpb -n nubo-db/dynoxide
```

The empty `"tools": []` in the manifest is load-bearing. The Smithery CLI drops
the `tools` field from its upload when the key is absent, and the registry then
rejects the payload with `400 "No values to set"`
(<https://github.com/smithery-ai/cli/issues/770>). Leave it empty; a populated
list is rejected on a separate schema mismatch.

To republish after a release, bump `version` in `mcpb/manifest.json`, rebuild,
and re-run the publish command. The server already exists, so it goes straight
to the new release.

## Glama (glama.ai)

One-off, and it needs your GitHub OAuth. `glama.json` at the repo root names the
maintainer (`hicksy`) for the ownership claim. To list it:

1. Go to <https://glama.ai/mcp/servers> and choose "Add MCP Server".
2. Sign in with GitHub and submit the repo URL for review.
3. Glama build-checks the server in a sandbox and lists it, usually within minutes.

`glama.json` names the maintainer (`hicksy`) so the listing is attributed to you;
the current flow submits for review rather than offering a separate claim step.

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
