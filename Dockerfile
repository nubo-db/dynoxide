# Dynoxide container image. FROM scratch: just the static binary, no shell,
# no CA certs (the binary has no TLS surface today). Multi-arch built with
# `docker buildx build --platform linux/amd64,linux/arm64`; the build context
# must contain both arches under dist/amd64/ and dist/arm64/.

FROM scratch

ARG TARGETARCH

COPY dist/${TARGETARCH}/dynoxide /usr/local/bin/dynoxide

# Ownership marker for the MCP registry. Must match the `name` in server.json;
# the registry reads this label off the published image to verify the OCI
# package belongs to io.github.nubo-db/dynoxide.
LABEL io.modelcontextprotocol.server.name="io.github.nubo-db/dynoxide" \
      org.opencontainers.image.title="Dynoxide" \
      org.opencontainers.image.description="DynamoDB emulator that starts in milliseconds. A drop-in for amazon/dynamodb-local, with no JVM." \
      org.opencontainers.image.source="https://github.com/nubo-db/dynoxide" \
      org.opencontainers.image.url="https://dynoxide.dev" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0" \
      org.opencontainers.image.vendor="Martin Hicks"

WORKDIR /data

# 8000: DynamoDB HTTP API (started by the default CMD).
# 19280: MCP Streamable-HTTP transport. Opt-in, not started by default - override
# CMD with `serve --mcp --mcp-host 0.0.0.0` and supply a bearer token via
# DYNOXIDE_MCP_AUTH_TOKEN (a non-loopback MCP bind refuses to boot without one).
# See the README "MCP over HTTP in Docker" section. EXPOSE is metadata only; it
# documents intent and lets `docker run -P` map the port.
EXPOSE 8000 19280

# Read by the healthcheck subcommand. Override with `docker run -e ...` when
# CMD is overridden to bind to a non-default port.
ENV DYNOXIDE_HEALTHCHECK_HOST=127.0.0.1 \
    DYNOXIDE_HEALTHCHECK_PORT=8000

# --start-interval only applies before the container first reports healthy, so
# it is set alongside --start-period. The binary boots in milliseconds, so the
# first probe lands about a quarter of a second in and the container is healthy
# from there; the ten second start period is headroom for a loaded runner, not
# a window it normally uses. That makes a healthcheck-based wait worth using,
# where the old 5s/30s pairing could cost half a minute.
#
# Building this file needs Docker Engine 25.0 or newer. --start-interval is
# parsed by the Dockerfile frontend, so an older daemon fails the build rather
# than ignoring the flag.
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --start-interval=250ms --retries=3 \
    CMD ["/usr/local/bin/dynoxide", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/dynoxide"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8000"]
