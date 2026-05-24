# Dynoxide container image. FROM scratch: just the static binary, no shell,
# no CA certs (the binary has no TLS surface today). Multi-arch built with
# `docker buildx build --platform linux/amd64,linux/arm64`; the build context
# must contain both arches under dist/amd64/ and dist/arm64/.

FROM scratch

ARG TARGETARCH

COPY dist/${TARGETARCH}/dynoxide /usr/local/bin/dynoxide

WORKDIR /data

EXPOSE 8000

# Read by the healthcheck subcommand. Override with `docker run -e ...` when
# CMD is overridden to bind to a non-default port.
ENV DYNOXIDE_HEALTHCHECK_HOST=127.0.0.1 \
    DYNOXIDE_HEALTHCHECK_PORT=8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/dynoxide", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/dynoxide"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8000"]
