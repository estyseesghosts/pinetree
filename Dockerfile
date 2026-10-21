# syntax=docker/dockerfile:1.7
#
# pinetree — modular Misskey-compatible SNS server.
#
#   docker build -t pinetree:latest .
#
# Debian (not Alpine) is deliberate: pinetree shells out to `curl` for
# outbound HTTPS federation delivery, and Debian's curl is built against
# OpenSSL 3 with the feature set the federation client relies on
# (--resolve, --raw, --max-filesize, --noproxy, --cacert).
#
# See pinetree_container.md §5 for the health-probe contract and §8 for the
# runtime gotchas this image works around.

ARG RUST_VERSION=1.99
ARG DEBIAN_RELEASE=bookworm

# ---- builder ---------------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS build

# `ring` compiles C; no OpenSSL headers are needed (pinetree has no
# native-tls dependency).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Dependency layer: only the manifests are copied, so an edit to src/ does
# not re-resolve and re-compile the whole dependency graph.
COPY Cargo.toml Cargo.lock ./

# Stub the two crate roots so cargo resolves and builds the dependency graph
# once. The real sources replace the stubs in the next layer.
RUN mkdir -p src \
    && printf 'fn main() {}\n' > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src/ ./src/

# The stub artefacts are already in target/; force a rebuild of the crate
# itself only, not of the dependencies.
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked \
    && strip target/release/pinetree \
    && install -Dm755 target/release/pinetree /out/pinetree

# ---- runtime ---------------------------------------------------------------
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

# ca-certificates → TLS roots for outbound federation
# curl           → the HTTPS federation transport (see §8.1); NOT optional
# tini           → PID 1: reaps children and forwards signals to the server
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tini \
    && rm -rf /var/lib/apt/lists/*

ARG APP_UID=10001
ARG APP_GID=10001

RUN groupadd --system --gid ${APP_GID} pinetree \
    && useradd --system --uid ${APP_UID} --gid ${APP_GID} \
        --home-dir /app --shell /usr/sbin/nologin pinetree \
    && mkdir -p /app /data \
    && chown -R ${APP_UID}:${APP_GID} /app /data \
    && chmod 700 /data

WORKDIR /app

COPY --from=build /out/pinetree /usr/local/bin/pinetree
COPY docker/pinetree.container.json /etc/pinetree/config.json

# Drop to the unprivileged user before the server starts. Everything above
# runs as root inside the build; only the server itself is unprivileged.
USER pinetree

# The internal listener is fixed at 3000 so the healthcheck, EXPOSE and the
# compose file cannot drift apart. Publish it as you like: `-p 8080:3000`.
EXPOSE 3000

# Liveness. `curl` is already in the image, so no extra package is needed.
# Readiness is available at /ready for orchestrators that need it.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:3000/healthz || exit 1

# tini is PID 1: it forwards SIGTERM to the server (which now drains) and
# reaps any child the server leaves behind.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/pinetree"]

# --data-dir and --listen are passed as flags so they survive an operator
# replacing /etc/pinetree/config.json with their own file (CLI flags
# override file values).
CMD ["--config=/etc/pinetree/config.json", "--data-dir=/data", "--listen=0.0.0.0:3000"]

VOLUME ["/data"]
