# syntax=docker/dockerfile:1.7
#
# pinetree — modular Misskey-compatible SNS server.
#
#   Minimal image (no FFmpeg; transcoding stays disabled unless the operator
#   explicitly opts in via config):
#     docker build --target runtime -t pinetree:latest .
#
#   FFmpeg image (same binary/hardening plus a known FFmpeg package):
#     docker build --target runtime-ffmpeg -t pinetree:ffmpeg .
#
# Enabling transcoding is always explicit in the chosen config JSON
# (`transcode.enabled: true` — see docker/pinetree.transcode-enabled.example.json);
# the FFmpeg image never overrides the application's disabled-by-default
# policy. With `transcode.enabled: true` but no working toolchain, startup
# fails closed (non-zero exit, never serve mode).
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

# ---- runtime-base ------------------------------------------------------------
# Shared base for both final targets: unprivileged user, /app + /data with
# correct ownership, the server binary and the default (transcode-disabled)
# config. No distro packages are installed here so the two final targets
# differ only by their package set. Stays root; each final target drops to
# the unprivileged user itself (USER carries across stages, so package
# installs must run before the drop).
FROM debian:${DEBIAN_RELEASE}-slim AS runtime-base

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

# ---- runtime-ffmpeg ----------------------------------------------------------
# FFmpeg variant: same binary and hardening as `runtime`, plus the known
# Debian FFmpeg package (bookworm: 5.1.x full build with libvpx-vp9, libopus
# and libwebp). Build-time probe checks the version line and the required
# encoders; the full encoder+decoder set is re-checked by the application's
# own startup gate (`TranscodeConfig::validate`), which fails closed when
# transcoding is enabled without a working toolchain.
FROM runtime-base AS runtime-ffmpeg

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tini \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && ffmpeg -hide_banner -version \
    && for enc in libvpx-vp9 libopus libwebp; do \
        ffmpeg -hide_banner -encoders | grep -q " $enc " \
            || { echo "ffmpeg image missing required encoder $enc" >&2; exit 1; }; \
    done

# Drop to the unprivileged user before the server starts. Everything above
# runs as root inside the build; only the server itself is unprivileged.
USER pinetree

# The internal listener is fixed at 3000 so the healthcheck, EXPOSE and the
# compose file cannot drift apart. Publish it as you like: `-p 8080:3000`.
EXPOSE 3000

# Liveness. `curl` is already in the image, so no extra package is needed.
# Readiness is available at /ready for orchestrators that need it; the
# healthcheck moves to readiness once the Batch G/H contract lands.
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

# ---- runtime -----------------------------------------------------------------
# Minimal production target: CA certificates, required transport/runtime
# tools, no FFmpeg. Deliberately LAST so a bare `docker build .` (no
# --target) yields the minimal image; request --target runtime-ffmpeg
# explicitly for transcoding. Both media pools live under /data (persistent
# storage); see the CMD/volume contract above.
FROM runtime-base AS runtime

# ca-certificates → TLS roots for outbound federation
# curl           → the HTTPS federation transport (see §8.1); NOT optional
# tini           → PID 1: reaps children and forwards signals to the server
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Drop to the unprivileged user before the server starts. Everything above
# runs as root inside the build; only the server itself is unprivileged.
USER pinetree

# The internal listener is fixed at 3000 so the healthcheck, EXPOSE and the
# compose file cannot drift apart. Publish it as you like: `-p 8080:3000`.
EXPOSE 3000

# Liveness. `curl` is already in the image, so no extra package is needed.
# Readiness is available at /ready for orchestrators that need it; the
# healthcheck moves to readiness once the Batch G/H contract lands.
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
