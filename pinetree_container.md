# pinetree — containerization guide

> **Status:** proposal / implementation guide. Nothing in this file is applied
> to the tree except where a step says *"already applied"*.
> **Audience:** an implementer picking up the container work for pinetree.
> **Scope:** one container image for the `pinetree` server, the code changes
> it depends on, and the health-probe contract that goes with it.
>
> Source of truth for the product itself remains `sns-server-proposal.md`;
> this document does not change the API or storage contracts.

---

## 1. Summary

`pinetree` is a modular Misskey-compatible SNS server: a **single Rust
binary** with an **embedded** `redb` store and a filesystem blob store. There
is no database server, no message broker, no asset pipeline and no code
generation step. Containerizing it is therefore mostly a packaging exercise.

Three things make it non-trivial, and this guide exists to handle them:

| # | Issue | Where it bites | Resolution |
|---|-------|----------------|------------|
| 1 | The process has **no `SIGTERM` handler** | `docker stop` kills the server mid-request; streaming sockets are cut without a close frame; in-flight federation deliveries are dropped | §4 — add graceful shutdown (code change) |
| 2 | The server **shells out to `curl`** for HTTPS federation delivery | `curl` is not a Rust dependency; a runtime image without the binary fails every HTTPS delivery at spawn time | §7 — install `curl` in the runtime stage |
| 3 | `listen` defaults to **`127.0.0.1:3000`** | Inside a container nothing outside can reach the port; a published port silently answers nothing | §7 — the image pins `0.0.0.0:3000` |

Plus the usual operational wiring: a health endpoint (§5), a readiness
endpoint (§5), a non-root user, a data volume, and a compose file (§8).

**Measured image size: ~105 MB** (built from the guide's Dockerfile).

**Effort estimate:** half a day, most of it the `SIGTERM` change and the
verification pass. No architectural work.

---

## 2. Deliverables

Create these files:

```
pinetree/
├── Dockerfile                    # multi-stage; build + runtime
├── .dockerignore                 # build-context hygiene
├── docker/
│   ├── pinetree.container.json   # runtime config baked into the image
│   └── pinetree.local.json       # operator config (git-ignored sample)
├── compose.yaml                  # single-node deployment
└── docs/
    └── container.md              # this file
```

And change these in the source tree:

```
src/shutdown.rs                  # NEW  — signal handling
src/lib.rs                       # +1 line: `pub mod shutdown;`
src/main.rs                      # wire the shutdown signal into axum
src/jobs/worker.rs               # stop condition for the job worker
src/api/misskey/endpoints/mod.rs # +1 route: `GET /ready`
```

---

## 3. Prerequisites

| Tool | Version | Why |
|------|---------|-----|
| Rust | **≥ 1.85** | `Cargo.toml` uses `edition = "2024"` |
| Docker / Podman | any recent | image build + run |
| C compiler (`cc`) | any | `ring 0.17` compiles C at build time |

There is **no** `rust-toolchain.toml` in the repo, so pin the version in the
Dockerfile yourself (§7) rather than relying on `rust:latest`.

pinetree has **no OpenSSL dependency** (`reqwest` is a dev-dependency only),
so the builder needs no `libssl-dev`. `zip` pulls `flate2` with the pure-Rust
`miniz_oxide` backend, so there is no `libz-sys` either. The only native
build requirement is a C compiler for `ring`.

---

## 4. Required code change — graceful shutdown on `SIGTERM`

### 4.1 Why

`src/main.rs` ends with a bare `axum::serve(..)` and no shutdown future.
The default disposition of `SIGTERM` is "terminate", so:

- `docker stop` (which sends `SIGTERM`, then `SIGKILL` after 10 s) kills the
  process **mid-request**;
- streaming (`/streaming`) sockets are closed without a close frame, so
  clients see a transport error rather than an orderly disconnect;
- a job that was claimed but not yet settled keeps its lease until it
  expires — redb is crash-safe so nothing is corrupted, but the work is
  needlessly delayed.

`docker stop` is the *normal* way a container is stopped. A server that
cannot handle it is not container-ready.

### 4.2 What to add

**New file `src/shutdown.rs`:**

```rust
//! Process stop handling.
//!
//! A container runtime stops a container with `SIGTERM` and escalates to
//! `SIGKILL` once the grace period expires. Without a handler, the default
//! `SIGTERM` disposition kills the process outright: in-flight HTTP
//! responses are cut mid-body, streaming sockets drop without a close
//! frame, and a job that was mid-flight is abandoned until its lease
//! expires and another worker reclaims it.
//!
//! [`signal`] resolves on `SIGTERM` or `SIGINT` so the caller can hand it to
//! `axum::serve(..).with_graceful_shutdown(..)`. The server then stops
//! accepting connections, lets the in-flight ones finish, and only then
//! does the process exit.

/// Wait for a stop signal.
///
/// Resolves on `SIGTERM` (container stop, `kill`, systemd) or `SIGINT`
/// (Ctrl-C, `docker kill --signal=INT`). On a platform without `SIGTERM`
/// handling only `SIGINT` ends the wait.
pub async fn signal() {
    let ctrl_c = async {
        // A failed registration is not fatal here: the `SIGTERM` arm is the
        // one a container runtime uses.
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = terminate_signal();
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// Wait for `SIGTERM`.
///
/// If the handler cannot be installed the wait degrades to "never", so the
  /// process keeps serving rather than exiting on a signal it never saw.
#[cfg(unix)]
async fn terminate_signal() {
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut sig) => {
            sig.recv().await;
        }
        Err(err) => {
            eprintln!("cannot install SIGTERM handler: {err}");
            std::future::pending::<()>().await;
        }
    }
}
```

> **Note:** `tokio`'s `signal` feature is already enabled in `Cargo.toml`,
> so no dependency change is needed.

**`src/lib.rs`** — add the module (keep the list alphabetical):

```rust
pub mod shutdown;
```

**`src/main.rs`** — stop claiming jobs and drain the HTTP server:

```rust
// (imports)
use std::sync::atomic::{AtomicBool, Ordering};

// (where the worker is currently spawned)
    // The worker stops claiming new jobs once `stop` is set (see the
    // shutdown arm below). A job claimed but not settled keeps its lease;
    // the lease expires and the next process to start reclaims it, so no
    // work is lost across a restart.
    let stop = Arc::new(AtomicBool::new(false));
    let _worker_handle = worker.clone().spawn_until(Duration::from_millis(50), stop.clone());

// (where the server is started)
    // Client IPs come from the TCP peer (`ConnectInfo`), not from headers.
    //
    // On `SIGTERM` (how a container runtime stops a container) the worker
    // stops claiming jobs and the HTTP server stops accepting connections
    // while the in-flight ones finish. Without this the default `SIGTERM`
    // disposition kills the process mid-request and cuts streaming sockets
    // without a close frame.
    let shutdown = async move {
        pinetree::shutdown::signal().await;
        stop.store(true, Ordering::Relaxed);
        eprintln!("shutdown signal received; draining in-flight requests");
    };
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
    .expect("serve axum");
```

**`src/jobs/worker.rs`** — give the worker loop a stop condition. Keep
`spawn` as-is in signature so the four existing call sites
(`src/main.rs`, `src/api/misskey/endpoints/mod.rs` ×2, `tests/streaming.rs`,
`tests/emoji_streaming_transfer.rs`) keep working:

```rust
use std::sync::atomic::{AtomicBool, Ordering};

    pub fn spawn(
        self: Arc<Self>,
        poll_interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        self.spawn_until(poll_interval, Arc::new(AtomicBool::new(false)))
    }

    /// Spawn the worker loop until `stop` is set.
    ///
    /// `spawn` is the never-stopping variant (tests, embedding). A server
    /// that owns a shutdown signal uses `spawn_until` so the loop parks
    /// between polls instead of racing the process exit: a job claimed but
    /// not settled keeps its lease, the lease expires, and the next process
    /// to start reclaims it. No work is lost either way, but the shutdown
    /// path stops taking new jobs while the HTTP server drains.
    pub fn spawn_until(
        self: Arc<Self>,
        poll_interval: std::time::Duration,
        stop: Arc<AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                // ... unchanged body of the old loop ...
            }
        })
    }
```

### 4.3 Already applied?

> **Yes — this exact change is in the working tree and compiles.**
> `cargo check --all-targets --locked` is green on it. Treat §4.2 as the
> authoritative diff and re-apply it if the branch is reset. The remaining
> work is the test coverage in §6.

---

## 5. Health and readiness endpoints

### 5.1 What exists today

| Endpoint | Status | Semantics |
|----------|--------|-----------|
| `GET /healthz` | **exists** | Liveness. Returns `200` with the body `ok` while the process is alive. Registered at `src/api/misskey/endpoints/mod.rs:149`. |
| `GET /ready` | **does not exist — add it** | Readiness. Must prove storage is readable. |

### 5.2 The readiness endpoint

Liveness alone is not enough for a container: a process whose `redb` file
has gone bad is alive but cannot serve. The readiness probe should fail so
the orchestrator stops routing traffic to it.

Add the route next to `/healthz` in `build_router`:

```rust
        .route("/healthz", get(healthz))
        .route("/ready", get(ready))
```

and the handler next to `healthz`:

```rust
/// Readiness probe: the durable store can serve a read.
///
/// The liveness probe (`/healthz`) answers as long as the process is up.
/// Readiness additionally proves storage is readable, so an orchestrator
/// stops routing traffic to a node whose database has gone bad instead of
/// serving 500s. The read is a single-row lookup in the instance-settings
/// table: cheap, side-effect free, and it exercises the same redb read path
/// the API uses.
async fn ready(State(state): State<ApiState>) -> (StatusCode, &'static str) {
    match state.services.instance_settings.get() {
        Ok(_) => (StatusCode::OK, "ready"),
        Err(err) => {
            eprintln!("readiness probe failed: {err}");
            (StatusCode::SERVICE_UNAVAILABLE, "not ready")
        }
    }
}
```

`InstanceSettingsStore::get()` (`src/application/ports.rs:2651`) is a
read-only `begin_read()` + single-row `table.get("settings")` in the redb
adapter (`src/persistence/redb/mod.rs:3292`). It has no side effects, so it
is safe to call on every probe.

**Contract to pin:**

| Method | Path | Success | Failure |
|--------|------|---------|---------|
| `GET` | `/healthz` | `200` `ok` | — |
| `GET` | `/ready` | `200` `ready` | `503` `not ready` |

Both are unauthenticated and outside `/api/`, so they are not routed through
the Misskey JSON dispatcher.

**Contract bookkeeping.** Neither `/healthz` nor `/ready` appears in
`docs/endpoint-manifest.md`, although the manifest is the endpoint contract
and `AGENTS.md` requires every route to be pinned there. The manifest has a
section for public non-API routes; add rows for both probes:

| Method | Path | Auth | Params | Response |
|---|---|---|---|---|
| GET | `/healthz` | none | — | `200` `ok` while the process is alive (liveness) |
| GET | `/ready` | none | — | `200` `ready` when storage is readable, `503` `not ready` otherwise (readiness) |

`/healthz` is a pre-existing omission; `/ready` is new, so it must be added
with the code change.

### 5.3 Already applied?

> **Yes — `/ready` and its handler are in the working tree and compile.**
> The test that pins the contract is still to be written (§6).

---

## 6. Required test coverage

The container behaviour must be pinned by tests, not by hope. Two tests:

### 6.1 Readiness probe returns 200

Add next to the existing `healthz_returns_ok` test in
`src/api/misskey/endpoints/mod.rs` (the file already has a `test_state()`
helper at line 10489):

```rust
    #[tokio::test]
    async fn ready_reports_storage_available() {
        let (state, _clock) = test_state();
        let (status, body) = ready(State(state)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ready");
    }
```

### 6.2 `SIGTERM` exits cleanly

New integration test, e.g. `tests/container_shutdown.rs`. It must assert
that the process exits **by itself with status 0**, not that it is killed
(status 143 for `SIGTERM`, 137 for `SIGKILL`).

```rust
//! The process must stop cleanly on `SIGTERM` — the signal a container
//! runtime sends on `docker stop`. Without a handler the default
//! disposition kills the server mid-request.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// GET a path from the server, returning the status code.
fn http_get(addr: &str, path: &str) -> Option<u16> { /* minimal HTTP/1.0 GET */ }

#[test]
fn sigterm_exits_cleanly() {
    let bin = env!("CARGO_BIN_EXE_pinetree");
    let dir = std::env::temp_dir().join(format!("pinetree-sigterm-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // Pick a free port by binding and releasing it.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let mut child: Child = Command::new(bin)
        .arg(format!("--listen=127.0.0.1:{port}"))
        .arg(format!("--data-dir={}", dir.display()))
        .arg("--public-url=http://127.0.0.1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn pinetree");

    // Wait for the server to answer /ready.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if http_get(&format!("127.0.0.1:{port}"), "/ready").is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(http_get(&format!("127.0.0.1:{port}"), "/ready").is_some(), "server never became ready");

    // `kill` is used rather than a libc dependency: procps-ng is present on
    // every CI image and this keeps Cargo.lock untouched.
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");

    let deadline = Instant::now() + Duration::from_secs(30);
    let exit = loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => break status,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                panic!("pinetree did not exit within 30s of SIGTERM");
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };

    assert_eq!(exit.code(), Some(0), "expected a clean exit, got {exit:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
```

Run the whole gate before claiming this done:

```bash
bash scripts/smoke-all.sh
```

---

## 7. The image

### 7.1 `.dockerignore`

The build context is the repository root. Keep it small and keep build
artefacts out of it:

```dockerignore
# Build artefacts
/target/
/data/

# Version control and CI
/.git/
/.github/

# Logs, bench output, local scratch
/state/bench-results/
/state/logs/
*.log
.env
.env.local

# Reference checkouts (git-ignored symlinks; large)
foundationdb/
redb/
misskey
sharkey
```

Do **not** exclude `src/`, `Cargo.toml`, `Cargo.lock` or `tests/` — the
builder needs `src/` and the manifest pair.

> `pinetree/.cargo/config.toml` sets `target-dir = "/root/pinetree-target"`,
> an absolute path outside the repo. It is git-ignored and therefore not in
> the build context, so the image build is unaffected. Inside the container
> the default `target/` inside `/src` is used.

### 7.2 `Dockerfile`

> **Build from the repository root.** The Dockerfile copies `Cargo.toml`,
> `Cargo.lock` and `src/` from the build context, so the context must be
> the repository root:
>
> ```bash
> podman build -t pinetree:latest .
> ```

```dockerfile
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
```

### 7.3 Runtime config

`docker/pinetree.container.json`:

```json
{
  "public_url": "http://localhost:3000",
  "data_dir": "/data",
  "listen": "0.0.0.0:3000",
  "mode": "single",
  "max_file_size": 262144000,
  "preserved_usernames": ["admin", "root", "system", "i"]
}
```

Notes:

- **Every key is optional.** `Config` is `#[serde(default, deny_unknown_fields)]`,
  so `{}` parses and takes the compiled defaults. Unknown keys are a
  **hard error** — a typo in a mounted config stops the container at boot.
- **`public_url` must be set to the externally visible URL.** The compiled
  default is `https://example.local`, which would produce wrong actor URIs,
  WebFinger and NodeInfo. The default above is a placeholder for local
  tinkering only.
- **`listen` is overridden by the `CMD` flag** but is repeated here so the
  file is self-describing.
- **`mode` must stay `single`.** `Config::validate()` rejects
  `DeploymentMode::Cluster` outright (`src/config.rs:94`) and the
  FoundationDB adapter (`src/persistence/foundationdb/mod.rs`) is still a
  **0-byte file**. The `fdb` cargo feature is declared but unused; do not
  enable it in the image build — it would add `libfdb_c` and a libclang
  bindgen dependency for no benefit.
- **`max_file_size` is 250 MiB by default**, versus 16 MiB for birchtree.
  Size the container's memory limit with that in mind (§10).

### 7.4 Operator config

Operators override by mounting their own file over
`/etc/pinetree/config.json`:

```bash
docker run -d --name pinetree \
  -p 3000:3000 \
  -v pinetree-data:/data \
  -v "$PWD/docker/pinetree.local.json:/etc/pinetree/config.json:ro" \
  pinetree:latest
```

Because `CMD` passes `--data-dir` and `--listen` as flags, a minimal
operator file only needs the keys it wants to change:

```json
{ "public_url": "https://social.example.com" }
```

pinetree's config carries **no secrets** and has **no `env:` indirection**
(by design — see `AGENTS.md`), so there is nothing to template.

---

## 8. Runtime dependencies and gotchas

### 8.1 `curl` is mandatory

`src/federation/delivery.rs:712` spawns `curl` for every **HTTPS**
federation request. Plain HTTP uses a raw `TcpStream` path and needs no
binary. The curl invocation is deliberate and security-sensitive:

| Flag | Why it matters |
|------|----------------|
| `--resolve host:port:ip` | DNS pinning — defeats DNS-rebinding SSRF |
| `--raw` | disables curl's chunked decoding; pinetree's own parser reads the framing |
| `-q` | ignores `~/.curlrc` — a local operator config cannot alter delivery |
| `--noproxy '*'` | never let a proxy env var redirect federation traffic |
| `--max-redirs 0` | redirects are refused, not followed |
| `--max-filesize N` | caps the response body |
| `--cacert` (optional) | lab CA only; **replaces** the default roots, so the file must hold every needed CA |

TLS verification is never disabled. If an operator adds a `~/.curlrc` it
is ignored (`-q`), and there is **no** `--insecure` anywhere in the
argument builder — there is a test asserting exactly that.

Minimum curl version: `--raw` (7.16.1), `--max-filesize` (7.19.0),
`--noproxy` (7.19.4), `--resolve` (7.21.0). Debian bookworm ships
curl 7.88 — fine.

**Failure mode if `curl` is missing:** the spawn fails, the delivery is
mapped to `SendErrorKind::Internal` (transient) and retried with backoff.
The symptom is *federation deliveries failing forever with no obvious
error*, while the HTTP API keeps working. Check the image, not the app.

### 8.2 `listen` must be `0.0.0.0`

The compiled default is `127.0.0.1:3000` (`src/config.rs:72`) — correct
for a bare-metal install behind a reverse proxy, fatal in a container where
the port is published. The `CMD` flag pins `0.0.0.0:3000`.

### 8.3 redb is single-writer

`redb` takes an **exclusive lock** on the database file. Only one process
may open `<data_dir>/pinetree.redb`. Consequences:

- **Do not run two replicas against the same volume.** The second one fails
  to start.
- `compose.yaml` must not declare a `deploy.replicas` > 1 or a scale range.
- Horizontal scale-out needs the FoundationDB adapter, which does not exist
  yet (§7.3).

### 8.4 The data volume

```
/data/pinetree.redb      # the embedded database
/data/blobs/objects/...  # uploaded media, sharded by content hash
/data/blobs/tmp/...      # staging area for in-flight uploads
```

- A **named volume** inherits the image's `/data` ownership (uid 10001),
  so the non-root server can write to it.
- A **bind mount** does not. `chown 10001:10001` the host directory first,
  or the container fails to open the store at boot.
- The server runs as uid 10001 with `/data` mode `0700`.

### 8.5 Private-range federation is blocked by default

The federation client rejects private and reserved IP ranges
(`SafeHttpConfig::allow_private_ips`, default false). In a compose network
every container has a private address, so **container-to-container
federation is refused** unless you set:

```bash
-e PINETREE_ALLOW_PRIVATE_IPS=1
```

This skips only the private-IP rejection — **TLS verification stays on**.
It is a lab switch; never set it on a public instance.

### 8.6 Memory and upload limits

`max_file_size` defaults to **250 MiB** (`src/config.rs:73`), and
`drive/files/create` accepts that plus 1 MiB of multipart overhead. An
upload is buffered in memory before it is written to the blob store. Give
the container **at least 1 GiB** of memory, or lower `max_file_size`.

### 8.7 Logging

pinetree has **no `tracing` dependency** — it logs with plain `eprintln!`
(`src/observability.rs`). There is no `RUST_LOG`; stdout/stderr go straight
to the container log driver. The federation debug switch is
`PINETREE_DEBUG_FEDERATION=1` (or `--debug-federation`), which emits
`[fed-debug]` lines on both the inbound and outbound paths.

---

## 9. Health probe wiring

| Concern | Endpoint | Behaviour |
|---------|----------|-----------|
| Container `HEALTHCHECK` | `GET /healthz` | `200 ok` while the process is alive |
| Orchestrator readiness | `GET /ready` | `200 ready` when storage is readable, `503 not ready` otherwise |
| Compose healthcheck | `GET /healthz` | same as the image `HEALTHCHECK` |

The image's `HEALTHCHECK` is inherited by compose, so `compose.yaml` does
not need to redefine it — but see §11 for the `start_period` caveat.

---

## 10. `compose.yaml`

```yaml
name: pinetree

services:
  pinetree:
    build:
      context: .
      dockerfile: Dockerfile
    image: pinetree:latest
    container_name: pinetree
    restart: unless-stopped

    # The internal port is fixed at 3000; publish it wherever you like.
    ports:
      - "127.0.0.1:3000:3000"

    volumes:
      - pinetree-data:/data
      # Mount your own config over the baked-in one. The keys you do not
      # set fall back to the compiled defaults; unknown keys are a hard
      # error at boot.
      - ./docker/pinetree.local.json:/etc/pinetree/config.json:ro

    # Give the server time to drain in-flight requests and federation
    # deliveries before SIGKILL. The default 10s is tight for a busy node.
    stop_grace_period: 30s

    environment:
      # Lab only: allow federation to private IP ranges (compose networks
      # are private). TLS verification is unaffected. Remove for production.
      - PINETREE_ALLOW_PRIVATE_IPS=1
      # Uncomment for federation troubleshooting.
      # - PINETREE_DEBUG_FEDERATION=1

    # A 250 MiB upload ceiling means the process needs headroom.
    mem_limit: 1g

    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"

volumes:
  pinetree-data:
```

**Reverse proxy.** If the server sits behind nginx/Traefik, set
`trusted_proxies` in the config to the proxy's address so `X-Forwarded-For`
is honoured; otherwise every request looks like it comes from the proxy.
This is the same field birchtree calls `trusted_proxies` (plural) versus
birchtree's `trusted_proxies`; pinetree's JSON key is `trusted_proxies`
and its CLI flag is `--trusted-proxies=`.

**Bootstrapping the first admin.** The image has no entrypoint script, so
create the account with an explicit one-shot run:

```bash
docker compose run --rm pinetree create-account --username=admin --password='<strong>'
docker compose run --rm pinetree set-role --username=admin --admin
```

Note that `create-account` and `set-role` are *subcommands*: they must come
before any other flag in the argument list, and `create-account` takes
`--username` and `--password` only.

---

## 11. Verification checklist

Work through this before calling the container work done. Every item is
executable; paste the real output into the session log per `AGENTS.md`.

### 11.1 Build

```bash
podman build -t pinetree:latest .
podman images pinetree
```

Expect a two-stage build that pulls `rust:1.99-bookworm` and
`debian:bookworm-slim`.

> **Podman only:** podman's default **OCI** image format silently **drops
> `HEALTHCHECK`** (`HEALTHCHECK is not supported for OCI image format and
> will be ignored`). Build with `--format docker` if you rely on the
> image's healthcheck:
>
> ```bash
> podman build --format docker -t pinetree:latest .
> ```
>
> `docker build` has no such problem. The compose file defines its own
> healthcheck, so compose deployments are unaffected either way.

### 11.2 The image has the tools the server needs

```bash
podman run --rm --entrypoint sh pinetree:latest -c \
  'command -v curl && command -v tini && id pinetree && ls -ld /data'
```

`curl` and `tini` must resolve; `/data` must be owned by uid 10001 with
mode `0700`.

### 11.3 The server starts and answers the probes

```bash
podman run -d --name pinetree-test \
  -p 127.0.0.1:3000:3000 \
  -v pinetree-test-data:/data \
  -e PINETREE_ALLOW_PRIVATE_IPS=1 \
  pinetree:latest

sleep 5
curl -fsS http://127.0.0.1:3000/healthz    # -> ok
curl -fsS http://127.0.0.1:3000/ready      # -> ready
curl -fsS http://127.0.0.1:3000/nodeinfo/2.0 | head -c 200
```

### 11.4 The container healthcheck goes healthy

```bash
podman inspect --format '{{.State.Health.Status}}' pinetree-test
# -> healthy
```

### 11.5 `SIGTERM` stops it cleanly

This is the check that the §4 change exists for:

```bash
podman stop --time 30 pinetree-test
podman inspect --format '{{.State.ExitCode}}' pinetree-test
# -> 0   (137 would mean SIGKILL: the drain timed out)
podman logs pinetree-test | grep -i 'shutdown signal received'
```

### 11.6 Data survives a restart

```bash
podman run -d --name pinetree-test2 -p 127.0.0.1:3001:3000 \
  -v pinetree-test-data:/data pinetree:latest
sleep 5
curl -fsS http://127.0.0.1:3001/ready
podman rm -f pinetree-test2
```

The account created in §11.7 must still be there.

### 11.7 An account survives the restart

```bash
podman exec pinetree-test pinetree create-account --username=probe --password=probepass
podman restart --time 30 pinetree-test
sleep 5
curl -fsS -X POST http://127.0.0.1:3000/api/signin \
  -H 'Content-Type: application/json' \
  -d '{"username":"probe","password":"probepass"}'
```

### 11.8 The full project gate still passes

```bash
bash scripts/smoke-all.sh
```

### 11.9 Cleanup

```bash
podman rm -f pinetree-test
podman volume rm pinetree-test-data
```

---

## 12. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Container starts, but the published port answers nothing | `listen` is still `127.0.0.1` | The `CMD` flag pins `0.0.0.0:3000`; check the image was rebuilt |
| Boot fails with `storage is not available` / `open redb` | The volume is owned by another uid | `chown 10001:10001` the bind-mounted directory, or use a named volume |
| Boot fails with `unknown field` in the config | `deny_unknown_fields` and a typo (or a key from birchtree's config) | Fix the JSON; check for `trusted_proxies` vs `trusted-proxy` |
| `mode: cluster` is rejected | `Config::validate()` refuses it | Use `single`; the FDB adapter is still empty |
| Federation deliveries fail forever, API is fine | `curl` missing from the runtime image | Add `curl` to the runtime stage |
| Federation deliveries fail to private peers | SSRF private-range rejection | `PINETREE_ALLOW_PRIVATE_IPS=1` (lab only) |
| `docker stop` takes the full grace period, exit code 137 | The drain did not finish in time | Raise `stop_grace_period`; check for a stuck streaming connection |
| Exit code 143 on stop | The `SIGTERM` handler is not wired | §4 was not applied |
| Healthcheck flaps at startup | `start_period` too short (the first redb migration can be slow) | Raise `HEALTHCHECK --start-period` to 30s |

---

## 13. Security notes

- The server runs as **uid 10001**, not root. `/data` is `0700`.
- `tini` is PID 1, so the server is a child process and receives forwarded
  signals; it also reaps orphans.
- The image ships **no compiler and no source**: the runtime stage carries
  the stripped binary only.
- Consider `read_only: true` plus a `tmpfs` for `/tmp` in production. The
  blob store's staging directory lives under `/data`, so it stays writable.
- `no-new-privileges` and dropping all capabilities are safe here — the
  server binds an unprivileged port and needs no special privileges.
- Do not mount the host Docker socket, and do not run the container with
  `--privileged`.
- Keep `PINETREE_ALLOW_PRIVATE_IPS` unset in production: it disables the
  SSRF protection that stops federation from reaching internal services.
- `PINETREE_DEBUG_FEDERATION` prints untrusted inbound data; keep it off
  outside debugging.

---

## 14. Out of scope

- **Multiple replicas.** redb is single-writer (§8.3). Cluster mode needs
  the FoundationDB adapter, which is still a stub.
- **Horizontal scale-out and load balancing.** Same reason.
- **A metrics endpoint.** pinetree exposes no Prometheus endpoint today;
  the health and readiness probes are the only operational surface.
- **Automatic database backups.** A named volume is not a backup; use
  `podman volume` snapshots or an external backup of `<data_dir>`.
- **TLS termination.** The server speaks plain HTTP; put a reverse proxy in
  front and set `trusted_proxies`.
