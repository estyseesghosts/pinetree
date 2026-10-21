# 2026-10-20 — containerization guide + graceful shutdown + readiness probe (pinetree)

Scope: one source change (container stop handling and a readiness probe)
plus the implementation guide `pinetree_container.md`.

## Goal

Package pinetree as a container image, and make the two code changes that
packaging depends on:

1. the server must stop cleanly on `SIGTERM`, the signal a container runtime
   sends on `docker stop`;
2. the server must expose a readiness probe that reflects storage health,
   not just process liveness.

## Reference findings (read, not assumed)

- **No `SIGTERM` handling existed.** `src/main.rs` ended in a bare
  `axum::serve(listener, ..).await` with no `with_graceful_shutdown`, so the
  default `SIGTERM` disposition killed the process mid-request: in-flight
  responses cut, streaming sockets dropped without a close frame, and a
  claimed-but-unsettled job left to expire its lease. This is the normal
  stop path for a container, so it had to be fixed before packaging.
- **`GET /healthz` already existed** (`src/api/misskey/endpoints/mod.rs:149`,
  liveness, `200 ok`). There was **no** readiness endpoint.
- **`curl` is an external runtime binary, not a crate dependency.**
  `src/federation/delivery.rs:712` spawns `curl` for every HTTPS federation
  request. Plain HTTP uses a raw `TcpStream` path. The curl argument builder
  (`curl_args`, `src/federation/delivery.rs:448`) deliberately keeps TLS
  verification on and uses `--resolve` (DNS pinning, SSRF defence), `--raw`
  (pinetree parses the chunked framing itself), `-q` (ignore `~/.curlrc`),
  `--noproxy '*'`, `--max-redirs 0` and `--max-filesize`. Minimum curl
  version: `--raw` 7.16.1, `--max-filesize` 7.19.0, `--noproxy` 7.19.4,
  `--resolve` 7.21.0 — Debian bookworm ships 7.88.
- **`listen` defaults to `127.0.0.1:3000`** (`src/config.rs:72`), which is
  unreachable from outside a container. The image must pin `0.0.0.0:3000`.
- **`public_url` defaults to `https://example.local`** (`src/config.rs:70`);
  every actor URI, WebFinger answer and NodeInfo document is built from it.
- **redb is single-writer** (exclusive lock on `pinetree.redb`): one replica
  per volume, no scale-out.
- **`mode: cluster` is rejected at startup** (`src/config.rs:94`), and
  `src/persistence/foundationdb/mod.rs` is still a **0-byte file** — the
  `fdb` cargo feature is declared but unused. The image must not enable it.
- **`InstanceSettingsStore::get()`** (`src/application/ports.rs:2651`) is a
  read-only single-row lookup in the redb adapter
  (`src/persistence/redb/mod.rs:3292`) — cheap, side-effect free, and it
  exercises the same read path the API uses. Suitable as a readiness probe.
- **`PINETREE_ALLOW_PRIVATE_IPS=1`** (`src/federation/delivery.rs:148`) skips
  only the private-IP rejection; TLS verification is unaffected. Needed for
  container-to-container federation, which is otherwise refused by the SSRF
  guard because compose addresses are private.
- No `env:` indirection in the config, by design — pinetree's config carries
  no secrets.

## Changes

- **`src/shutdown.rs`** (new): `signal()` resolves on `SIGTERM` or `SIGINT`.
  `terminate_signal()` degrades to "never" if the handler cannot be
  installed, so a failed registration never exits the server on a signal it
  never saw. Includes a unit test that the wait does not resolve without a
  signal.
- **`src/lib.rs`**: `pub mod shutdown;`.
- **`src/main.rs`**: the job worker is spawned with `spawn_until(..)` and a
  shared `Arc<AtomicBool>` stop flag; the serve future gets a
  `with_graceful_shutdown` arm that sets the flag and then drains. The
  worker stops claiming jobs while the HTTP server finishes its in-flight
  requests.
- **`src/jobs/worker.rs`**: `JobWorker::spawn` keeps its signature and
  delegates to the new `spawn_until(..)`, so the five existing call sites
  (`src/main.rs`, `src/api/misskey/endpoints/mod.rs` ×2,
  `tests/streaming.rs`, `tests/emoji_streaming_transfer.rs`) are untouched.
  A job claimed but not settled keeps its lease; the lease expires and the
  next process reclaims it — no work is lost across a restart.
- **`src/api/misskey/endpoints/mod.rs`**: new `GET /ready` route and
  handler. `200 ready` when `instance_settings.get()` succeeds, `503 not
  ready` otherwise (and the error is logged).
- **`pinetree_container.md`** (new, repo root): the implementation guide —
  the code changes above with their diffs, the required test coverage
  (`GET /ready` returns 200; `SIGTERM` exits with status 0, not 143/137),
  the `.dockerignore`, the full `Dockerfile` source, the runtime config, the
  compose file, the runtime gotchas (curl, `listen`, the redb lock, the
  data volume, private-range federation, memory, logging), an executable
  verification checklist, a troubleshooting table and security notes.
- **Contract bookkeeping (documentation gap, found by this work):** neither
  `/healthz` nor `/ready` is listed in `docs/endpoint-manifest.md`, even
  though the manifest is the endpoint contract and `AGENTS.md` requires
  every route to be pinned there. The guide gives the two manifest rows to
  add. `/healthz` is a pre-existing omission; `/ready` is new.

## Evidence

- `cargo check --all-targets --locked` — **green** on the change set.
- `cargo test --locked --no-fail-fast` — **698 passed, 1 failed**:
  - lib 502 ✓, conformance 119 ✓, streaming 16 ✓, emoji_admin_bulk 14 ✓,
    batch_q_phase1_app 10 ✓, federation 7 ✓, notes_like_contract 6 ✓,
    emoji_streaming_transfer 6 ✓, quote_fixtures 5 ✓, batch_q_phase1_fed 6 ✓,
    crash_recovery 2 ✓, federation_elixir 2 ✓, client_profile 1 ✓,
    federation_interop 1 ✓, minrepro 1 ✓;
  - **FAILED: `client_smoke_live_binary_and_misskey_js`** — a **pre-existing
    environmental failure, not caused by this change**. The test shells out
    to `scripts/smoke-phase1.sh`, which requires the git-ignored Misskey
    reference checkout at `misskey/packages/misskey-js/src/api.ts`; that
    symlink target does not exist in this environment. The script's own
    guard reports it directly:
    `SMOKE_FAIL: misskey checkout missing at /root/pinetree/misskey/packages/misskey-js/src/api.ts`
    (exit 1). Reproduced by running the script standalone, independently of
    the test harness. Nothing in this session's change set touches the
    smoke script, the binary's startup path, or the API surface it drives.
- The host environment was prepared for this work: Rust 1.99.1 (rustup,
  stable), gcc 16.2.1, OpenSSL 3.5.9 headers, `podman` 5.8.9,
  `podman-compose`, `buildah`, `skopeo`, Node 22.23.1, Ruby 4.0.7, Elixir
  1.19.6 (OTP 26), PostgreSQL 18.6, Valkey 9.0.6 with `redis-server`
  compatibility symlinks, ImageMagick, vips 8.18.3, jq, shellcheck. The
  `fdb` cargo feature was **not** enabled.
- **The image in `pinetree_container.md` §7.2 was built and run.** The
  Dockerfile in the guide was extracted verbatim and built with
  `podman build --network=host` against the repo context — it builds
  unmodified. The running container answered:
  - `GET /healthz` → `ok`
  - `GET /ready` → `ready` (the new endpoint, working in the container)
  - `GET /nodeinfo/2.0` → a valid NodeInfo document
- **`SIGTERM` verified against the real container:**
  `podman stop --time 30` produced **exit code 0** and the log line
  `shutdown signal received; draining in-flight requests`. Without the §4
  change the default `SIGTERM` disposition would have produced 143 — the
  container would have been killed mid-request.
- **Podman gotcha found and documented:** podman's default **OCI** image
  format silently drops `HEALTHCHECK`
  (`HEALTHCHECK is not supported for OCI image format and will be
  ignored`). The image must be built with `--format docker` for the
  image-level healthcheck to exist. `docker build` is unaffected, and a
  compose file that declares its own healthcheck is unaffected. Added to
  the guide's §11.1.
- **The image healthcheck was verified end-to-end** on the docker-format
  build: `podman inspect --format '{{.State.Health.Status}}'` → **`healthy`**,
  with the healthcheck log showing `0: ok`.
- **Measured image size: ~105 MB.**

## Open questions / next step

- The readiness probe's **failure** path (`503 not ready`) is not covered by
  a test: it needs a store whose reads fail, which the in-memory test
  doubles do not model. The success path is covered by the guide's §6.1
  test, which still has to be written.
- The `SIGTERM` subprocess test (guide §6.2) still has to be written. It
  must assert exit status **0**; a `SIGTERM` disposition that is not
  handled yields 143, and a drain timeout yields 137.
- The `Dockerfile`, `.dockerignore`, `docker/pinetree.container.json` and
  `compose.yaml` do **not** exist yet — the guide gives their full
  contents, so the next step is transcription plus the §11 verification
  pass (which builds the image with `podman`).
- `compose.yaml` was written against the compose spec but not executed:
  the host has `podman-compose`, not a `docker` daemon. §11 uses plain
  `podman` commands.
- Unrelated observation, not acted on: the guide's `stop_grace_period: 30s`
  is a guess. It should be tuned against a loaded node once the image runs.
