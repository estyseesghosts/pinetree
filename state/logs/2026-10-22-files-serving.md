# 2026-10-22 public file and proxy serving (adversarial-2 slice 3)

Covers finding 3, finding 5 (hit path), finding D. Read slice 1's
`fetch.rs` guard and coordinated with it; did not rewrite it.
Not a production-readiness claim.

## Changed

- `src/api/misskey/endpoints/files_http.rs`
  - `serve_file` no longer does blocking work on a Tokio worker. Metadata
    lookup, LRU touch, and the cold refetch run in `spawn_blocking`
    (`serve_file_prepare`); the blob open (stat + sidecar check) runs in a
    second `spawn_blocking` (`open_served_blob`); the response is built on
    the worker from already-resolved state.
  - No more whole-file `Vec<u8>` buffering on the serve path: when the pool
    offers `BlobStore::open_reader`, the response streams 64 KiB chunks
    from a Tokio file through an `http_body_util` channel body (4-chunk
    backpressure window). A mid-stream read error aborts the body rather
    than truncating silently. Backends without a streaming path (in-memory
    doubles) fall back to `get` off the worker.
  - Cold refetches take a shared non-blocking slot
    (`MAX_CONCURRENT_FILE_FETCHES = 8`) BEFORE any cache reservation
    exists; exhaustion answers Misskey-shaped 429 `RATE_LIMIT_EXCEEDED`
    with `Retry-After: 5`, so nothing leaks (slice 1 guard untouched).
  - Refetch responses use the REFRESHED `mime`/`blob_key` for
    `Content-Type` and `apply_media_security_headers`, not the pre-fetch row.
  - Hit path (bytes present) calls `remote_cache_touch(source_url,
    variant, now_ms)` before returning, so `last_access_ms`,
    `access_count`, and `generation` move. No touch on a miss that failed.
  - `proxy_media`: cheap validation (UA, `?url=`, scheme) stays on the
    worker; the SSRF-guarded `federation_http.send` with the unchanged 8
    MiB cap runs in `spawn_blocking` under the SAME slot limit, so proxy
    fetches share the bound with `/files` refetches. Origin failure keeps
    the plain-404 contract; slot exhaustion is 429 with `Retry-After`;
    `spawn_blocking` join failure is 500. Inline-safe allowlist, SSRF
    client, UA loop protection, and cache-control all unchanged. No Range
    parsing added.
- `src/media/storage.rs`
  - New `FsBlobStore::open`: sidecar-checked streaming open returning
    `Option<std::fs::File>` without reading content. Same corruption rule
    as `get`/`byte_len`: missing/mismatched sidecar reads as `None`, never
    short bytes. Wired as `BlobStore::open_reader`.
  - New unit test `blob_store_open_streams_without_reading_content`.
- `src/application/ports/blob_store.rs`
  - New `BlobStore::open_reader` with a default `Ok(None)` (no streaming
    path; callers fall back to `get` off the worker). Only `FsBlobStore`
    overrides it. No other implementor changed.
- `Cargo.toml` / `Cargo.lock`
  - `http-body-util` gains `features = ["channel"]` (the channel body used
    for file streaming). Lock delta is exactly `tokio` added to
    `http-body-util`'s deps; no new crate, no version change.
- `tests/files_http_cache_hit.rs` (new; shared test files untouched)
  - Black-box through the real router (loopback server + mutable canned
    origin, `FsBlobStore` pools), so no handler internals are coupled.
  - `file_hit_touches_cache_record`: hit serves bytes + security headers
    and bumps `access_count`/`generation`.
  - `refetch_with_changed_mime_serves_new_type`: stale pre-fetch row says
    `application/octet-stream`, first cold fetch serves `image/png`; after
    the origin flips to text and cached bytes are dropped, the refetch
    serves `text/plain` with updated metadata (would have stayed
    `image/png` from the stale row before the fix).
  - `proxy_serves_allowlisted_media_and_refuses_active_content`: 200 +
    headers for PNG; 400 for `text/html`; 400 without UA; 403 for a
    `misskey/` UA (allowlist/SSRF surface unchanged).
  - `slot_exhausted_cold_fetch_is_retryable_without_reservation_leak`:
    adaptive (parks gated fetches until the first immediate 429, no
    hardcoded slot count). Rejected probe is 429 `RATE_LIMIT_EXCEEDED`
    with `Retry-After`, never reaches the origin, and `/proxy` is 429 too
    while exhausted (shared limit). Parked fetches each hold exactly one
    max-size reservation; after release all complete 200 and
    `in_flight_bytes == 0`.

## Verified (native Windows cargo, `CARGO_TARGET_DIR=C:\Users\julie\.cargo\shared-target`)

Test binary mtime (19:53:19) is newer than the last source edit, so the
green run below is on fresh code (`Compiling pinetree` in the log).

```text
cargo test --locked --offline --test files_http_cache_hit
test proxy_serves_allowlisted_media_and_refuses_active_content ... ok
test file_hit_touches_cache_record ... ok
test refetch_with_changed_mime_serves_new_type ... ok
test slot_exhausted_cold_fetch_is_retryable_without_reservation_leak ... ok
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.20s
```

Regressions (same env, all green):

```text
cargo test --locked --offline --lib tests_drive
test result: ok. 11 passed; 0 failed (MemBlobs fallback path: upload/serve
roundtrip, security headers, slot 429, caps)

cargo test --locked --offline --lib storage
test result: ok. 11 passed; 0 failed (incl. new open-streaming unit test)

cargo test --locked --offline --test cache_reservation_accounting --test cache_evict_starvation
test result: ok. 5 passed (reservation) + 7 passed (eviction); 0 failed
```

`cargo fmt --check` is clean (formatted only the two touched files with
`rustfmt --edition 2024`; no other worktree file modified).

## Coordination notes (for the parent merge)

- Slice 1's `fetch.rs` guard was read and relied on, not rewritten: the
  slot is acquired before `fetch_on_first_request`, so exhaustion exits
  before `reserve_cache_bytes`; fetch failures release via the slice 1
  single path. The parked-fetch assertion (`in_flight == parked * max`)
  plus post-release `in_flight == 0` pins both sides.
- `endpoints/mod.rs` (`ApiState`, router) was NOT edited: the slot budget
  is a module-private static in `files_http.rs`, so no shared-file merge
  is needed from this slice. If the parent later wants the bound in JSON
  config, `MAX_CONCURRENT_FILE_FETCHES` is the single knob to wire.
- Left alone: inline-safe MIME set, SSRF checks, proxy 404 contract,
  `RUST_MIN_STACK`/worker count, Range support, tombstone/reconcile
  paths, upload slots.
