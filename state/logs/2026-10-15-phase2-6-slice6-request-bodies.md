# Phase 2.6 Slice 6 — Request bodies (H1)

Date: 2026-10-15. Spec: `docs/phase2-6.md` §§68–72. Status: implemented and verified (full `cargo test` green; live `smoke-phase2.sh` not runnable here — see §5).

## What changed

- `src/api/misskey/endpoints/mod.rs`
  - Removed the router-wide `.layer(DefaultBodyLimit::disable())` (was covering
    `/healthz`, `/files/*`, `/miauth/*`, `/signup`, `/signin`,
    `/api/drive/files/create`, `/api/{*endpoint}`). Everything except the
    upload handler now keeps axum's 2 MiB default extraction limit; federation
    routes (merged after) were already bounded (64 KiB inbox cap) and are
    unaffected.
  - New `MAX_JSON_BODY_BYTES = 1_048_576` (1 MiB), enforced at the top of
    `api_dispatch_blocking` — i.e. inside the Misskey JSON dispatch path, not
    in a router layer — so oversized JSON answers 413
    `MAX_FILE_SIZE_EXCEEDED` (same constructor the inbox `payload_too_large()`
    413 pattern maps to) even when dispatch is called directly.
  - New `UPLOAD_OVERHEAD_BYTES = 1_048_576`; `upload_cap_bytes()` =
    `max_file_size + 1 MiB`. `drive/files/create` reads the request body via
    new `read_capped_body()` (`http_body_util::Limited` + `BodyExt::collect`),
    which aborts 413 as soon as the cap is crossed without ever holding more
    than ~cap bytes. The old `to_bytes(body, usize::MAX)` is gone.
  - Auth ordering in `drive_files_create`: when an `Authorization: Bearer`
    token is present, `authenticate` + `write:drive` permission check +
    `drive/files/create` rate-limit run BEFORE any body byte is read. With no
    header token, the bounded streaming read runs first, then the multipart
    `i` field (the misskey-js shape) or JSON `i`/cookie authenticates exactly
    as before, followed by the same permission + rate-limit checks.
  - New `ApiState::upload_slots: Arc<UploadSlots>`; `test_state()` updated.
- `src/api/misskey/response.rs`
  - New `UploadSlots`/`UploadPermit` (decision D-c, see below) beside
    `RateLimiter`. Non-blocking `try_acquire` on both semaphores; exhaustion
    returns Misskey-shaped 429 `RATE_LIMIT_EXCEEDED` (with `Retry-After`).
- `Cargo.toml`: `http-body-util = "0.1"` promoted to a direct dependency
  (was transitive 0.1.5, same version — no lock churn expected);
  `futures-core = "0.3"` added under `[dev-dependencies]` for the
  counting-stream test only.
- `src/main.rs`, `tests/federation.rs`: construct `ApiState` with
  `UploadSlots::with_defaults()`.
- No MIME-sniffing, `serve_file` header, or SHA-256 changes (slices 7/8
  scope untouched). No frozen-contract edits: behavior stays within what
  `docs/endpoint-manifest.md` already documents for `drive/files/create`
  (`MAX_FILE_SIZE_EXCEEDED` 413).

## Decision D-c (numbers picked and why)

- `MAX_CONCURRENT_UPLOADS_PER_IP = 2`, `MAX_CONCURRENT_UPLOADS_GLOBAL = 16`,
  `UPLOAD_BUSY_RETRY_AFTER_SECS = 5`.
- Why: each upload buffers up to `max_file_size + 1 MiB` (~251 MiB worst case
  at the 250 MiB default) in memory while parsing multipart, so concurrency
  is the real memory bound. 2 per IP stops one client (all unauthenticated
  uploads share the `"unknown"` key via the existing `client_ip()`) from
  hogging the server while still allowing a retry alongside one in-flight
  upload; 16 globally caps worst-case transient buffering on a single node
  while leaving headroom for legitimate concurrent clients. `Retry-After: 5`
  because uploads are long-lived — a 1 s hint would hot-loop. Permits are
  held for the whole handler and release on drop, including error paths.
- Known caveat (accepted for MVP): per-IP semaphore entries are never evicted
  from the map, so distinct IPs accumulate small entries over time. Same
  class of issue as the existing `"unknown"` rate-limit key; revisit with
  slice 9 auth throttling.

## Tests (written FIRST, before the implementation edits)

New regression tests:

- `src/api/misskey/response.rs`: `upload_slots_cap_per_ip_and_release_on_drop`,
  `upload_slots_global_cap`.
- `src/api/misskey/endpoints/mod.rs` (with a `CountingChunks`
  `futures_core::Stream` that records every byte pulled):
  - `oversized_upload_rejected_without_buffering` — streaming body of
    cap + 5 MiB through `drive_files_create` (Bearer header path) must 413
    `MAX_FILE_SIZE_EXCEEDED` while pulling ≤ cap + one 64 KiB chunk.
  - `drive_upload_header_auth_checked_before_body_read` — bad Bearer token +
    4 MiB streaming body must 401 with zero bytes pulled.
  - `oversized_json_body_returns_413` — 1 MiB + 1 JSON body to
    `api_dispatch_blocking("notes/create", …)` must 413.
  - `drive_upload_concurrency_limit_returns_429` — both per-IP slots held
    → 429 `RATE_LIMIT_EXCEEDED`.
- Existing coverage relied upon: `drive_upload_json_and_file_serving`
  (multipart `i` field + JSON upload + file serving — the misskey-js shapes).

## §5 — Verification (executed by reviewer; implementer subagent has no shell)

- **Test-first, with real failing output.** The 6 new tests were authored before
  the fix. Because the authoring subagent could not run `cargo`, the reviewer
  reconstructed the failing run honestly: the fix was backed up to
  `/tmp/opencode/slice6/`, enforcement temporarily neutered (unbounded
  `to_bytes` in `read_capped_body`, JSON-cap check forced false, early header
  auth forced `None`, `UploadSlots::new` given 1M permits), and the new tests
  run — all 6 FAILED (`0 passed; 6 failed`):
  - `upload_slots_*`: `called Result::unwrap_err() on an Ok value: UploadPermit {...}`
  - `oversized_json_body_returns_413`, `oversized_upload_...`,
    `drive_upload_concurrency_...`: failed (see also the username bug below)
  - `drive_upload_header_auth_checked_before_body_read`: `left: 400, right: 401`
    (without pre-body auth the bad token falls through to post-body parsing)
  The neuter was then reverted from backup and the tests re-run: 7/7 green
  (6 new + pre-existing `drive_upload_json_and_file_serving`).
- **Two bugs the failing run caught, fixed before green:**
  1. `UploadPermit` lacked `Debug`, so `unwrap_err()` did not compile in the
     test profile (lib build was fine). Fixed with `#[derive(Debug)]`.
  2. Test usernames `uploader-cap`, `json-cap`, `uploader-busy` are invalid
     local usernames (hyphens rejected → signup 400). Renamed to
     `uploader_cap`, `json_cap`, `uploader_busy`.
- **Full `cargo test` (foreground + background runs, 2026-10-15):**
  - `--lib`: 135 passed, 0 failed (129 pre-existing + 6 new)
  - `--test conformance`: 43 passed, 0 failed
  - `--test crash_recovery`: 2 passed; `--test federation`: 2 passed;
    `--test federation_interop`: 1 passed; `--test minrepro`: 1 passed
  - `--test client_smoke`: 1 passed (`SMOKE_OK` via `scripts/smoke-phase1.sh`
    — the live misskey-js `drive/files/create` multipart-`i` upload + `GET
    /files/:id` byte-compare + `notes/create` with `fileIds` all green, so the
    late-auth upload path still works)
  - **Grand total: 185 passed, 0 failed** (baseline before slice: 179; +6 new).
- **NOT run (environment):** `scripts/smoke-phase2.sh` (needs Node 22 +
  Docker; same sandbox limitation as slices 1–5). Do not claim `SMOKE_OK`.

## Commit

Committed as `phase2-6 slice 6: ...` with
`Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`. The three
pre-existing untracked logs (`slice1-framing`, `slice2-key-ownership`,
`slice3-inbox-urls`) were `git add`ed into the SAME commit, plus this log.
