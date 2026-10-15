# Phase 2.6 Slice 7 — Media safety (H2 + C3)

Date: 2026-10-15. Author: Beeline (Muse Spark 1.3 Contributor).
Status: implemented and verified — full `cargo test` green (see §Verification).

## What changed

One shared sniffer (`src/domain/media.rs::sniff_mime`, no caller fallback):
delete the `commands/media.rs` duplicate and the fallback-accepting
`media/ingest.rs` duplicate (ingest keeps `md5_hex` + `sanitize_filename` only).
Both local `upload_media` and `remote_cache` call the shared fn. Allowlist
(`image/png|jpeg|gif|webp|avif`, `video/mp4|webm|mpeg`, `audio/mpeg|ogg|wav`,
`text/plain` for non-markup UTF-8 only, else `application/octet-stream`;
never html/svg/xhtml/xml). `upload_media` drops its `mime` parameter —
declared types refine nothing. `remote_cache` ignores both `mediaType` and
the response `Content-Type` (old `:68-73` declared-beats-header precedence
deleted). `video/mpeg` for `0x000001BA` is a deliberate addition beyond the
slice allowlist text (honest type, still `video/*` inline-safe); any other
`ftyp` (incl. QuickTime `qt`) maps to `video/mp4`; avif brands checked first.

`serve_file` (`src/api/misskey/endpoints/mod.rs`): always
`X-Content-Type-Options: nosniff` + `Content-Security-Policy: default-src
'none'; sandbox`; `Content-Disposition: attachment` unless inline-safe
(`image/*|audio/*|video/*`; octet-stream and text/plain always attach, as do
legacy rows like `application/pdf`). Plus a global `nosniff` middleware
layer on the whole router (covers errors, redirects, identicons, AP routes).

Inbound Create hardening (`src/federation/inbox.rs`):
(a) read-only `ActivityDedup::is_recorded` gate at the top of `handle_create`,
before any fetch; `record_note_activity` stays the atomic authority (benign
race, documented in code).
(b) caps: **16 attachments/note** (reuses existing `MAX_FILE_IDS`, Misskey
parity) — **truncate** keeping document order; **64 MiB cumulative fetch
budget/note/job** in the worker (first attachment exempt so a single large
file still caches); per-file `max_file_size` unchanged.
(c) idempotent insert per (remote URL, note): new nullable
`MediaMeta.source_url` (+ `InsertMediaInput.source_url`); worker pre-checks
`MediaStore::find_by_source_url` ∩ note `file_ids` (zero network on retry),
and `NoteStore::attach_remote_media` re-checks inside its single
insert+append txn (closes the race; returns the existing row).

D-d outcome: **moved NOW** (not deferred). Inbox enqueues one durable
`JobKind::RemoteMediaFetch` job atomically with `record_note_activity`
(`note_id` via the existing `__PENDING_NOTE_ID__` in-txn substitution);
worker fetches + attaches via `AppServices::attach_remote_media_to_note`.
Per-attachment `Validation` (bad URL, non-2xx, oversize) is a best-effort
skip; storage faults propagate to worker retry; note-deleted maps to Done.
No fetch of any kind happens on the inbox request anymore.

## Tests (written first, in edit order)

- `domain::media` — `sniff_detects_image_and_av_magic`,
  `sniff_markup_never_becomes_text_or_xml` (html/svg/xml/BOM/whitespace),
  `sniff_utf8_is_plain_and_binary_is_octet`, `source_url_bounds`.
- `application::commands::tests` — `local_non_utf8_upload_claiming_html_is_not_html`,
  `local_upload_sniff_beats_declared_type`.
- `media::remote_cache` — `remote_html_and_svg_attachments_stored_as_octet_stream`
  (declared image/png + svg ignored), `remote_media_fetch_job_truncates_to_16`.
- `federation::inbox::media_safety_tests` — `replayed_create_triggers_zero_fetches`
  (0 hits after inbox, 1 hit after worker drain, still 1 after replay; 0 new
  jobs; note keeps 1 file), `create_with_too_many_attachments_truncates_to_16`.
- `api::misskey::endpoints` — `serve_file_sets_security_headers`
  (png inline + nosniff + CSP; txt/bin attach), `inline_safe_mime_set`.
- `tests/conformance.rs` Group M — `case_m1_media_source_url_lookup` /
  `m1_media_source_url_lookup`, `case_m2_attach_remote_media_idempotent` /
  `m2_attach_remote_media_idempotent` (FDB inherits both).

Frozen-contract notes: `docs/application-ports.md` (job kind + payload,
`is_recorded`, `find_by_source_url`, `attach_remote_media`),
`docs/domain-types.md` (`source_url`, MIME rule). No slice-8/9 scope touched
(no blob-key, password, or rate-limit changes).

## Numbers picked (D-c-style)

16 (truncate, Misskey parity) · 64 MiB cumulative/job (first exempt) ·
per-file `max_file_size` · `TOO_MANY_ATTACHMENTS` only reachable via crafted
jobs (worker pre-truncates).

## Open questions

- Single-file notes larger than 64 MiB followed by more attachments will
  cache only the first file; acceptable for MVP (logged).
- `fetch_and_cache_remote_media` (standalone, no note) is kept for its
  roundtrip test but has no production caller left; slice 8 may fold it.
- Identicon SVG keeps `image/svg+xml` inline, but the response now also
  carries `nosniff` + `Content-Security-Policy: default-src 'none'; sandbox`
  (same as served files). Rationale: the bytes are fully server-generated
  (SHA-256-derived hex colors/coordinates, no user input, no script
  elements), and a sandboxed document cannot execute script even on direct
  navigation, while `<img>` embedding keeps working. `is_inline_safe_mime`
  explicitly excludes `image/svg+xml` (and `*+xml`), so any legacy/stored
  SVG row serves as an attachment.
- Orphan *blob bytes* (no meta row) remain possible if the process dies
  between `BlobStore::put` and the attach commit — inert and content-
  addressed; same exposure as `upload_media`. Meta-row orphans are closed.

## Reviewer fixes (authoring subagent has no shell; all found by executing)

1. Test profile did not compile: `UploadPermit`-class misses — here
   `InsertMediaInput` missing `is_link` (`remote_cache.rs`) and a missing
   `IdGenerator` import (inbox `media_safety_tests`). Fixed.
2. `RedbStore::attach_remote_media` called `write_txn.abort()` while table
   handles borrowed it (E0505). Restructured: reads decode into owned
   values (guards dropped), idempotent-hit collected before acting, explicit
   `drop(table)` before every early `abort()`.
3. New tests caught two real sniff bugs: the bare 2-byte MP3 frame-sync
   rule mislabeled the `[0xFF, 0xFE, …]` binary fixture (also the UTF-16LE
   BOM) as `audio/mpeg` — replaced with header validation
   (`looks_like_mpeg_frame`: sync + nonzero version/layer + valid
   bitrate/sampling), plus positive MP3/ID3 assertions; and
   `is_inline_safe_mime("image/svg+xml")` was true — now excluded (see
   identicon note above).
4. The embedded-object replay test could not fail without the
   `is_recorded` gate (no inline fetch exists anymore), so a URI-object
   replay test was added (`replayed_uri_object_create_triggers_zero_note_fetches`,
   path-based JSON stub): without the gate the replay re-fetches (verified
   failing: hits 2 ≠ 1); with it, zero fetches and zero new jobs.
5. Pre-existing `tests/federation.rs::federation_two_node_end_to_end`
   asserted the attachment is cached synchronously through Create — the
   intended D-d change. Updated to `pump_until` workers drain the
   `RemoteMediaFetch` job (note carries the file afterwards).
6. `tests/crash_recovery.rs` still passed 7 args to the new 6-arg
   `upload_media`; fixed. Its bytes are ASCII → now `text/plain`
   (attachment); no MIME assertion exists there.
7. Warning hygiene: removed newly-unused imports (`Clock`/`IdGenerator` in
   `remote_cache.rs`; `JobQueue`/`MediaStore`/`NoteStore` in the new inbox
   test module). Remaining warnings are pre-existing (verified identical
   on HEAD for `commands/tests.rs`, `conformance.rs`, second inbox module).

## Failing-first evidence (reconstructed honestly by the reviewer)

New tests were authored before the fix, but the author could not execute.
The reviewer backed the tree up, neutered enforcement (markup guard off,
lax MP3 sync, `apply_media_security_headers` uncalled, 16-truncate off,
replay gate off), and ran the new tests: 8 failed
(`sniff_markup…`, `sniff_utf8…`, `local_non_utf8…`,
`local_upload_sniff…` (declared text/html survived), `remote_html…`,
`remote_media_fetch_job_truncates…`, `serve_file_sets…`,
`create_with_too_many…`), with the gate covered separately by the URI
replay test (fails neutered, passes fixed). Neuters reverted afterwards;
final tree contains no `TEMP-NEUTER` markers (verified by grep).

## §Verification — EXECUTED 2026-10-15

Baseline before this slice: `cargo test` 185 green (HEAD 68bcce7).

1. `cargo test --lib` — 148 passed, 0 failed (135 pre-existing + 13 new:
   4 domain sniff + 2 commands + 2 remote_cache + 3 inbox incl. the new
   URI-object replay test + 2 endpoints).
2. `cargo test --test conformance` — 45 passed, 0 failed (43 + M1/M2).
3. `cargo test` (full, `EXIT=0`): lib 148, main 0, client_smoke 1
   (`SMOKE_OK` via `scripts/smoke-phase1.sh` — live misskey-js upload still
   green under sniff-only MIME), conformance 45, crash_recovery 2,
   federation 2, federation_interop 1, minrepro 1.
   **Grand total: 200 passed, 0 failed** (baseline 185; +15).
4. Warning check: no new warnings. Remaining warnings verified
   pre-existing via `git log -S` (Phase 2 / phase2-5 era); two new-import
   warnings introduced during the slice were removed before commit.
5. NOT run: `scripts/smoke-phase2.sh` (needs Node 22 + Docker; same sandbox
   limitation as slices 1–6). Do not claim `SMOKE_OK`.

Compile-risk spots for the reviewer (no shell here, read extra carefully):
- `src/persistence/redb/mod.rs::attach_remote_media` — table-open
  discipline (NOTES/MEDIA_META/ACCOUNTS each once per write txn) and the
  `let _ = write_txn.abort(); return ...` pattern (abort consumes the txn).
- `axum::middleware::from_fn(nosniff_middleware)` layer after
  `.with_state(state).merge(federation_routes)` — state-type unification.
- `MediaMeta.source_url` `#[serde(default)]` — old rows without the field
  must still decode (m2 exercises new rows only; reopen tests cover old DB
  files — none predate the field in test fixtures).
- `__PENDING_NOTE_ID__` substitution inside the JSON fetch payload
  (string replace in `enqueue_jobs_in_txn`); attachment URLs containing
  that literal would corrupt — absurd, documented.
- `process_inbox(&services, ...)` calls in the new inbox tests pass
  `&Arc<AppServices>` (deref coercion) — verify.
