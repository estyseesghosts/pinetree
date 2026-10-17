# 2026-10-17 — bench fixes: P1 uri uniqueness + ap/show unification

Follows `docs/handoff-fediverse-bench-fixes.md` (steps 1–2).

## Changed
- `src/persistence/redb/mod.rs`: new `NOTES_BY_URI` index; uniqueness check + insert in
  `create_note_in_txn`; `record_note_activity` maps a duplicate uri to `Ok(None)`;
  `delete_note_in_txn` releases the uri; `find_by_uri` is O(1); `run_uri_index_backfill`
  at open (richest duplicate row wins).
- `src/federation/inbox.rs`: `ingest_fetched_remote_note`; quote-target caching now
  enqueues the media job (`fetch_media=true`).
- `src/api/misskey/endpoints/mod.rs`: `ap/show` note path calls the inbox ingest instead of
  a hand-built `CreateNoteInput` (fixes missing reply link / media job → P4, P5 at source).
- `tests/conformance.rs` Group U (3 tests); `docs/persistence-conformance.md` contract section.

## Verified
- `cargo test`: all suites green (conformance 86 incl. u1–u3; lib 390).
## Not verified / open
- Bench re-run (900-post storm, duplicate-uri sweep) and `scripts/smoke-all.sh` NOT run
  (need live rig). P3/P6 untouched; P2 (`notes/reactions`) not started.
- Existing duplicate rows are not deleted, only shadowed in the uri index.
