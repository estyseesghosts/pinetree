# 2026-10-16 — Phase 3 REST endpoint coverage (profile §3 table)

Implements every REST row of `docs/phase-3-profile.md` §3 (frozen profile;
no profile changes). Streaming worker built `src/streaming/*` concurrently;
shared files (`ports.rs`, `queries.rs`, `redb/mod.rs`, `packers.rs`,
`endpoints/mod.rs`, `events.rs`) were edited by both sides — all edits here
are additive and `cargo test --offline` is green at handoff.

## What changed

**Ports (`src/application/ports.rs`)** — three required methods, no defaults:
- `MediaStore::list_by_owner(owner_id)` (newest-first, id-ordered).
- `NotificationStore::list_notifications_filtered(notifiee, cursor,
  include, exclude)`; old `list_notifications` kept (no remaining
  non-test callers, retained for compat).
- `AuthStore::delete_token_by_hash(token_hash)` (missing row = no-op).
- Impls: `redb` (real) + the two in-memory test doubles
  (`packers.rs`, `webfinger.rs`).

**Queries (`src/application/queries.rs`)** — all bounded-scan
(`list_all_notes` 10k) + `is_visible` + `apply_cursor`, reusing the
timeline patterns: `user_notes`, `note_replies`, `note_renotes`
(`NO_SUCH_NOTE` on unknown target, Misskey parity), `note_children`
(replies + quotes with text/files; no `hasPoll` disjunct — no polls exist),
`note_conversation` (oldest-first; full depth-guarded walk, invisible
ancestors skipped but walked through), `search_notes` (case-insensitive
substring over text+cw; corpus = local authors by default, `host="."` =
same local scope, other `host` = exact cached-host match),
`notes_with_tag` (case-insensitive exact tag), `user_followers` /
`user_following` (bounded fetch + id/date cursor, newest-first),
`mark_all_notifications_read` (store flip + synchronous
`push_main(ReadAllNotifications)`; co-located here to avoid a new module).

**Commands** — `notes.rs::unrenote` (deletes all caller's renotes incl.
quotes via the existing `delete_note` path, returns count, idempotent);
`media.rs::get_media_for` / `update_media` (same-id `insert_metadata`
upsert; Misskey `validateFileName` parity) / `delete_media` (metadata +
blob bytes) / `list_drive_files` (owner + MIME-prefix + cursor).

**Packers** — `pack_following()` → `{id, createdAt (from follow id via
`ids.parse_time`), followerId, followeeId, follower, followee}` with both
sides as `UserDetailedNotMe` (null when the row is gone).

**Endpoints (`endpoints/mod.rs` dispatch only)** — exact allowed-field
lists (`check_allowed_fields`; `offset`/`channelId`/`rangeStartAt|EndAt`
on search correctly fall out as `UNSUPPORTED_FIELD`):
`users/notes`, `notes/replies|renotes|children|conversation`, `notes/unrenote`
(`write:notes`, shared `notes/delete` bucket), `users/followers|following`
(public lists, no `FORBIDDEN`), `drive/files` (`read:drive`, non-null
`folderId` → `INVALID_PARAM`, `type` pattern-checked prefix match,
non-`-createdAt` sort → `INVALID_PARAM`), `drive/files/show|update|delete`
(owner-only → `NO_SUCH_FILE`; bad names → `INVALID_FILE_NAME`),
`i/notifications` (default limit **10**, cursor, `markAsRead=true`,
include/exclude with obsolete-type dropping), `notifications/mark-all-as-read`
(`write:notifications`), `i/revoke-token` (no-params → revoke caller
token; empty/unknown → no-op; third-party can only revoke itself), `notes/search`
+ `notes/search-by-tag` (`read:account`).
Rate limits reuse existing buckets only (`response.rs` is outside this
workstream's edit scope, so no new buckets were added):
unrenote → `notes/delete`, drive update/delete → `drive/files/create`;
mark-all-as-read and revoke-token are unthrottled (documented choice).

## Test evidence

`cargo test --offline` green at handoff: **211 lib** (incl. 10 new
`phase3_*` endpoint tests covering happy path + key errors + pagination
for every new endpoint, plus 1 new `pack_following` packer test) and all
integration suites green (51 + 2 + 2 + 1 + 1 + 8). Three
write-then-verify bugs were caught by the new tests and fixed (empty
`includeTypes` presence check, conversation `limit` early-break, 1 s
`notes/delete` bucket interval in the unrenote test). No new compiler
warnings attributable to this work.

## Open questions / deviations from profile (all intentional, none silent)

1. `i/revoke-token` `tokenId`: accepted but always no-op success — pinetree
   tokens are hash-addressed and no token ids are ever issued, so no
   `tokenId` can resolve (mirrors Misskey's unknown-target no-op).
2. `notes/conversation` is oldest-first per the profile (Misskey returns
   nearest-first); `offset`/`limit` apply over the visible chain.
3. `users/notes` applies `withReplies`+`withFiles` literally when both are
   set; Misskey's `BOTH_WITH_REPLIES_AND_WITH_FILES` error is not raised.
4. `notes/search` empty-string `query` is `INVALID_PARAM` (Misskey would
   accept it); default corpus is local-only per the profile.
5. Non-owner drive access answers `NO_SUCH_FILE` per the profile (Misskey
   answers `ACCESS_DENIED`; no moderator bypass exists here).
6. `mark_all_notifications_read` fans out via synchronous
   `StreamingSink::push_main` (default no-op; broadcast hub overrides) —
   no new `StreamingEvent` variant was added, deliberately, so the
   streaming worker's exhaustive `hub.rs` match keeps compiling.
   Streaming side must route `MainEventKind::ReadAllNotifications`
   (already defined in `events.rs`) to `main`-channel
   `readAllNotifications` bodies.
7. `docs/endpoint-manifest.md` was left untouched (concurrent edits in
   flight); the frozen profile required no changes.
