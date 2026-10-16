# 2026-10-16 — Five-finder audit + fixes (supplements Phase 3 exit)

Owner asked for a thorough audit (completeness, functionality,
correctness, code quality, missed issues) via finder subagents. All five
ran read-only; this log records the triage and the fixes applied on top
of the green Phase 3 tree (`cargo test` 283 green before starting).

## Completeness — clean, no fixes

Every profile §2/§3 row verified present; every §5-out item returns its
documented error; no scope creep (only extra: `receiveFollowRequest`
main event — Misskey-native, kept).

## Fixed (all verified below)

1. `notes/search`, `notes/search-by-tag` were credential-gated; Misskey
   has `requireCredential: false`. Now public; anon viewers get only
   `is_visible` notes. Logged-out web search/tag pages work.
2. `notes/search` corpus defaulted local-only; Misskey default (no
   `host`) is global. Now: absent `host` = all cached hosts, `"."` =
   local. Still visibility-filtered (anon sees public, same as
   global-timeline).
3. `notes/search` gained `offset` (pre-limit) + `rangeStartAt/EndAt`
   (ms bounds, same as `sinceDate/untilDate`) — Misskey web sends all
   three; they 400d before. `channelId` still rejected (no channels).
4. `users/notes` `withReplies+withFiles` now
   `BOTH_WITH_REPLIES_AND_WITH_FILES` (Misskey code + id
   `91c8cb9f-…`), and unknown `userId` is `NO_SUCH_USER` (was `[]`;
   the old code comment claiming no such error was wrong).
5. `drive/files` `sort` honors all six Misskey variants (with Misskey's
   inverted `+createdAt→newest` mapping); non-null `folderId` still
   rejected (no folder model).
6. `pack_note` emits optional `uri`/`url` (+ always-null `channel`).
7. `pack_notification` emits `note` only on note-bearing variants and
   `reaction` only on the reaction variant (was unconditional `null`/`""`).
8. `i/notifications` requires `read:notifications` (was `read:account`).
   This broke `smoke-phase1.sh` part 2 (its fixture token lacked the
   scope) — fixture `PERMS` extended, correctly: real clients request
   the scope Misskey documents.
9. Inbound `Undo(Follow)` now enqueues `Unfollowed` and inbound `Accept`
   enqueues `MeUpdated{ followee }` (both were silent; mirrors local paths).
10. Inbound note creation dedups `Notify` per user (reply+mention double
    frames fixed; mirrors local `already_notified`).
11. Main-channel `mention`/`reply`/`renote` frames re-gate at socket time
    via `notes_show` (deleted/narrowed notes no longer ship stale packs).
12. `server.rs` timeline fan-out is a single eligibility pass (was two).
13. Hub `Unfollowed` arm diagnoses instead of vanishing (`eprintln`;
    worker interception remains canonical).
14. `job.attempts.saturating_add`, `TimelineMaterialize` no-op comment,
    deleted empty `streaming/{connection,channels,subscriptions}.rs` stubs.
15. Agent-owned `phase3_*` tests updated to the tightened contract
    (BOTH guard, sort variants, public search, `NO_SUCH_USER`).

## Verified kept-as-is (audited, rationale logged)

- Public follow lists (no visibility fields exist and no client can set
  non-public; Misskey gate noted in profile §3).
- `tokenId` revoke no-op, empty-query 400, `tag`-only search-by-tag (no
  target client sends `query[][]`; tag.vue sends `{tag}`), `notes/renotes`
  + conversation Misskey-exact `NO_SUCH_NOTE`/empty semantics,
  `users/search` empty-wildcard + offset (Misskey `LIKE '%%'` parity).
- MeUpdated-on-follow shape (follower not notified; harmless extra
  clients tolerate; tests pin the followee one).
- Crash-between-insert-and-push hint loss + StreamingPublish replay
  duplicates (ephemeral hints; fresh-insert-only minimizes duplicates;
  clients refetch on reconnect).
- 1536-evict refcount drop (Misskey-identical structure).
- Read rate buckets, follow-set caching, `list_all_notes` indexes →
  Phase 5 scale work (correct today, unoptimized).
- Suspended-token WS 403, caps/idle-reap live tests, remote-notes
  streaming, search host/userId filter smokes → still uncovered (same
  standing note as Phase 3 exit).

## Evidence (executed 2026-10-16)

- `cargo test --offline`: 211 lib + 51 + 2 + 1 + 1 + 2 + 1 + 1 + 13 = 283
  green (3 agent tests updated mid-batch, all passing).
- `scripts/smoke-phase1.sh` → `SMOKE_OK` (after PERMS fix).
- `scripts/smoke-phase3.sh` → `SMOKE3_OK`.
- `scripts/keystone-sweep.sh` → `KEYSWEEP_OK` 41/41.
- Profile extended with superseding §7 (logged decision mechanism for
  the frozen doc).
