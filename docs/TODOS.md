# Pinetree TODOs — gaps vs Misskey

Status: Draft
Source: `docs/inventory-pinetree-vs-misskey-2026-10-20.md` (code inventory 2026-10-20, 7 scouts, `file:line` evidence) + full docs sweep 2026-10-20 (6 sweeps, every `docs/*.md` + wiki).
Scope: backend only. Items are missing/broken/divergent-followup; out-of-scope-by-design items are marked `[declared-out]` so a later decision can promote them.

Conventions: `- [ ]` = open, `- [x]` = done (move to log, don't delete the line). Each item cites where the gap lives. Phase-4 items are expected-empty, not regressions.

## P0 — multi-node (Phase 4, all empty stubs)

- [ ] FDB adapter: `src/persistence/foundationdb/mod.rs` empty; no `use foundationdb` in `src/`; pass conformance suite on FDB.
- [ ] Account directory/ownership: `src/cluster/account_directory.rs` empty; `get_owner` returns local default (`persistence/redb/mod.rs:3970-3981`).
- [ ] Request routing by owner: `src/cluster/routing.rs` empty.
- [ ] Secure internal cross-node protocol: `src/cluster/internal_protocol.rs` empty; enforce no-self-federation rule.
- [ ] Account moves/migration preserving ID+keys+content: only `moved_to_uri` profile metadata exists, no migration.
- [ ] Backend migration tool redb→FDB with downtime procedure (existing export/import is per-user portability only).
- [ ] Multi-worker jobs: lease protocol exists but poll-storm/`claim_next` contention, per-process `DestinationTracker` (`main.rs:330`), in-process `Hub` with no cross-node transport — design + test.
- [ ] Shared object storage (S3-compatible) to replace local `FsBlobStore`; shared rate limiting; readiness gates.
- [ ] Recovery/ownership-transfer/node-loss/stale-routing tests (proposal §8).

## P1 — media lifecycle + storage safety (single-node, broken/risky)

> WIP 2026-10-21 (storage overhaul, `docs/remote-media-cache-plan.md` addendum 2026-10-21): S1 shared contracts landed (config keys, ports, redb tables, dual pools, `drive/cache/stats`). Subagents building domain/storage/fetch_stream, commands/reconciler/remote-cache, and `tests/storage_*.rs` suites in parallel. Boxes below stay open until `cargo test --offline` + `bash scripts/smoke-all.sh` (incl. cap-never-exceeded, local-survives-eviction, cross-owner delete, crash-heal) are green.
>
> S3 2026-10-21 (finish pass) — DONE, gate green (`SMOKE_ALL_OK` 2026-10-21;
> log `state/logs/2026-10-21-storage-overhaul-s3.md`): `JobKind::CacheEvict|
> ReconcileStorage|BackfillDurableAccounting` + worker arms + `backfill_durable_accounting`
> + `set_blob_refcount`/`set_durable_bytes` + `durable_dir`/`cache_dir` overrides +
> reserved-sum guard + `tests/storage_two_pools|storage_cache_evict|storage_reconcile.rs`
> (10/10). Stack-overflow fix: explicit 8 MiB runtime threads + guardrail test
> (`thread_stack_size` in `main.rs`, `dispatch_fits_bounded_stack`). Live steps ran
> under WSL against `./misskey` → `../kissme/kissme-fe`.


- [x] Remote-media GC/quotas/eviction/reconciler — LANDED 2026-10-21 (S1+S2+S3): per-user durable quota (`DRIVE_CAPACITY_EXCEEDED`), refcounted shared blobs, global+class caps, LRU+TTL eviction (bytes-only), on-demand fetch with in-flight reservation, idempotent reconciler, 3 storage job kinds + worker arms, 22 storage tests green, gate `SMOKE_ALL_OK` (phase2 SKIP).
- [x] Shared-blob refcount: `delete_media` drops row+bytes non-atomically (`application/commands/media.rs:211-216`) — fix before eviction. — LANDED 2026-10-21: `delete_media_record` decrements in-txn, bytes deleted only at refcount 0 outside txn, reconciler sweeps the rest.
- [x] Stream-to-disk under byte cap (local upload + remote fetch hold full bodies in RAM: `media/remote_cache.rs:174,232-254`). — LANDED 2026-10-21: `fetch_stream.rs` stages to disk under cap with mid-stream abort + `import_file`; transport (`SafeHttpClient`) buffering is a documented follow-up, not a cap hole.
- [x] Backup/snapshot/restore for `pinetree.redb` + `blobs/` — LANDED single-node (Batch F): versioned ZIP online snapshot (single read txn + 103-table registry + both pools + sidecars, `.partial` staging + verify + atomic publish, shared media gate) + offline restore into fresh dir (validate → checksums → schema → counts → integrity → accounting → sidecars → RESTORE_READY.json); `tests/backup_snapshot_f1.rs` + `tests/backup_snapshot_f-fix.rs` green. Live Docker drill stays unqualified until provisioned host run.
- [x] `drive/files/find|move-bulk|check-existence|attached-notes` — LANDED 2026-10-21 (gaps WP2a final; manifest §43.1). Still OPEN: `attached-chat-messages` (no chat model, SKIP stays).
- [ ] Thumbnails/`Range`/avatar-emoji static variants (declared-out; `thumbnail_url` = own url until the thumbnail route exists). Transcode itself LANDED 2026-10-21 (WP1; WebM/VP9+WebP, toolchain gate; manifest §43.9).

## P1 — client-visible API holes (all → 404 `endpoints/mod.rs:9122` unless noted)

- [x] `drive/folders/*` + `drive` + `drive/stream` — LANDED 2026-10-21 (gaps WP2a final; real folder model, `folderId` non-null, `drive/stream`; streaming `drive` channel in §43.8; manifest §43.1). No longer declared-out.
- [ ] `[declared-out, SKIP 2026-10-21]` `channels/*` (16 endpoints). Code REMOVED from the tree (Misskey's implementation conflicts with pinetree's direction) — endpoints keep 404ing per the owner SKIP order (`docs/gaps.md:5`).
- [ ] `[declared-out, SKIP 2026-10-21]` `chat/*` (~25 incl. rooms/invitations). Code REMOVED from the tree — endpoints keep 404ing per the owner SKIP order.
- [x] `announcements(+/show)` + `i/read-announcement` + `promo/read` — LANDED 2026-10-21 (gaps WP2b final; badge clears on read; manifest §43.6).
- [x] `following/update|update-all|list` + `following/requests/sent` — LANDED 2026-10-21 (gaps WP2a final; `following/list` op + report-specified update/sent ops; manifest §43.2).
- [x] `hashtags/*` (list/search/show/trend/users) — LANDED 2026-10-21 (gaps WP2b final; manifest §43.6). No longer declared-out; `notes/search-by-tag` stays the note-search answer.
- [x] `notes/thread-muting/create|delete` — LANDED 2026-10-21 (gaps WP2a final; manifest §43.2). Manifest §11 "stays out" line superseded.
- [x] `i/2fa/*` (8) + `my/apps` + `app/create|show` — LANDED 2026-10-21 (gaps WP2c final; TOTP supported, WebAuthn 400 documented; manifest §43.3).
- [x] `users/update-memo`, `notes/show-partial-bulk`, `notes/polls/recommendation`, `notes/clips` (alias of `clips/notes`), `users/pages`, `users/lists/create-from-public` — LANDED 2026-10-21 (gaps WP2a final; manifest §43.2). Still OPEN from this line: `notifications/create`, `users/achievements|flashs|get-following-*-by-birthday|get-frequently-replied-users`.
- [x] Admin families: bulk/remote/copy/import-zip emoji (LANDED 2026-10-20) + `admin/roles|queue|relays|webhook|ad|announcements|federation|stats|user-ips|send-email|server-info|proxy-account|mfa|reset-password` + abuse-report notification recipients — LANDED 2026-10-21 (gaps WP2b/WP2c/WP3 final; `queue/*` = stats real + delay/clear loud 400; manifest §43.5/§43.7).
- [x] Misc `gallery/posts`, `users/pages`, `users/lists/create-from-public`, `ping`, `retention`, `reset-db` (loud 400), `sw/*`, `v2/admin/emoji/list`, `endpoint(s)` (+ `export-custom-emojis` LANDED 2026-10-20) — LANDED 2026-10-21 (gaps WP2a/WP2b/WP2c final; gallery/flash shapes in manifest §43.6). Still OPEN from this line: `users/achievements|flashs|get-following-*-by-birthday|get-frequently-replied-users`, `page-push` (not an identifiable Misskey endpoint, stays 404 truly-unknown).

## P1 — streaming holes

- [x] Missing channels — LANDED 2026-10-21 (gaps WP4 final; manifest §43.8): `hashtag`, `roleTimeline` (silent, DIVERGENCE), `drive` (bodies `{id}` until folder packer, DIVERGENCE), `serverStats`/`queueStats` (snapshot + `statsLog`, no 1 s broadcast, DIVERGENCE), `admin`, `clip`. Stay OUT by decision: `chatUser`/`chatRoom`, `reversi`/`reversiGame` (no model; REST unwired per SKIP), `channel`. All other listed channels were never missing.
- [x] Missing `main` events — LANDED 2026-10-21 (gaps WP4 final; manifest §43.8): all remaining keys (`unreadNotification`, `unreadAntenna`/`readAntenna`, `readAllAnnouncements`/`announcementCreated`, `myTokenRegenerated`, `signin`, `registryUpdated`, `driveFileCreated`, `urlUploadFinished`, `pageEvent`, `newChatMessage`) forwarded generically; owning-command emission rides the final-wiring commit.
- [x] Missing broadcast `emojiAdded/Updated/Deleted` — LANDED 2026-10-20: `StreamingEvent::EmojiBroadcast` + `push_broadcast` + `{type,body}`-to-all dispatch; contract tests in `tests/emoji_streaming_transfer.rs` (manifest §40.2).
- [x] `reacted` lacks optional `emoji` detail — LANDED 2026-10-21 (`{name, url} | null`, fail-open to null; manifest §43.8).
- [x] `userList` ignores `withFiles/withRenotes` — LANDED 2026-10-21 (both honored; manifest §43.8).
- [ ] `pollEnded`-as-`noteUpdated` is a pinetree extension stock clients ignore — the real `pollEnded` *notification* (`NotificationKind::PollEnded` + packer/receive-config + worker producers) rides the final-wiring commit (deferred in WP4; manifest §43.8).

## P2 — federation interop risks

- [x] `Update(Note)` string-URI — LANDED 2026-10-21: fetch-and-apply (Sharkey parity; replay gate → SSRF/size/origin guards → `attributedTo == signer` + exact-authority → shared tail; manifest §43.10).
- [ ] Inbound poll shape gate degrades to poll-less (`inbox.rs:2217-2247`) — exotic polls render as plain notes; decide parity vs document.
- [ ] Stamp fetch is pre-txn network I/O (`inbox.rs:2341-2366`) — slow/quoted-host outage delays `Create`; consider defer/re-verify path only. (Negative stamp dedup landed WP5; the pre-txn placement decision stays.)
- [x] `Undo(Accept)` by-ID — LANDED 2026-10-21: minimal `ActivityIdIndex` (`record/resolve/forget`); embedded shape keeps the same tear-down path (manifest §43.10).
- [x] Generic `Add/Remove` + `Tombstone`-typed activities — LANDED 2026-10-21: per-Misskey-dispatch handling + `Delete(Tombstone)` recognition + documented relay refusal (manifest §43.10). Safe-202-ignore posture superseded.

## P2 — domain/application transitions (columns exist, decision fns live elsewhere or nowhere)

- [ ] Follow-request accept/reject + locked-account branching in domain (columns bounds-checked only) — decide: keep in application (current) and drop from domain TODO, or move.
- [ ] Note edit/delete permission + tombstone transitions (`is_deleted` read effect only; `NoteEdit` shape only).
- [ ] Reaction unreact/idempotency (only quote-auth has it: `domain/quote.rs:65-67`).
- [ ] Rename/move state machine (`moved_to_uri` is profile metadata, no migration).
- [ ] MFM sanitization in domain (real MFM↔HTML lives in `activitypub/objects.rs:287-529`; domain has only `extract_mentions/hashtags`).
- [ ] `role_ids` stored never enforced; `pinned_page_id`/`verified_links`/clip `notesCount`/antenna `exclude_notes_in_sensitive_channel`/7-day auto-deactivation deferred (all documented in-domain).
- [ ] Password signin/2FA/WebAuthn/session management: no `signin/login` app op — credential check lives in api; decide layering.
- [ ] Hashtag trend/announcements/ads/games/promo/flash/bubble: no app backing (only exact-tag lookup `notes_with_tag` + featured ranking).

## P3 — ops/observability/config

- [x] `/healthz` static `"ok"` — LANDED: `/healthz` liveness + `/ready` bounded readiness (redb read + storage roots, 503 on fail); Docker probes `/ready`; `tests/admin_diagnostics_h-fix.rs` proves readiness-fails-while-liveness-up.
- [ ] Metrics/prometheus/tracing counters (bytes used, hits/misses, evictions — plan `:141-143` requires "without shell access").
- [x] Structured logging levels — LANDED: structured levels + stable events + correlation IDs + redaction (`observability.rs`); `fed_debug!` gate retained.
- [x] Config surface — LANDED single-node: `rate_limit.*` + `uploads.*` JSON-configurable (validate >0, round-trip + typo tests); `transcode.enabled=false` default, `unsafe.allow_local_http_for_testing` JSON opt-in, `diagnostics.*`, `rfc9421_mode`; remaining env-only `PINETREE_ALLOW_PRIVATE_IPS`, `PINETREE_DEBUG_FEDERATION`, `PINETREE_REPLY_REPAIR_LIMIT` documented as throwaway-rig/test exceptions.
- [x] Rate limits bounded in-memory per-process (`UploadSlots`, `RateLimiter`) — LANDED Batch D (bounded + expiry + race-safe, `tests/rate_limit_bounded_d4.rs` + `tests/rate_limit_config_d-fix.rs`); shared limiter still needs multi-node (out of scope).

## Verify-then-close — RESOLVED 2026-10-20 (freshness pass; section intentionally empty)

Verified against code and closed (evidence in `state/logs/2026-10-20-docs-freshness-pass.md`):

- [x] `notes/reactions` cursor — still diverges (`sinceId`-only, newest-first; `queries.rs:1944`). Promoted to P2 "API parity" below, not closed.
- [x] `notes/timeline` `withReplies` — confirmed absent and documented as decided (manifest §19; code `mod.rs:6132-6147`).
- [x] `i/revoke-token` `tokenId` — implemented, not a no-op (`mod.rs:6776-6799`). Profile line superseded.
- [x] Streaming `updated` — emitted (`server.rs:700-728`). Manifest §9 stale line superseded.
- [x] `users/followers|following` moderator bypass — live, not always false (`queries.rs:1704-1713,1737,1853`).

## Docs sweep additions (2026-10-20) — everything the docs promise but code doesn't have yet

Items already covered above are NOT repeated. `[landed?]` = doc said TODO but inventory shows WORKS — verify then close, don't re-implement.

### Remote-media cache plan (`docs/remote-media-cache-plan.md`, Status: Draft) — the ordered build

- [ ] Reference-safe deletion first: refcount (or remaining-reference check) before blob delete; metadata+accounting in txn, blob delete outside txn with reconciler sweep (plan §2, `:60-64`).
- [ ] New schema: remote-cache index keyed by (source URL, variant) + reference accounting on same schema (`:105-108`).
- [ ] Metadata-preserving eviction: evict drops bytes only, keeps metadata + source URLs for re-fetch (`:109-110`).
- [ ] On-demand default: replace download-on-note-arrival with fetch-on-first-media-request; background jobs opt-in warming only, relays excluded (`:74-77,111-113,158-159`).
- [ ] Separate local vs remote stores (preferred), or accounting that provably protects durable refs (`:84-86,114-115`).
- [ ] Global + per-class budgets + auto-eviction with existing per-file/per-note limits as inner guardrails; starting point 512 MiB global (avatars/headers/emoji 128 MiB/30d, previews 256 MiB/7d, fulls 128 MiB/3d, audio/video 0 persistent) (`:78-80,116-132`).
- [ ] Independent accounting incl. in-flight reservation (not post-hoc totals); must respect FDB 5s/10 MB constraints (`:142-145`).
- [ ] Safe-eviction rule + periodic idempotent reconciler (orphaned/missing blobs, stale metadata, unfinished deletions, sidecar mismatches, pre-slice-8 MD5 rows) (`:149-157`).
- [ ] Observability without shell: bytes used, utilization, per-class breakdown, hits/misses, bytes fetched, evictions, failed origins (`:160-162`).
- [ ] Stream-to-disk under byte cap (may move earlier if memory pressure measured) (`:93-95`, build order `:172-188`).
- [ ] Scoped warming last (opt-in, rate-limited, relay-excluded) (`:172-188`).
- [ ] Document global+class budget as deliberate divergence in parity plan (`:164-168`).
- [ ] Decide/document whether remote rows are ever directly deletable (`:68-69`); audit eviction path against same rule (`:70`).
- [ ] Revalidate avatar/emoji URLs (content at a URL can change) (`:78-80`).
- [ ] Acceptance tests: cap-never-exceeded with in-flight reservation; local uploads survive eviction; shared-blob + crash-heal regressions; mid-stream abort (absent/dishonest length, HTTPS, chunked); per-note budget pinning; evicted re-fetch (`:192-203`).
- [ ] Birchtree sequencing: lifecycle+eviction here first, then carry policy+store separation to Birchtree (eager fetch + no content addressing there) (`:207-212`).

### Communities / extension plan (`docs/community-planning.md`, `docs/extension-plan.md`, `docs/pinetree-decisions.md`)

- [ ] Core seal: `restrictedTo` check only (~50 lines, no groups/mods/UI in core); generalizes later (`community-planning.md:4-5`, extension-plan Phase A2).
- [ ] Native model: `community` + `community_member` tables, `note.community_id` (dual-write from Catodon `forumId`/`forumScoped`, then cutover), `CommunityPolicyService.canPost/canView/canModerate`, `communityTimeline:{id}` fanout + home fill, materialized counts/catalog, per-community modlog, first-class notif prefs (`:8-13`).
- [ ] Visibility/privacy: public/unlisted/local_only/private enforcement incl. extension-to-extension private, `ApprovalRequired` flow, flip-to-non-federating = tombstone-then-update, local-only 404, private fetch needs follower-instance signature (`:15-18`).
- [ ] Roles/mod: create/edit/delete/transfer, ban-unban+wipe, approvals queue, posting-restricted-to-mods, pins/locks/distinguish/warn, reports, shared modlog (`:20-23`).
- [ ] Lemmy-compatible Group wire: `!name@host` actor, audience=community, `Page`+`Announce`, comments as `inReplyTo` Notes, bare Like/Dislike + audience, Delete/Block(+removeData)/Add/Lock/Update(Group/Page)/Flag/Resolve/Warn, wrapped+raw inbox accept, newest-~20 outbox Announce, paged resumable outbox backfill, lenient parsing, deterministic Create ids, RFC 9421 + legacy verify-if-present permissive-first (`:25-29`, extension-plan G2).
- [ ] Migration Catodon→native (community row per isGroup user, dual-write, member/ban copies, cutover, drop forumId/forumScoped + `user.forum*` after verify) (`:31-35`).
- [ ] Deferred: Lemmy `/api/v3` shim v1-or-later TBD; exact DDL + policy code implementation pass; open community DDL/policy TBD (`:38-40`, decisions `:33`).
- [ ] Join queue: `approvalRequiredForSignup` + `approvalRequiredEmailDomains` + approve/decline endpoints + approvals page + join reasons (extension-plan B1, decisions `:28`).
- [ ] Day-to-day user ops copy (TransFem one-click set + spam-log search/restore, CSAM quarantine/resolve, purge-blocked) + instance plumbing copy (set-root, restart-migration, roles clone + annotate-condition, gen-vapid-keys, shared-access grants + dialog, purge-user-content) (extension-plan B2/B3, decisions `:29-30`).
- [x] Like-vs-favorite split — LANDED 2026-10-20: `notes/like` (`override ?? defaultReaction ?? default_like ?? ❤`, Sharkey toggle, 2/sec bucket; manifest §41; `tests/notes_like_contract.rs` 6 + endpoint tests 2 green). Both references verified to implement like as react-with-default (no separate table/activity); Misskey favorites stay local-only bookmarks, untouched.
- [ ] Specified-visibility hiding: non-event specified notes stay out of home/local/global/custom feeds, profile notes, live streams (owner sees own via exempt); invite-only event posts are NOT messages and still list (decisions `:22`, extension-plan C2).
- [ ] Cat mode split: `isCat` (cosmetic) independent from `speakAsCat` (speech transform) (decisions `:25`, extension-plan C3).
- [x] RFC 9421: verify-if-present permissive-first LANDED single-node (rsa-v1_5-sha256; `tests/federation_sig_mode_e5.rs` + overflow hardening `tests/federation_sig_mode_e-fix.rs`); enforce opt-in; live-interop pending. Was deferred per wiki roadmap.
- [ ] Events: steal Catodon FEP-8a8e outright — NoteEvent sidecar, EventRsvp unique-pair, EventInvitation PK-pair, dual-dialect RSVP, inviteTo DM fallback, visitor-gated `.ics`, `events/{upcoming,rsvp,unrsvp,attendees,invitees,invite}` shape, ≤20/call public-invite local-only never federates (decisions `:18-20`, extension-plan D1-D3).
- [ ] Imports: per-source parsers for Misskey, Pixelfed, Mastodon, Sharkey, Catodon now; Lemmy later; Akkoma/Firefish explicitly out unless demanded (decisions `:27`, extension-plan E1).
- [ ] Open: translate pipeline composition (provider picker + LLM quality gate composed, single langdetect lingua; heuristic works with zero deps, LLM gate flagged only) (decisions `:35`, extension-plan E2).
- [ ] Open: rec engine MVP scope (pgvector + heuristics first; Milvus/LLM flagged later; NO Milvus/vLLM/Qwen/Python sidecar in MVP) (decisions `:36`, extension-plan F1/F2).
- [ ] DVD constraint: AI models never default-or-only path; heuristic/structural fallbacks first-class; external deps flagged, degradable, never 500s (decisions `:4-5`).

### Quotes / replies follow-ups (`docs/quote-posts.md`, `docs/replies.md`)

- [ ] Quote manual-approval configurability: review workflow still OPEN. `manualApproval` is correctly not advertised. Auto-approval covers Public AND Home, not only Public (`inbound/quotes.rs:113-114`); decide whether that is intended and update quote-posts (quote-posts `:132-135,156`).
- [ ] Mastodon/Glitch-Soc fixtures from actual target revisions: NO revision pinned yet (`tests/quote_fixtures.rs:3-16,214-260`) (`:105`).
- [x] Migration/default so ordinary renotes aren't misclassified as quotes — LANDED via serde default (`domain/note.rs:92`).
- [x] Fix `replies_count` never decremented on delete — LANDED on redb, single store path (`redb/mod.rs:1797-1801,1865,4212-4217`).
- [x] Fix inbound ingestion dropping unresolved `inReplyTo` — LANDED: pending-URI + resolver (`inbox.rs:2113-2126,2170-2177`).
- [ ] Opt-in bounded repair for historical remote notes whose parent URI was discarded (`repair_unresolved_reply_parents`, rate-limited/restartable, never guesses, never unbounded at startup) (`:124,176,202`).
- [ ] Replies slices: A (storage index+query) and B (unresolved-parent preservation) LANDED on redb; C (federation + regression hardening) PARTIAL; FDB backend OPEN (`:163-183`).
- [x] Hide-followers fields — LANDED: `Account` carries `followers_visibility`/`following_visibility` (`domain/account.rs:142,145`), AP collections return 403 for non-public (`activitypub/actor.rs:922,981`). Remaining gap: no follower-only exemption (activitypub-collections `:112`; phase2-6 slice 11).
- [ ] Outbox `totalItems` needs store-side index (omitted by documented choice) (`:113`).
- [x] `blocked_hosts()` — LANDED: store-backed (`application/queries.rs:135-137`, `instance.rs:195`, `outbox.rs:1844`). Instance MUTE (§0B) still OPEN.
- [x] Drop resolved application-ports open-decision #3 row (already struck through at `application-ports.md:109`).
- [ ] Exact media URL paths deferred to media contract; full routing protocol + internal auth are a separate later contract (identity-rules `:72,82`).

### Handoff batches N–Q remainder — mostly landed, verify-then-close

- [ ] `[landed?]` Batch N (emoji, favorites+state, notifications incl. `followRequestAccepted`, antennas, abuse/Flag, admin remainder, MiAuth hardening, server word-mutes) — inventory shows WORKS; verify each, close.
- [ ] `[landed?]` Batch O (invalidate, search-by-username-and-host, self-service password/apps/real revoke-token, upload-from-url, find-by-hash, url-preview, user lists + antenna `src=list`) — inventory shows WORKS; verify, close. Still-open: remote list members via system actor (owner decision pending), `create-from-public` deferred.
- [ ] `[landed?]` Batch P (email loop, `i/move` + inbound Move, `i/registry` family, small reads/meta, mentions, test-notification/flush, recommendations, versions+translate) — inventory shows WORKS; verify, close.
- [ ] Batch Q audit findings (9 at HEAD: requests/list permission kind, notes/delete moderator vs manifest, users/show suspension + CONTENT_RESTRICTED, notifications bucket+mark-read order, following/create withReplies default, fileIds/mediaIds merge, timeline flags, federation trio, meta.mediaProxy route) — re-audit at HEAD, fix or close each (handoff-batch-q `:21-29`).
- [ ] Batch Q remainder: auth hardening (`NO_SUCH_APP`/2FA/captcha/`my/*`), webhooks ARE in scope (subscription+delivery+`webhooks/*`), media pipeline (thumbnails/blurhash/EXIF/Range + backfill), reads/discovery remainder, scoped analytics/retention+charts, rate-limit/role remainder (`rateLimitFactor` + unbucketed expensive reads) (handoff-batch-q `:44-48`).
- [ ] Smoke gate: `|tail -30` masks failure, phase-2 SKIP still `SMOKE_ALL_OK`, extend `keystone-sweep.sh` 38→new count (implementer-guide `:428-436`).

### Bench forensics — pinetree-owned subset (rest is birchtree/harness/upstream)

- [x] `notes/reactions` list endpoint — LANDED (bench P2 closed).
- [x] Outbound poll options `type:Note` — LANDED (`objects.rs:1070`).
- [ ] `EmojiReact` outbound per-host render LANDED 2026-10-21 (WP5; Akkoma/Pleroma get `EmojiReact`, Misskey-family emoji-aware `Like`, Mastodon-family bare `Like`; manifest §43.10). Live Akkoma round-trip before claiming interop → moved to the OPEN interop list below (was: needs a live round trip).
- [x] Reply `replyId` via `ap/show` — LANDED: shared ingest with pending-parent machinery (`inbox.rs:2081-2153`).
- [x] Remote media on `ap/show` path — LANDED: `RemoteMediaFetch` queued in-txn via shared ingest (`inbox.rs:2081-2153`). Quote-path fetch decision still open under the poll/quote items.
- [ ] Poll-vote silent drops — counters/logs per drop reason + fixtures (fediverse-bench-fixes P6).
- [x] `NOTES_BY_URI` uniqueness — LANDED (`DUPLICATE_NOTE_URI` `redb/mod.rs:1139`; tombstones refused `:1148`). Still OPEN: `find_by_uri` full-scan fix, upgrade-on-second-sight, migration/dedup.
- [ ] Quote debt: stamp authority host-granular (not exact), `StampFetchPending` non-atomic, no negative stamp dedup, quote lookups full-table scans (audit `:115`).
- [ ] Reply debt: N-waiters N jobs, no Reply notification on late link, repair startup-env only (audit `:116`).
- [x] S6 Delete-beats-Create race — tombstone coverage LANDED (`inbox.rs:1740-1748`).
- [ ] Harness-only (do NOT implement in pinetree): `fetch_bytes` lab CA, S7 500-char cap/exemption, features media settle-until-deadline, interop oracle rewrite, Mastodon-422 confirmation, catodon-log needs — track in bench docs, not here.

### Wiki roadmap (planned / non-goals)

- [ ] Phase 4 FDB multi-node + Phase 5 scale/hardening — planned, see P0 (wiki Roadmap).
- [x] Deferred: RFC 9421 LANDED single-node (permissive + enforce, overflow-hardened); OAuth still deferred (only if target client needs), public-internet federation soak still open (wiki Roadmap).
- [ ] Non-goals (keep marked, promote by decision only): Mastodon client API, Reversi/chat + legacy features unless target client requires, `assetlinks`/`oauth-authorization-server` 404-fine, Sharkey `service-server` ML/queues, C2S outbox, relays + LD-sign relays (wiki + catodon `:68`).

### Manifest/profile stale lines — verify-then-close (supplemental to list above)

- [x] Manifest `UNSUPPORTED_FIELD` unknowns: `Update(Note)` TODO(§8) stub — edit branch LANDED (`inbox.rs:1796-1797,1953`); manifest addendum §39.
- [ ] Manifest: string-URI `Update(Question)` not fetched / invalid-Question downgrade / custom-emoji passthrough (`:273-279`) — all recorded DIVERGENCE; confirm current code matches and close.
- [x] Manifest: `users/lists/create-from-public` deferred (`:148`, profile `:91`) — LANDED 2026-10-21 (gaps WP2a final; manifest §43.2). Remote-member system actor still open (owner decision pending).
- [ ] Manifest: verifiedLinks re-verification loop deferred (`:183`); decoration-registry filtering deferred (`:188`) — schedule or keep deferred. (`federation/instances` skipped is CLOSED: LANDED 2026-10-21, manifest §43.5. Note-body cleanup job still deferred; admin notifications skipped; remote-suspend sweeps local-only with possible revisit.)
- [x] Manifest needs-owner-decision — DECIDED + LANDED 2026-10-21 (gaps WP2c final; manifest §43.5): `federation/update-instance`, `refresh-remote-instance-metadata`, `find-by-email`, `get-user-ips` all ship (destructive `remove-all-following`/`delete-all-files` default-out).
- [x] Profile: `i/notifications-grouped`, `notes/featured`, `notes/reactions` — shipped (`mod.rs:8998,9029,9067,6278`); profile lines superseded in WS1 addenda.
- [x] Profile: `notes/search` offset/range — shipped (`mod.rs:8146-8178`); `channelId` still rejected.
- [x] Manifest: `server-info` shipped (`mod.rs:7759`). `queue/stats` LANDED 2026-10-21 (gaps WP2c final; delay/clear arms loud 400; manifest §43.5). `stats` shipped (§35).
- [x] Manifest: `reset-password` shipped (`mod.rs:7311`); superseded in manifest addendum.
- [ ] Parity plan: per-host `EmojiReact` render LANDED 2026-10-21 (WP5; manifest §43.10); the live Akkoma round-trip before claiming interop moves to the OPEN interop list below. `customEmoji` export/import out (`:168`); smoke-gate fix + §6 docs pending (`:191`).
- [x] Domain-types deferred list partially SUPERSEDED — addendum written 2026-10-20 (antennas/pages/clips/polls landed; channels, folders, access keys, forensics still out) (`:13,34`).
- [ ] Ports: `TimelineMaterialize` reserved Phase 5 (`:64`); closed `JobKind` payload schemas open (`:107`); lease/retry numbers deferred to delivery contract (`:108`); cursor/limit defaults belong to manifest (`:110`); `FeaturedChanged` relays deferred (`:172`).

## Added 2026-10-20 by the freshness pass (found while verifying; not in the sections above)

- [ ] P2 `unimplemented!()` trait defaults: 20 methods (`FollowStore` x3, `ActivityDedup` x17) in `src/application/ports.rs:1037-1612` panic if an adapter omits them. Contradicts `application-ports.md:181`. Make required or return `StorageUnavailable` (addendum in `application-ports.md`).
- [ ] P2 API parity: `notes/reactions` cursor still `sinceId`-only, newest-first (`queries.rs:1944`); upstream is ASC (manifest §4). Decide parity vs documented divergence.
- [ ] P2 Quote auto-approval covers Public AND Home (`inbound/quotes.rs:113-114`); quote-posts says Public only. Decide and align the doc or the code.
- [ ] P2 Replies slice C (federation + regression hardening) PARTIAL; FDB backend OPEN (see the replies line above).
- [ ] P3 Config rule: `PINETREE_REPLY_REPAIR_LIMIT` is env-only (`main.rs:307`); job lease/retry numbers are hard-coded (`main.rs:334-335`, not config keys). Move both into `--config` or document an exception.
- [ ] P3 Gate: `scripts/smoke-all.sh:53` pipes cargo through `tail -30` (exit status masked); SKIP still prints `SMOKE_ALL_OK` (`:77-92`). Fix the script (a separate, code-touching change; not part of this docs pass).
- [ ] P3 Keystone sweep count: `scripts/smoke-all.sh` runs 38; `docs/testing.md` says 41. Reconcile by running the sweep.
- [ ] Doc: Phase 2 Sharkey pin: `docs/phase-2-plan.md` header says `2025.4.4`; `docs/README.md` and `phase-2-exit-criteria.md` say `2025.4.7`. Reconcile; the 2025.4.4 run is the recorded evidence.
- [x] Verify: Batch M WP8 (admin emoji delete/list) LANDED 2026-10-20. `users/lists/create-from-public` LANDED 2026-10-21 (manifest §43.2). Still open: remote-member system actor (owner decision pending) per `implementer-guide-batch-m.md`.
- [ ] Verify: Batch N/O/P endpoints exist (spot-checked 2026-10-20) but per-feature behaviour is not re-tested.
- [ ] Verify: `docs/sharkey-parity.md` Sharkey and Misskey claims were not re-checked against the reference checkouts.
- [ ] Open (from phase plans): phase2-5 slice 7 (`webfinger.rs:4`) and phase2-6 slice 13 gate evidence.

## OPEN — live interop + bench (2026-10-21 gaps final)

Internet-gated: none of these can be claimed hermetically. Everything above this section is code-complete in-tree; everything below requires live servers.

- [ ] Akkoma round-trip: per-host `EmojiReact` render (§43.10) verified against a live Akkoma (content vs tag per host, Undo, inbound accept).
- [ ] Pleroma round-trip: same `EmojiReact` matrix against live Pleroma.
- [ ] Mastodon round-trip: bare-`Like` favouriting both directions; `Question`/`Update` handling; Mastodon/Glitch-Soc fixtures pinned to actual target revisions (quote-fixture revision still unpinned).
- [ ] Glitch-Soc round-trip: same matrix as Mastodon + any Glitch extensions exercised.
- [ ] Sharkey round-trip: `Update(Note)` fetch-and-apply, `EmojiReact`, like-toggle, schedule shim against live Sharkey.
- [ ] Public-internet soak: staged allowlist → open, SSRF/TLS/redirect/size budgets unchanged; soak checklist signed.
- [ ] Bench 0-fail: crossfamily + stress + features harness green with zero failures less documented-divergence (poll-vote drop counters/logs per reason still open; stamp-authority host-granular + `StampFetchPending` non-atomic + quote-lookup full-table scans still open as audit debt).
