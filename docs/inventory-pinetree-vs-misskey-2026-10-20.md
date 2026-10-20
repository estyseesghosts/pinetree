# Pinetree vs Misskey inventory (code, not just docs)

Status: Draft
Date: 2026-10-20
Scope: `src/` vs real Misskey (`./misskey` symlink, `2026.10.1-alpha.0`) + Sharkey (`2025.4.7`) reference checkouts.
Method: 7 parallel read-only code scouts over disjoint source areas. Every claim below has a `file:line` citation the scout actually read. Nothing here is doc-only.

## TL;DR

- **Core social graph works:** accounts, notes (create/show/delete/edit), follows (+locked-account requests), blocks, mutes, renote-mute, reactions (incl. `_misskey_reaction`), polls, pins/featured, clips, pages, antennas, user lists, favorites, drive files (flat, no folders), abuse reports, invites, import/export, MiAuth + token auth, timelines (home/local/global/hybrid + list + mentions + search), notifications, streaming main+timeline+list+antenna+capture channels, WebFinger/NodeInfo/actors/signatures/inbox/outbox/delivery, remote-media fetch with SSRF/validation gates.
- **Intentionally divergent (documented):** no channels/chat/gallery/hashtag-trend/announcements/reversi/flash/drive-folders/roles (rejected or out-of-scope per `docs/endpoint-manifest.md` §5/§9, `docs/phase-3-profile.md` §5); `notes/edit` + `notes/versions` + recommendations + `i/move` root=oldest-admin + quiet hardening extras; fan-out-on-read timelines; in-process streaming hub (no Redis); presence = live sockets; quote FEP-044f superset; poll shape gate; `Update(Note)` embedded-only; `specified`/`followers` visibility matrix incl. fail-closed pending replies.
- **Missing/broken (ranked at bottom):** everything multi-node (FDB adapter empty, directory/routing/internal-protocol stubs, `Cluster` mode hard-rejected); remote-media lifecycle (no GC/quotas/eviction, unsafe shared-blob delete, full-body buffering); ops (no S3, no backup, static `/healthz`, no metrics); client-visible API holes (drive-folders, channels, chat, announcements, follow-settings/update, hashtag surfaces, thread-muting, file find/move-bulk, 2FA/apps, memos, bulk-show); streaming holes (hashtag/role/channel/drive/stats/admin/clip/chat/reversi channels + `unreadNotification`/`announcementCreated`/`driveFileCreated`/etc. main events + `reacted.emoji` detail + list `withFiles/withRenotes`); domain transition gaps (follow-request branching, note edit/delete permission, reaction unreact/idempotency, rename/move state machine, MFM sanitization lives in AP layer not domain).

Phase context: Phase 3 exit is green (client compat + streaming for the declared profile). Phase 4 (FDB multi-node) has not begun — the stubs below are expected, not regressions.

## 1. What works

### 1.1 Misskey API transport/auth/envelope — WORKS
- `POST /api/{endpoint}` JSON dispatcher + dedicated routes (`/healthz`, `/files/{id}`, `/miauth/*`, `/signup`, `/signin`, `/api/drive/files/create`, `/proxy`, `/streaming`): `src/api/misskey/endpoints/mod.rs:148-166`.
- Credential order Bearer → body `i` → `pinetree_session` cookie (cookie is a pinetree addition for web MiAuth): `src/api/misskey/auth.rs:187-208`, `mod.rs:250-264`.
- Suspended → 403 `USER_SUSPENDED`, deleted → 401: `auth.rs:262-285`.
- Error envelope `{error:{message,code,id,kind,info?}}` + stable per-endpoint UUIDs; unknown endpoint → 404 `UNKNOWN_API_ENDPOINT` (`mod.rs:9122`); unknown field → 400 `UNSUPPORTED_FIELD` on every arm (`src/api/misskey/request.rs:16`); void → `204`.
- 1 MiB JSON cap → 413; pagination `sinceId/untilId/sinceDate/untilDate`, limit 1–100 default 10 (`mod.rs:431-441,953-961`); per-family rate buckets with `Retry-After` (manifest §7).
- Token order case-sensitive scheme, `requireCredential`/`require_permission(kind)` per endpoint; public reads take anonymous; `i/revoke-token` fully implemented incl. `tokenId` (`mod.rs:6770-6854`).

### 1.2 Accounts/auth/meta — WORKS
`meta`, `miauth/gen-token` (+hardening), `GET/POST /miauth/:session`, `auth/session/generate|show|userkey`, `auth/accept`, `signup`, `signin` (Argon2id+legacy upgrade `auth.rs:60-118`), `i`, `i/update` (all 39 + `rejectQuotes` + `defaultReaction`), `users/show`, `users/search`, `users/search-by-username-and-host`, `username/available`, `users/relation`: `mod.rs:2531-2647,2711-2843,2844-3061,3062-3151,4169-4190,8603-8644`.

### 1.3 Notes core — WORKS
`notes/create` (visibility default public, `fileIds??mediaIds`, `reactionAcceptance`, poll; `channelId` → `UNSUPPORTED_FIELD`), `notes/show` (invisible → `NO_SUCH_NOTE`), `notes/delete`, `notes/unrenote`, `notes/local-timeline`, `notes/global-timeline`, `notes/hybrid-timeline`, `notes/user-list-timeline`, `users/notes`, `notes/replies|renotes|children|conversation`, `notes/search`, `notes/search-by-tag`, `notes/mentions`, `notes/state`, `notes/polls/vote`, favorites, pins: `mod.rs:4191-4267,4268-4301,6131-6277,4121-4168,6856-6970,8145-8212,7784-7830,6402-6459,6350-6401,8968-8997`.
Domain validation behind it: text 1–3000, CW 1–100/512, `file_ids ≤16`, tags/emojis ≤128, URI caps, specified-requires-users, renote/reply narrowing, blocked-author pure-renote reject, quote-vs-renote classifier: `src/domain/note.rs`, `src/domain/policy.rs:60-182`.

### 1.4 Follows/blocks/mutes — WORKS
`following/create|delete|invalidate`, `requests/list|accept|reject|cancel`, `users/followers|following` (+private/followers gates), `blocking/*`, `mute/*`, `renote-mute/*`, `users/report-abuse`: `mod.rs:5987-6130,6985-7052,8383-8602,4965-4982`. App ops `src/application/commands/follows.rs`, `blocks.rs`; inbox dispatches to same ops (`federation/inbox.rs:406,934,941,1191,711,1235`).

### 1.5 Polls/drafts/scheduled/pins — WORKS
`vote_on_poll`, `handle_poll_expiry`, inbound vote/question-update, expiry job: `src/application/commands/polls.rs:21,141,173,259,308`. Drafts + scheduled (`create/update/delete/list/count/post`, `create_scheduled_note`): `commands/drafts.rs:45-451` + `JobKind::PostScheduledDraft`. Pins/featured rank: `commands/pins.rs:86-290`.

### 1.6 Clips/pages/antennas/lists/emoji/abuse/moderation/invites/import-export — WORKS
- Clips (11 + follow/unfollow/followers): `commands/clips.rs:47-691`, `mod.rs:3489-3943`.
- Pages (create/update/delete/show/like/unlike/featured + remote Article ingest): `commands/pages.rs:130-877`, `mod.rs:3944-4120`.
- Antennas (CRUD + notes + match job, 200-cap): `commands/antennas.rs:27-338`, `mod.rs:5268-5446`.
- User lists (CRUD + push/pull + membership + timeline): `commands/user_lists.rs:27-174`, `mod.rs:3152-3488`.
- Drive files (flat): create/list/show/update/delete/upload-from-url/find-by-hash: `commands/media.rs:24-222`, `mod.rs:7093-7192,8017-8144` + `GET /files/{id}` serve hardening (`mod.rs:1233-1292`, inline-safe MIME only, `nosniff`, `/proxy` 8 MiB cap).
- Emoji local+remote ingest/fetch: `commands/emoji.rs:57-291`, `mod.rs:5080-5267`.
- Abuse/Flag, moderation/suspend/roles/logs/instance, invites, export/import, keys, notifications write side: `commands/abuse_reports.rs`, `moderation.rs`, `invites.rs`, `exports.rs`, `keys.rs`, `notifications.rs`.

### 1.7 Queries/timelines — WORKS
`notes/show` (pending-withhold, `CONTENT_RESTRICTED_BY_USER`, `ugcVisibilityForVisitor`, mute/block filter): `queries.rs:447-519`. `users/show`, followers/following (+visibility gate), reactions, replies/children/conversation/renotes, home/local/global/hybrid (+includes, merge-dedupe), search (substring, no FTS — 10k in-memory window), mentions, list timeline, recommendations (simplified scorer, documented), notifications (+grouped), `i/*`, stats/featured/antenna/clip-favorites/drive/drafts/invites/logs: `queries.rs` throughout (§2 of app report).

### 1.8 Streaming (implemented channels) — WORKS
- Upgrade `/streaming?i=` + Bearer-wins, 401/403, anonymous onto local/global only, 2-min reap + 60s ping, `main` shared, `pong:true` → `connected`: `src/streaming/server.rs:46-49,97-134,233-239,264-280,438-468`.
- Channels: `main`, `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline`, `antenna` (owner-checked), `userList` (+`userAdded/Removed`): `server.rs:341-442,565-634,798-894`.
- Client msgs: `channel/ch` (+`readNotification` superset), top-level `readNotification` → mark-all-read + `readAllNotifications`, `subNote/s/sr` + `unsubNote/un` with 1536 LRU + refcount: `server.rs:281-328,471-490`.
- Events: timeline `note`, list/antenna `note`, capture `reacted/unreacted` (light gate), `deleted`, `pollVoted`, `updated {cw,text}`, `main` notification/mention/reply/renote/edited/follow/followed/unfollow/receiveFollowRequest/meUpdated/readAllNotifications/notificationFlushed: `server.rs:136-145,523-768`, `worker.rs:360-442,467-494`, `events.rs`.
- Fan-out: state+`StreamingPublish`/`Notify` job in one txn → worker → in-process `Hub` broadcast → per-connection filter+pack, at-most-once, backlog drop: `hub.rs:1-11,148-248`, `worker.rs:354-358,467-497`.

### 1.9 Federation/AP — WORKS
- WebFinger JRD+XRD (+`host-meta`), NodeInfo 2.0/2.1 (`software.name=misskey`): `src/activitypub/webfinger.rs:102-140,176-287,352-534`.
- Actor docs (Person/Service/bot, canonical `/users/{id}`, `#main-key`, sharedInbox, followers/following/featured/outbox, `manuallyApprovesFollowers`, no `_misskey_content`): `actor.rs:424-583,619-717`. Remote parse + inbox hardening: `actor.rs:95-260`.
- Signatures: RSA-2048, SPKI/PKCS#8/PKCS#1, `rsa-sha256` sign (`ring`) / verify (`rsa`), Digest SHA-256, `(request-target) date host digest`, `Signature:` + `Authorization: Signature`, `hs2019`, `(created)/(expires)`, 300s skew, cached-key → refresh-once, `activity.actor == signer` enforced: `signatures.rs:33-473`, `inbox.rs:183-355`, `fetch_gate.rs:81-134`.
- SSRF: http(s) only, literal+resolved-IP private/reserved reject, TLS always verified, 10s/20s/5s timeouts, ≤3 GET redirects, downgrade refused, 64 KiB AP cap, 64 KiB inbox body cap: `delivery.rs:174-744`, `inbox.rs:90-128`.
- Inbox activities: Follow/Accept/Reject/Undo(Follow)/Create(Note|Question|Answer)/Announce/Like/EmojiReact/Delete/Update(Person|Question|Note|OrderedCollection)/Add/Remove(clip+featured)/Block/Undo(Block)/Flag/Move/QuoteRequest(+Accept/Reject)/Undo(Announce|Like|Accept): `inbox.rs:334-2058`. Dedup atomic `check_and_record` in-txn, `202` on replay: `inbox.rs:1-11,963-1027,1319-1327,1592-1595`.
- Outbox/delivery: every mutation commits `FederateDeliver` in-txn (`outbox.rs:46-305`); fan-out preferred-inbox, followers paged 1000, inline ≤100 else one-inbox jobs with derived keys; per-inbox 2xx/408/429/5xx/Retry-After/3xx/4xx classification, exponential cooldown + `Deferred`, instance-block check, stable activity IDs for at-least-once: `outbox.rs:307-1929`, `retry.rs:40-147`, `objects.rs:793-1043`.
- Objects: `to/cc ↔ Visibility` both directions, Note/Question render+parse, polls (render + `Update(Question)` tallies + vote `Create`), FEP-044f quotes (5 inbound carriers, outbound `quote+_misskey_quote+quoteUrl+quoteUri` + stamp verify + visibility-gated stamp serve), replies (pending-URI + resolver, direct-parent outbound), mentions, attachments, per-host reaction split (emoji-aware vs plain for mastodon-family, 7-day NodeInfo cache), deletes/tombstones/`410`, `Update(Note)`/`Add/Remove`/`Block`/`Flag` renderers, MFM↔HTML sanitize, remote emoji ingest: `objects.rs`, `quotes.rs:123-486`, `host_caps.rs:33-169`, `inbox.rs:2109-2266`, `remote_cache.rs:37-100`.
- Remote media: inbox never fetches inline — durable `RemoteMediaFetch` job atomically committed; pre-fetch URL/SSRF validation; worker per-file `max_file_size`, 64 MiB/note budget, MIME sniff, idempotent per (URL,note): `inbox.rs:2151-2177`, `media/remote_cache.rs:8-206`.
- Outbox/followers/following/featured collections paging (40/page, budget, 403 unless public): `actor.rs:719-1048`.

### 1.10 Domain read-time rules — WORKS
IDs (aidx + misskey-id), username shapes, handle normalization, actor-URL form, remote/local account bounds, profile bounds, block/mute shapes, reaction normalize/length/LikeOnly, emoji shapes, visibility predicate + timeline eligibility + list reply rule, antenna/word-mute/instance documented behaviors, quote machine, note-edit shape: `src/domain/*` (see domain table in analysis; `todo!`/`unimplemented!` grep clean — 0 hits, only 2 plain `TODO(§0B)` comments).

### 1.11 Persistence (single-node) + jobs — WORKS
- redb: state+job/outbox in one txn, crash recovery, 64 KiB/500-job bounds, lease claim/no-double-claim/retry/quarantine/defer, dedup + tombstones, migration markers, media-bytes-outside-DB: `src/persistence/redb/mod.rs` (~9727 lines), `ports.rs:23,851-910`, `jobs/worker.rs:15-750`. Conformance **118/118 green on redb** (`docs/persistence-conformance.md:303,318`, `tests/conformance.rs` 119 tests).
- Media ingest validation (size + sniffed MIME, allowlist, filename sanitize, MD5-per-owner dedup): `commands/media.rs`, `domain/media.rs`, `media/ingest.rs`, `media/storage.rs` (content-addressed fs blobs, atomic tmp+rename).
- Config: JSON `--config`, 11 fields, defaults, `validate()`, `deny_unknown_fields`, CLI-overrides-file, round-trip tests: `src/config.rs:29-220`, `main.rs:195-275,394-406`. Admin bootstrap (`create-account`, `set-role`), 50 ms/30 s/1 s worker, per-process rate limiter/upload slots, `FeaturedCache`: `main.rs:23-365`.

## 2. Intentionally divergent (documented, keep)

1. **Out-of-scope surfaces** (`endpoint-manifest.md` §5/§9, `phase-3-profile.md` §5): `channels/*` (16), `chat/*` (~25), `gallery/*` (9), `flash/*`, `reversi/*`, `hashtags/*` trend surfaces, `announcements` + `promo`, `roles/*`, `federation/*` admin, `charts/*`, `drive/folders/*` + `drive/stream`, `i/2fa/*`, `i/webhooks/*`, `my/apps`, `app/*`, `sw/*`, `clip`/`drive`/`serverStats`/`queueStats`/`admin`/`hashtag`/`roleTimeline` streaming channels, `relay`/`ad`/`webhook` admin. `folderId` non-null → 400 "folders unsupported"; `channelId` → `UNSUPPORTED_FIELD`; `has_channel=false` forced.
2. **Sharkey-parity / pinetree-first additions:** `notes/edit` + `notes/versions` + `notes/recommendations` + `users/recommendation`, `notes/schedule/*` shim over drafts, `clips/follow|unfollow|followers`, `i/import-clips`, `i/move`, streaming `edited` + `pollEnded`-as-`noteUpdated`, `test-notification`/`flush`, `translate`/`fetch-rss`/`url-preview` subsets, `pinned-users → []`, `featured` simplified ranking, presence = live sockets, `UserSuspended` socket-drop hardening, credential-refused connect silent-ignore + pinetree `UNSUPPORTED_CHANNEL` error (upstream throws).
3. **Federation divergences (documented):** `Update(Note)` embedded-only (string-URI accepted-without-fetch; Sharkey fetch-and-apply differs); inbound poll shape gate degrades to poll-less (Misskey stores whatever arrives); reply model (pending-URI, never flattened); relay/quote `RE:` fallback only public→public; `show_remote_emojis` opt-in; `reject_quotes`; `NonSensitiveOnly*` passthrough; `ugcVisibilityForVisitor` default `all` vs upstream `local`; server-side word-mute (upstream frontend-only); per-host Like split; fan-out-on-read timelines (proposal §5: start here).
4. **MiAuth hardening:** `session`+`permission[]` required on `gen-token`, 30-min expiry, unsafe-callback blocklist, suspended-submit gate; `auth/session/generate` kept without app registry; signin checks password before suspension (manifest §23).

## 3. Missing / broken

### 3.1 Multi-node — MISSING (Phase 4 not begun, expected)
- `src/persistence/foundationdb/mod.rs` empty; `src/cluster/{account_directory,routing,internal_protocol}.rs` empty stubs; `src/jobs/queue.rs` empty (real queue lives in redb facet `persistence/redb/mod.rs:7033`).
- `fdb` cargo feature declared but no `use foundationdb` anywhere in `src/`; `config Cluster` hard-rejected (`config.rs:85-87`); `main.rs:26` unconditionally opens `RedbStore` + `FsBlobStore`.
- No directory/ownership primitive (`get_owner` returns local default, `set_owner` unvalidated non-empty proof), no routing, no internal protocol, no account-move/migration, no export/import backend-migration tool (existing export/import is per-user portability), no node-loss/stale-routing tests.
- Jobs multi-claimant lease is sound but multi-worker deployment untested (poll storms, `claim_next` scan contention, per-process `DestinationTracker`, in-process `Hub` has no cross-node transport).

### 3.2 Media lifecycle + storage — MISSING/BROKEN
- No expiry/GC/quotas/reconciler (plan-doc confirmed `docs/remote-media-cache-plan.md:35-39,102-138`); unbounded disk growth; per-note 64 MiB is per-job-run, not a hard total.
- Unsafe shared-blob delete: `delete_media` drops row+bytes with no refcount, non-atomically (`commands/media.rs:211-216`) — cross-owner/identical-remote bytes orphaned.
- Full-body buffering before `blobs.put` (local upload + remote fetch `remote_cache.rs:174,232-254`); needs stream-to-disk under byte cap.
- No thumbnails/transcoding/variants/emoji-avatar/static variants/dummy.png (`thumbnail_url` always `None`); no `drive/files/attached-notes|attached-chat-messages|check-existence|find|move-bulk`; no `Range`; no S3/shared storage; no backup/snapshot/DR.

### 3.3 Ops/observability — MISSING (minor)
- Logging minimal (`observability.rs:21` no-op init, ad-hoc `eprintln!`); federation debug gate works (`--debug-federation`).
- `/healthz` static `"ok"` (`mod.rs:192-194`), no DB check, no `readyz`, no metrics/prometheus/tracing (cache plan `:141-143` requires counters "without shell access" — not built).
- Config gaps: timeouts/SSRF/redirects/proxy-8 MiB/cache-budget/log knobs not in JSON (env/CLI-only or hardcoded); `PINETREE_ALLOW_PRIVATE_IPS`, `PINETREE_DEBUG_FEDERATION`, `PINETREE_REPLY_REPAIR_LIMIT` violate the "no env/CLI-only settings" rule.

### 3.4 Client-visible API gaps (all → 404 `mod.rs:9122` unless noted)
1. `drive/folders/*` + `drive` + `drive/stream`; `folderId` rejected everywhere — drive navigation broken in web client.
2. `channels/*` (16) — channel pages dead.
3. `chat/*` (~25) — chat UI dead.
4. `announcements(+/show)` + `i/read-announcement` + `promo/read` — badge never clears.
5. `following/update|update-all|list`, `following/requests/sent` — per-follow `withReplies`, bulk list, outgoing-request review absent.
6. `hashtags/*` (5) — hashtag pages dead (`notes/search-by-tag` covers direct lookup only).
7. `notes/thread-muting/*` (documented out, manifest §11).
8. `drive/files/find|move-bulk|check-existence|attached-notes|attached-chat-messages`.
9. `i/2fa/*` (8) + `my/apps` + `app/create|show` (legacy `auth/session/*` partially compensates).
10. `users/update-memo`, `notes/show-partial-bulk` (N×`show` fallback), `notifications/create`, `notes/polls/recommendation`, `notes/clips` (renamed `clips/notes` exists), admin bulk/remote/copy/import-zip emoji, `admin/roles|queue|relays|webhook|ad|announcements|federation|stats|user-ips|send-email|server-info|proxy-account|mfa|reset-password` families.

### 3.5 Streaming gaps
Missing channels (→ `UNSUPPORTED_CHANNEL`): `hashtag`, `roleTimeline`, `channel`, `drive`, `serverStats`/`queueStats`, `admin`, `clip`, `chatUser`/`chatRoom`, `reversi`/`reversiGame`. Missing `main` events: `unreadNotification`, `unreadAntenna`/`readAntenna`, `readAllAnnouncements`/`announcementCreated`, `myTokenRegenerated`, `signin`, `registryUpdated`, `driveFileCreated`, `urlUploadFinished`, `pageEvent`, `newChatMessage`. Missing broadcast `emojiAdded/Updated/Deleted`. `reacted` lacks optional `emoji` detail; `userList` ignores `withFiles/withRenotes`; `pollEnded`-as-`noteUpdated` is a pinetree extension stock clients don't handle (real expiry signal is the `pollEnded` *notification*).

### 3.6 Domain/application transition gaps (columns exist, decision fns don't)
- Follow-request accept/reject transitions + locked-account branching live in application, not domain (`is_locked`, `auto_accept_followed`, approval-seconds are bounds-checked columns only).
- Note edit/delete permission + tombstone transitions (domain has `is_deleted` read effect + `NoteEdit` shape, no `can_edit/delete()` validator); reaction unreact/idempotency (only quote-auth has idempotency); rename/move state machine (URL stable by id, but `moved_to_uri` is profile metadata, no migration); MFM sanitization (no `mfm.rs` in domain — markup guard only for media bytes; real MFM↔HTML lives in `activitypub/objects.rs:287-529`); `role_ids` stored never enforced; `pinned_page_id`/`verified_links`/`clip notesCount`/antenna `exclude_notes_in_sensitive_channel`/7-day auto-deactivation deferred; custom `:name:` passthrough with no registry/role checks (documented).

## 4. Top gaps ranked (client/ops impact)

1. No FDB adapter / directory / routing — Phase 4 has zero code; single-writer assumption baked in.
2. No shared object storage — local `FsBlobStore` blocks horizontal scale.
3. No remote-media lifecycle (GC/quotas/eviction) + unsafe shared-blob delete.
4. `drive/folders`, `channels`, `chat` absent — big web-client surfaces dead (declared out, but highest UI impact).
5. Announcements/read-announcement absent — omnipresent badge papercut.
6. Follow settings/update/list/sent absent — settings half of follows missing.
7. `hashtags/*`, thread-muting, file find/move-bulk, 2FA/apps, memos/bulk-show absent.
8. Streaming: missing channels + `unreadNotification`/`announcementCreated`/`driveFileCreated`/etc. + `reacted.emoji` + list filter params.
9. `Update(Note)` string-URI never applied; exotic polls degrade to poll-less; `Undo(Accept)` by-ID unresolvable; stamp fetch pre-txn network I/O can delay `Create`.
10. Health/readiness/metrics/backup absent; full-body buffering; env-only knobs.

## Appendix — where to look

- API: `src/api/misskey/endpoints/mod.rs` (~9122-line dispatcher), `auth.rs`, `request.rs`, `errors.rs`, `packers.rs`.
- Streaming: `src/streaming/server.rs`, `hub.rs`, `events.rs`.
- Federation: `src/federation/inbox.rs` (~2500 lines), `outbox.rs`, `delivery.rs`, `retry.rs`, `fetch_gate.rs`, `host_caps.rs`; `src/activitypub/actor.rs`, `objects.rs` (~2300), `quotes.rs`, `signatures.rs`, `webfinger.rs`.
- Domain: `src/domain/{account,note,follow,blocking,muting,reaction,emoji,visibility,policy,antenna,clip,page,quote,word_mute,instance,media}.rs`.
- Application: `src/application/{commands/{accounts,follows,blocks,notes,polls,drafts,pins,clips,pages,antennas,user_lists,favorites,media,emoji,abuse_reports,moderation,invites,exports,keys,notifications},queries.rs,ports.rs}`.
- Storage/jobs: `src/persistence/redb/mod.rs`, `src/jobs/worker.rs`, `src/cluster/*` (stubs), `src/persistence/foundationdb/*` (empty).
- Media/ops: `src/media/{storage,ingest,remote_cache}.rs`, `src/config.rs`, `src/observability.rs`, `src/main.rs`.
