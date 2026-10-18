# Misskey parity plan — backend only

Status: Draft rev.3 (2026-10-16; backend only, no frontend work; all sections LANDED + VERIFIED — see per-section status lines and `state/logs/2026-10-16-misskey-parity-complete.md`)

Rule: match upstream Misskey params, errors, and pack shapes by default.
Copy Sharkey only where Misskey has no equivalent (`rejectQuotes`, `notes/edit`).
Divergences are marked `DIVERGENCE` with a reason.

Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`), Sharkey `2025.4.7` (`./sharkey`).
Pinetree base: `src/api/misskey/endpoints/mod.rs` dispatcher, `src/domain/*`, `src/application/*`, `src/persistence/redb/*`.

## 0. Prerequisites

### 0A. Blocking / muting subsystem (required by §§1–2, 4, 11–12)

Misskey: `MiBlocking{blocker,blockee}` unique, `MiMuting{muter,mutee,expiresAt?}`, `MiRenoteMuting{muter,mutee}`; `UserBlockingService.block()` cancels requests both ways, unfollows both ways, federates `Block` for local→remote; read-time `QueryService generate*FilteringQuery` excludes blocked/muted/suspended/host-blocked authors, replies, and renotes. Thread-muting (`notes/thread-muting/*`) is out of scope for this plan.

Pinetree today: only `Follow` + `FollowRequest`, no stores, `isBlocking/isBlocked/isMuted=false` hardcoded in `packers.rs`, `renote_target_blocks_author=false` hardcoded at both create paths.

Work:
- `domain/blocking.rs`, `domain/muting.rs` + `BlockStore/MuteStore` in `application/ports.rs`.
- redb tables `blockings/mutings/renote_mutings` (FDB inherits per `docs/persistence-conformance.md`).
- Endpoints `blockings/create|delete|list`, `mute/create|delete|list`, `renote-mute/*`, `users/relation`, Misskey error codes.
- Enforce on follow (both directions), react, poll vote, pure-renote create; filter on all timeline/note reads.
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (stores, endpoints, enforcement, FeedFilter on every read; conformance Group G; lib suite green).

### 0B. Full profile store (required by §§1–4, 11–13)

`i/update.ts:141-237` has 39 params; pinetree accepts 10 (`mod.rs:1769-1798`). Other 29 return `UNSUPPORTED_FIELD`.

Add columns for: `followedMessage, lang, avatarDecorations[16], fields[16], hideOnlineStatus, publicReactions, carefulBot, autoAcceptFollowed, followApprovalLocalSeconds/RemoteSeconds, noCrawle, preventAiLearning (default true), requireSigninToViewContents, makeNotesFollowersOnlyBefore/HiddenBefore, injectFeaturedNote (default true), receiveAnnouncementEmail (default true), alwaysMarkNsfw, autoSensitive, followingVisibility, followersVisibility, chatScope, pinnedPageId, mutedWords, hardMutedWords, mutedInstances, notificationRecieveConfig (17 keys), emailNotificationTypes, alsoKnownAs[10]`. Plus §3 `rejectQuotes` and §6 `defaultReaction` (both DIVERGENCE columns, see those sections).

Behaviors, not just columns (`i/update.ts` handler): `name` trim/`""->null`/prohibited-words; `description`/`fields`/`followedMessage` MFM emoji/hashtag side effects (`updates.emojis/tags`, `hashtagService.updateUsertags`); `verifiedLinks` reset to `[]` + `verifyLink` re-check loop for `https://` field values; `avatarId/bannerId` role (`canUpdateBioMedia`) + `image/*` + blurhash checks + `null` clears (`update.ts:377-411`) — requires `drive/files/create` to exist before avatar/banner are testable; `avatarDecorations` role gating (`avatarDecorationLimit`, only decorations whose `roleIdsThatCanBeUsedThisDecoration` the user may wear); `mutedWords/hardMutedWords` `wordMuteLimit` count + RE2 regex validation + `enableWordMute` flag; `alwaysMarkNsfw` `restrictedByRole` guard; `alsoKnownAs` remote resolution (`resolveUser`, skip unresolvable, `forbiddenToSetYourself`, `uriNull`, moved-account guard); unlock accepts pending follows (`update.ts:553-568`). Fix wrong packer defaults to upstream column defaults (`preventAiLearning`, `injectFeaturedNote`, `receiveAnnouncementEmail`, `chatScope`).
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (all 39 params + behaviors; `rejectQuotes`/`defaultReaction` columns stored + packed, enforced/resolved by §§3/6 in 764ec7a/888f522).

## 1. Private followers / following

Follows Misskey `users/followers.ts:115-135`, `users/following.ts:123-143`, `UserEntityService.ts:465-473`, `ActivityPubServerService.ts:225-233,322-330`, `QueryService.ts:398-430`.
- `private` = self-only (mods bypass), `followers` = self or follower, else `FORBIDDEN`.
- Mask counts when not public. AP collections: any non-`public` → `403`, no per-requester exception.
- Keep cursor paging `sinceId/untilId/sinceDate/untilDate, limit 1-100 default 10` (already matches).
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (§9's 250-follow sweep test rides here, both directions).

## 2. Private reactions

Follows Misskey `users/reactions.ts:77-95` + pack rule `UserEntityService.ts:558`.
- New `users/reactions` endpoint; mods bypass; remote target → `IS_REMOTE_USER`; `!publicReactions && requester!=target` → `REACTIONS_NOT_PUBLIC`; blocked-by-target → silent `[]`.
- Pack `publicReactions: isLocal ? profile : false`. Filter reads with visibility + blocked-host + suspended queries.
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a.

## 3. Sharkey-style disable quotes

Sharkey-only (Misskey has zero `quoteDisabled` hits). Copy Sharkey's enforcement, add Misskey/Sharkey-missing self-service toggle:
- `User.rejectQuotes bool default false` (Sharkey `User.ts`, `default: false`), packed on `MeDetailed`/`UserDetailed` (Sharkey `UserEntityService.ts:564`).
- DIVERGENCE (self-service): `i/update` accepts `rejectQuotes: boolean`. Upstream Sharkey exposes it admin-only (`admin/reject-quotes.ts`); the requirement needs user opt-in, so pinetree adds the user-facing param. Keep `admin/reject-quotes.ts` (`write:admin:reject-quotes`, moderation log, `localUserUpdated`) for moderation. Instance-level `rejectQuotes` is out of scope.
- Enforcement copies `NoteCreateService.checkQuotePermissions()` (verified live against the pin: the gated user is the QUOTER — `createNote(user, data)` passes the note's own author at `NoteCreateService.ts:317`, and `:1179` throws on `user.rejectQuotes`): local quoter with the flag → `400 QUOTE_DISABLED_FOR_USER (1c0ea108-…)` (`notes/create.ts:147-151,423-425`, same wiring in `notes/edit.ts:477`); remote quoter → strip `renote=null` + `quoteUnavailable`, downgrade to plain note. CORRECTION (2026-10-16): rev.2 described this as "the local quote TARGET author's flag" — that inverts upstream; the implementation follows upstream (quoter-side) verbatim. The instance-level `rejectQuotes` check (Sharkey `:1191`) stays out of scope with the rest of instance-level flags.
- Status (2026-10-16): LANDED + VERIFIED — enforcement commit 764ec7a (create + edit paths, quoter-side, remote strip); verification tests commit 07d834e (conformance E6 strip/throw storage; endpoint e2e incl. `admin/reject-quotes`, self-service `i/update`, edit-path `QUOTE_DISABLED_FOR_USER`).
- Enforce in `checkQuotePermissions()` position: after renote-narrowing, before pure-renote block check. `isQuote` includes text/cw/reply/poll/files.

## 4. Polls

Follows Misskey `notes/create.ts:168-184`, `NoteCreateService.ts:405-470,659-722,819-820`, `Poll.ts`, `PollVote.ts`, `notes/polls/vote.ts`, `json-schema/note.ts:118-`.
- `poll:{choices[2..10] unique 1..50, multiple, expiresAt|expiredAfter}`, `CANNOT_CREATE_ALREADY_EXPIRED_POLL`.
- Tables `polls{noteId,choices,votes,multiple,expiresAt+denorms}`, `poll_votes UNIQUE(user,note,choice)`.
- Vote: `NO_SUCH_NOTE/NO_POLL/INVALID_CHOICE/ALREADY_VOTED/ALREADY_EXPIRED/YOU_HAVE_BEEN_BLOCKED`; `multiple` allows distinct choices; `UPDATE votes[i]+=1`; emit `pollVoted` + `deliverQuestionUpdate`.
- Pack `poll:{expiresAt,multiple,choices:[{isVoted,text,votes}]}` + `hasPoll` (pinetree currently hardcodes `has_poll:false` — fixed here). Expiry job with delay.
- Federation: parse/render `Question`, send vote activity on vote. Inbound `Update` routing is shared with §8: this slice owns only the poll-vote/`Question`-expiry branch; the `Update(Note)` edit branch lives in §8 behind the same inbox dispatcher.
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (tables, endpoint, vote errors verbatim, pack incl. `hasPoll`, expiry job, `pollVoted` + `deliverQuestionUpdate`, `Update(Question)` arm).

## 5. Drafts + scheduling (one subsystem)

Follows Misskey `NoteDraft.ts:18-167`, `notes/drafts/create|list|update|delete|count.ts`, `NoteDraftService.ts:60-338`. There is no `notes/schedule/*`; scheduling = `scheduledAt + isActuallyScheduled` on a draft.
- `NoteDraft` mirrors `Note` fields + `hasPoll/poll*/scheduledAt/isActuallyScheduled`; relaxed validation (empty text/files allowed); `write:account`, 300/hr. Ship `create|list|update|delete` plus `count`.
- Errors `TOO_MANY_SCHEDULED_NOTES/SCHEDULED_AT_REQUIRED/SCHEDULED_AT_MUST_BE_IN_FUTURE`; policy limits `noteDraftLimit/scheduledNoteLimit`.
- New `JobKind::PostScheduledDraft{draftId}` with not-before = `scheduledAt`; `update/delete` clear + reschedule (`clearSchedule` scan).
- Notifications `scheduledNotePosted/Failed`.
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (`NoteDraft` mirror + relaxed validation, `create|list|update|delete|count`, `JobKind::PostScheduledDraft` with clear/reschedule, error codes + policy limits, notifications; conformance Group S).

## 6. Default like emoji

Two parts: Misskey wire compat (required) + per-user default (DIVERGENCE extension, decided).

Wire compat follows Misskey `ReactionService.ts:34-48,125-162,383-401`: `FALLBACK='\u2764'` (bare U+2764, no VS16), `_reaction ?? FALLBACK`, `likeOnly` forces it, `legacies` map, strip `FE0F` unless ZWJ, custom-emoji permission/sensitivity checks. Pinetree uses `❤️` (U+2764+FE0F) and only handles `LikeOnly` (`src/domain/reaction.rs:6-43`) — normalize fallback to bare `❤` + map.

DIVERGENCE (decided): per-user `profile.defaultReaction`. Misskey has only per-note `reactionAcceptance`; there is no upstream per-user default (zero `defaultReaction` hits in Misskey/Sharkey backend + frontend). Semantics:
- Stored per user, set via `i/update {defaultReaction}` (nullable, same 1..=260-char validation as reactions), packed on `MeDetailed`.
- Heart/like click with no explicit reaction resolves server-side to `profile.defaultReaction ?? FALLBACK`. This is the "default like emoji" the requirement asks for.
- Federation: send `EmojiReaction` (`Like` + `_misskey_reaction`, cf. `src/activitypub/objects.rs:884-900`) to emoji-aware servers (Misskey/Sharkey/pinetree); send plain `Like` (Mastodon favourite semantics) to servers without emoji-reaction support (Mastodon, Lemmy). Inbound `Like` without `_misskey_reaction` maps to `FALLBACK`.
- Status (2026-10-16): LANDED + VERIFIED — wire compat + `i/update`/pack store commit 764ec7a; remainder commit 888f522: heart-click resolution (`notes/reactions/create` param optional, `react_with_default`, DIVERGENCE noted in the manifest), federation split (per-recipient-host `HostCapsStore` + lazy NodeInfo 2.1→2.0 probe off-transaction, 7-day TTL, unknown/failed → emoji-aware for upstream always-sends parity; `UndoLike` re-resolves), inbound no-`_misskey_reaction` → `FALLBACK` test-pinned. `defaultReaction` validation is 1..=260 chars only — pinetree has no custom-emoji registry to check permissions/sensitivity against (documented).

## 7. Pinned / featured

Follows Misskey `UserNotePining.ts`, `NotePiningService`, `RoleService pinLimit:5`, `i/pin|unpin.ts`, `UserEntityService.ts:552-553`, `notes/featured.ts`, `users/featured-notes.ts`.
- `pinnings UNIQUE(user,note)`; must own note else `NO_SUCH_NOTE`; `PIN_LIMIT_EXCEEDED`; dup `ALREADY_PINNED`; federate for local public/home.
- Pack `pinnedNoteIds/pinnedNotes` (today hardcoded `[]`).
- Featured uses one documented ranking (compatible-simplified, not upstream-identical): recent non-sensitive public notes (local + remote, no channels) scored by `reactions + renotes` over the trailing window, tie-break newest-first. Applies to both `notes/featured` (global/channel param) and `users/featured-notes {userId}`; both return `Note[]` with 3600s cache.
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (`pinnings UNIQUE(user,note)`, `i/pin|unpin` with `PIN_LIMIT_EXCEEDED`/`ALREADY_PINNED`, pack fix, federation; featured ranking shipped with 3d global / 7d per-user trailing windows, 3600s cache).

## 8. Post editing + delete-and-redraft (decided: ship Sharkey `notes/edit` shape)

Misskey has no edit (`get-note-menu.ts:215 delEdit()` = `notes/delete` + `post({initialNote…})`, warns reactions/renotes/replies are deleted). Delete-and-redraft needs no new backend (`notes/delete` + `notes/create` exist) and stays a pure client flow.

Real editing copies Sharkey `notes/edit.ts` (484 lines) + `NoteEditService.ts` (979 lines), same `quoteDisabled` wiring as §3:
- Endpoint `notes/edit {editId + create-like fields}` (`requireCredential`, `write:notes`, `prohibitMoved`, 300/hr, res `{createdNote: Note}`); errors include `NEEDS_EDIT_ID/NO_SUCH_NOTE/YOU_ARE_NOT_THE_AUTHOR/NOT_LOCAL_USER/CANNOT_QUOTE_CURRENT_NOTE/CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE/QUOTE_DISABLED_FOR_USER` plus the create-parity set (visibility, reply/renote, poll-expiry, channel, block, length).
- Service rules: author-only; `replyId` immutable (original parent kept); `visibility`/`localOnly` forced back to the original note's values (visibility change on edit is ill-defined); poll add/remove/replace resets votes; text/cw trimmed + truncated to limits; no-change edit returns the original note.
- Version history: `note_edits{noteId,oldText,newText,cw,oldDate,updatedAt}` row per effective edit (same-note no-op writes nothing).
- Side effects reuse the create path: note-stream `updated` event, `edited` notifications to reply author + interacters, AP `Update(Note)` deliver to followers + mentioned/replied/renoted/reacted remote users (+ relays when public). Owns the `Update(Note)` edit branch behind the shared inbox dispatcher (§4 owns only the poll-vote branch).
- Status (2026-10-16): LANDED + VERIFIED — implementation commit 764ec7a (`notes/edit` arm, `note_edits`/`note_edits_by_note` tables, `apply_note_edit`/`note_edits`, `handle_update_note`, `Update(Note)` deliver); verification tests commit 07d834e (conformance Group E: lifecycle/no-op/poll-reset/history-chaining/inbound-dedup; endpoint gates + reply-immutable + visibility-forced + `edited` notifications; inbox render→apply→replay-converge round-trip + authority gates). Delete-and-redraft stays client-only as decided.

## 9. Full follower list

No cap to remove: both sides page `limit 1-100 default 10`. "Full" = cursor walk with `sinceId/untilId/sinceDate/untilDate` (pinetree `src/application/queries.rs:753-771` `user_followers/user_following` already do newest-first). Only missing piece is the §1 visibility gate. Add a 250-follow sweep test for no dupes/gaps (both directions).
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (sweep test included, both directions).

## 10. Notification grouping

Follows Misskey `i/notifications-grouped.ts:46-64,106-153`, `Notification.ts:147-162`, `types.ts:51-55`.
- Same params as `i/notifications`; group consecutive `reaction` on same `noteId` → `reaction:grouped{reactions:[{userId,reaction}]}`; consecutive `renote` on same target → `renote:grouped{userIds[]}`.
- Pure pack-layer change; no storage change. Today returns `UNKNOWN_API_ENDPOINT` (explicit OUT).
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (supersedes the `UNKNOWN_API_ENDPOINT` note: `i/notifications-grouped` now ships with the same params as `i/notifications`).

## 11. Notification filtering

Follows Misskey three layers (`NotificationService.ts:97-148`, `UserProfile.ts:252-272`, `types.ts:28-57`):
1. Creation-time receive config + read-time type filter. Creation: `notificationRecieveConfig[type] = never|following|follower|mutualFollow|followingOrFollower|list(userListId)` enforced at `Notify` creation. Read: `includeTypes/excludeTypes` over the 20 `notificationTypes`; drop obsolete `pollVote/groupInvited`. Pinetree has 6 + 13 `is_unemitted` — shrink the list as polls/scheduling/exports land.
2. `mutings.has(notifier)` drops; self-notify drops.
- Requested presets map to existing machinery, no second list: blacklist = `mutings`, whitelist-only = `list(userListId)` pointing at a user-owned list, mutuals-only = `mutualFollow`, followed-only = `following`, followers-only = `follower`, either-direction = `followingOrFollower`.
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (creation-time receive-config + read-time include/exclude + muting/self drops); the `exportCompleted` type gap closed by commit 888f522 (§13). DIVERGENCE (documented): pinetree has no user-list subsystem, so `list(userListId)` configs are accepted on write but drop notifications at creation time (upstream missing-list parity — `notifications.rs` gate, never errors).

## 12. Timelines: replies, blocks, mutes

Follows Misskey `timeline.ts:206-214`, `local-timeline.ts:119-189`, `hybrid-timeline.ts`, `global-timeline.ts:70-98`, `QueryService.ts:92-390`, `NoteEntityService.ts:131-328`.
- DIVERGENCE (decided): home has no `withReplies` param and includes replies from followed users (the requirement). Misskey parity would be `replyId IS NULL OR (reply AND replyUserId==me)`; pinetree intentionally keeps the current `src/application/queries.rs:149-190` behavior (followed authors in scope, including their replies) and only adds the §0A filters below.
- Local/hybrid keep Misskey parity: `withReplies default false` but keep self-replies (`replyUserId == authorId`); fix pinetree `src/application/queries.rs:191-302`, which today drops all replies when `with_replies=false`, to keep the self-reply exception.
- Global: `visibility=public AND channelId IS NULL`, no `withReplies`, carries replies (matches both Misskey and pinetree `global_timeline`).
- Apply §0A filters on every timeline/note read: blocked/muted/suspended authors, blocked hosts, `mutedInstances`, renote-mutes.
- Word mutes are client-side only (decided, Misskey parity): backend stores `mutedWords/hardMutedWords/mutedInstances` (§0B) and ships them via `MeDetailed`; no server CW injection or hiding. Upstream reference is `frontend/src/utility/check-word-mute.ts` + `use-note.ts`, not `NoteEntityService.hideNote()` (which covers `specified`/`followers` visibility only).
- Status (2026-10-16): LANDED + VERIFIED — commit 764ec7a (home DIVERGENCE kept as decided; local/hybrid self-reply exception fixed; `mutedInstances` + renote-mutes applied on every read; §0A filters wired everywhere incl. streaming capture gates).

## 13. Profile import / export

Follows Misskey `i/export-following|blocking|mute|notes|antennas|clips|favorites|user-lists` + `import-blocking|muting|following|antennas|user-lists` (`core/QueueService.createExport*/Import*Job`), `exportCompleted` notification. Export endpoints are `secure:true`, `1/hr`. (`customEmoji` appears in the `userExportableEntities`/`userImportableEntities` enums but has no dedicated `i/export|import` endpoint in this pin — out of scope.)
- Order: `export-following|blocking|mute|notes` first (`export-following` keeps `excludeMuting/excludeInactive` params; Misskey-compatible CSV/JSON payloads per entity), then lists/antennas/clips/favorites.
- New `JobKind::Export|Import` reusing lease/backoff; completion via `exportCompleted` (also closes a type gap in §11).
- `import-blocking` guards (`import-blocking.ts:53-86`): size/type checks, per-row validate, skip unresolvable, report counts. Same guard shape for `import-muting|following|antennas|user-lists`. Notes export = own notes only. Profile fields themselves are not part of Misskey export/import (they move via `i/update`, §0B).
- Status (2026-10-16): LANDED + VERIFIED — commit 888f522 (all 8 exports + 5 imports, `JobKind::Export|Import`, `exportCompleted` notification, verbatim per-endpoint import UUIDs, manifest §20 documents payloads + DIVERGENCEs). CORRECTION: upstream `export-mute` writes `mute-*.csv` and `import-muting` consumes CSV (`ImportMutingProcessorService` errors "We need csv file.") — the "export-mute … JSON" wording in this section was a plan slip; shipped as CSV, upstream-verbatim. DIVERGENCE (decided): `excludeInactive` accepted+ignored (no last-active tracking); locals export bare `username`; remote resolution store-only; no-subsystem entities (lists/antennas/clips/favorites) export empty valid files and import zero rows; drive upload dedups on content (`force==false`) so retried exports converge.

## 14. Moderation and daily-use batch (Batch M, pre-Phase 4)

Guide: `docs/implementer-guide-batch-m.md` (status lines there are the spec; this section records what landed). Reference pins: Misskey `2026.10.1-alpha.0`, Sharkey `2025.4.7`. Per-WP status, newest session first.

- §3 foundations (roles `is_admin`/`is_moderator`, `ModerationLogStore` + `InstanceSettingsStore` ports with compound atomic methods, redb tables, CLI `set-role`, `require_moderator`/`require_admin`, real packed `isAdmin`/`isModerator`, `viewer_is_moderator` wired) + conformance Group H1–H4.
- WP1 — roles, guards, moderation log, `admin/show-moderation-logs`, and the `admin/reject-quotes` auth fix (live hole: any first-party credential could flag any account).
  - Status (2026-10-17): LANDED + VERIFIED — commit e331d4f (`cargo test --offline` 532/532 green: 410 lib + 94 conformance incl. 4 new Group H cases + 38 integration; see `state/logs/2026-10-17-batch-m-foundations-wp1.md`).
  - DIVERGENCEs (decided): log action names match upstream where they exist — `updateServerSettings` (guide §3.2 says `updateMeta`), `createInvitation` (guide: `createInvite`), and Sharkey's `acceptQuotesUser`/`rejectQuotesUser` for the quote toggle (guide: `updateUserRejectQuotes`); moderator-target refusals answer `400 INVALID_PARAM` carrying upstream's `cannot moderate moderator account` message (upstream throws an untyped `Error` → `500 INTERNAL_ERROR` generic, `ApiCallService.ts:#onExecError`); CLI `setRole` rows carry `user`/`userId` = `null` (upstream rows always have a user); `admin/show-moderation-logs` is moderator-only (upstream `requireAdmin`).
- WP2 — registration policy, invites, `admin/accounts/create` fix: LANDED (2026-10-17) — `InviteStore` port (create/list/delete + `consume_and_create_account`: claim + account create + password hash in ONE transaction), `AppServices::signup` (policy gate + claim; upstream's plain empty 400 for gate rejections), `invite/create|list|delete|limit`, `admin/invite/create|list` (moderator, `createInvitation` row), `admin/meta` + `admin/update-meta` (`disableRegistration`, `blockedHosts` stored — enforced WP5), `admin/accounts/create` admin-only + gate bypass + `createAccount` row, conformance Group I1–I4 green (98/98), 4 endpoint tests green (`cargo test --offline` 540/540: 414 lib + 98 conformance + 28 integration). Details + DIVERGENCES (ticket `id` beyond the guide's store sketch; `deleteInvite` row where upstream logs nothing; `setupPassword` accepted-and-ignored; `EXCEEDED_LIMIT_OF_CREATE_INVITE_CODE` unused under the uncapped default policy): `docs/endpoint-manifest.md` §22 and `state/logs/2026-10-17-batch-m-wp2.md`.
- WP3 — suspension (setter + stream close + bounded undo-follow fan-out): PENDING. Open item: guide WP3 says one Undo-Follow job per outgoing follow in the suspend transaction, which is unbounded against `MAX_JOBS_PER_TXN` (500) and the FDB txn cap — plan is one sweep job the worker expands into per-recipient deliveries (§11 transaction-size rule).
- WP4 — abuse reports + Flag forwarding: PENDING.
- WP5 — instance blocks (`host_is_blocked` suffix-match rule already in `domain/instance.rs`): PENDING.
- WP6 — admin read/user views: PENDING.
- WP7–WP14 — favorites, emoji (+ WP8b remote emoji opt-in), self-service, follower removal, `followRequestAccepted`, `url-preview`, drive extras, self-delete: PENDING (conformance Group J lands with WP7/WP8).
- WP15 antennas, WP16 user lists: PENDING.
- §7 smoke-gate fix, §6 remaining docs (phase-3-profile §5 note, testing.md, wiki, AGENTS counts): PENDING, batch end.

## Build order

1. §0A + §0B — unblocks everything (§0B includes the `rejectQuotes`/`defaultReaction` columns for §§3/6).
2. §§1–2 privacy gates (small, high value; need §0A + §0B).
3. §4 polls (adds types/notifications used elsewhere; owns only the poll-vote/`Question` branch of inbound `Update`).
4. §5 drafts/scheduling, §7 pins, §10 grouping (independent, parallelizable).
5. §11 filtering + §12 timelines (needs §0A + §0B).
6. §3 quote-disable (needs §0B column; enforcement touches create + edit paths).
7. §8 notes/edit (needs §4 poll shapes for poll edits; owns the `Update(Note)` edit branch behind the shared inbox dispatcher from §4).
8. §13 import/export last.
9. §6 emoji normalization rides with §4 (reaction path); per-user `defaultReaction` rides with §0B store. §9 sweep test rides with §1.

Per slice: domain type → `ports.rs` trait → redb impl (FDB inherits) → application op (single txn state+jobs, 64KiB cap, 500/txn) → route + packer → federation out/in if needed → conformance cases + `bash scripts/smoke-all.sh`.
