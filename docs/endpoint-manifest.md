# Misskey endpoint + streaming manifest (initial — Phase 0)

**Status:** Frozen (Phase 0 freeze review 2026-10-14), extended by
superseding Phase 3 decision below (2026-10-16)

**Status (2026-10-20 freshness pass):** contract unchanged; superseding notes
are inserted at the top of each affected section, and the code-verified
corrections with `file:line` evidence are in §39 at the end. Where a section
body and a later note disagree, the note wins. Frozen text is not rewritten.
**Sources:** `sns-server-proposal.md` §6; Misskey checkout (read-only), verified this session: `endpoints/notes/create.ts:19-214`, `const.ts:6,20`, `ApiServerService.ts:157`, endpoint survey (meta, auth, accounts, follows, timelines) and streaming survey (`StreamingApiServerService.ts:42-159`, `stream/Connection.ts`, `stream/channel.ts`, timeline channels).

Phase 0 scope = proposal §6 step 1 (core account/note flows) + step 2 basics (auth'd sockets, timeline channels, note capture). Richer fields, remaining channels, and polls/quotes arrive in Phase 3 driven by real client tests. Anything not listed here is **unsupported**: the server answers it per §8, never with an accidental shape.

## 1. Transport and auth conventions

- JSON over POST at `/api/{endpoint}`. Credentials via the `i` body parameter or `Authorization: Bearer` (proposal §6).
- Auth declaration per endpoint: `requireCredential` + permission `kind` (`read:account`, `write:notes`, `write:following`, `write:account`). throws `authenticate` failures as 401 `invalid_token`; suspended callers get 403.
- IDs on the wire match `misskey:id` (`/^[a-zA-Z0-9]+$`); our IDs are `aidx` (identity rules §2).
- Rate limits are per-endpoint fixed windows (initial values §7, instance-configurable).
- Error envelope (M `ApiCallService.ts:306-325`, `error.ts:6-32`): `{ error: { message, code, id, kind, info? } }` with HTTP status mapped from `kind` (`401` auth, `403` permission, `404` unknown endpoint, `413` payload too large, `429` rate limit with `Retry-After`, `400` client/validation, `500` internal) and stable per-endpoint `code`/`id` idents (e.g. `NO_SUCH_NOTE`, `ALREADY_FOLLOWING`). Unknown endpoints → 404 (`UNKNOWN_API_ENDPOINT`), unknown fields on listed endpoints → 400 `UNSUPPORTED_FIELD` naming the field in `info.param`. Void endpoints return `204 No Content`.

## 2. Instance and auth

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `meta` | none | `detail: bool = true` | instance metadata (detailed or lite). Our detailed set is fixed at implementation; unknown future keys are additive-only |
| `miauth/gen-token` | credential, `secure` | `session*`, `permission: string[]*`, `name?`, `description?`, `iconUrl?` | `{ token }` (32-char secure random, cf. reference) |
| `POST /api/miauth/:session/check` (also `/miauth/:session/check`) | none (session token in path) | — | polls approval; returns `{ ok: true, token, user: UserDetailedNotMe }` once (`fetched` one-shot flip), else `{ ok: false }` (M `ApiServerService.ts:157-177`) |
| `GET /miauth/:session` | web UI | query `name?`, `icon?`, `callback?`, `permission?` (comma-separated) | Server-rendered login + approve form; on approve mints token and redirects to `{callback}?session={session}` (rejects `javascript:/file:/data:/mailto:/tel:/vbscript:` schemes, M `pages/miauth.vue:52-66`) |
| `POST /signup` (also `/api/signup` + `/api/admin/accounts/create`) | none (or setupPassword when configured) | `username*` (`^\w{1,20}$`), `password*` | `MeDetailed` (with secrets) merged with `{ token }` (M `SignupApiService.ts:249-257`, `admin/accounts/create.ts:103-116`) |
| `POST /signin` (also `/api/signin`) | none | `username*`, `password*` | `{ id, token, i: token, user: MeDetailed }` + sets session cookie for web MiAuth approval |
| `auth/session/generate` | none | `appSecret*` | `{ token, url }` (legacy auth flow) |
| `auth/session/show` | none | `token*` | `{ id, app, token }` |
| `auth/session/userkey` | none | `appSecret*`, `token*` | `{ accessToken, user }` |
| `auth/accept` | credential, `secure` | `token*` | binds session to caller (void) |

OAuth is deferred (proposal §6: add only if selected clients require it).

## 3. Accounts

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `i` | `read:account` | none | `MeDetailed` of caller |
| `i/update` (full profile store) | `write:account`, 20/h | all 39 upstream params (see §12) + DIVERGENCE `rejectQuotes?`/`defaultReaction?` | updated `MeDetailed` + `meUpdated` stream event |
| `users/show` | none | exactly one of `userId`, `userIds[]`, or `username` (+ optional `host`, null = local) | `UserDetailed` (or array) |
| `users/search` | none | `query*`, `offset = 0`, `limit` 1–100 (= 10), `origin` = combined, `detail` = true | `User[]` (Detailed or Lite) |
| `username/available` | none | `username*` (`^\w{1,20}$`) | `{ available }` (free + untombstoned + unpreserved) |

`i/update` accepts the full profile store (§12); only genuinely unknown params → 400 `UNSUPPORTED_FIELD` naming the field (§8). `description`/`location`/`birthday` live on the Account type with the bounds above.

## 4. Notes

`notes/create` (credential `write:notes`, 300/h; response `{ createdNote: Note }`):

| Param | Rule (verified M `endpoints/notes/create.ts:129-214`, `const.ts:6`) |
|---|---|
| `visibility` | `public\|home\|followers\|specified`, default `public` |
| `visibleUserIds` | id[], required iff `specified` |
| `text` | 1–3000 chars, nullable; required (non-blank) when no renote/files/poll |
| `cw` | 1–100 chars, nullable |
| `replyId` / `renoteId` | id, nullable; `channelId` → rejected (`UNSUPPORTED_FIELD`) |
| `fileIds` / `mediaIds` | 1–16 unique ids; `fileIds ?? mediaIds` — when `fileIds` is present it wins outright (`notes/create.ts:226`), `mediaIds` is a legacy fallback only |
| `localOnly`, `noExtractMentions`, `noExtractHashtags`, `noExtractEmojis` | bool, default false |
| `reactionAcceptance` | null + 4 values (domain types §2), default null |
| `poll` | §4 polls (`docs/misskey-parity-plan.md` §4): `{ choices[2..10] unique 1..50 chars, multiple = false, expiresAt? \| expiredAfter? (ms, `expiredAfter` wins) }`; past expiry → `CANNOT_CREATE_ALREADY_EXPIRED_POLL`; a poll stands in for `text` (see §15) |

Errors (stable idents): `NO_SUCH_RENOTE_TARGET`, `NO_SUCH_REPLY_TARGET`, `CANNOT_RENOTE`, `YOU_HAVE_BEEN_BLOCKED`, `NO_SUCH_FILE`, `CONTAINS_PROHIBITED_WORDS`, `CANNOT_CREATE_ALREADY_EXPIRED_POLL`, plus `UNSUPPORTED_FIELD`/`VALIDATION`.

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `notes/show` | none | `noteId*` | `Note` (detail pack); invisible → `NO_SUCH_NOTE`-class error, never a null body |
| `notes/delete` | `write:notes`, 300/h | `noteId*` | void; non-owner/non-moderator → `ACCESS_DENIED` |
| `following/create` | `write:following`, 100/h | `userId*`, `withReplies = false` | `UserLite` followee; `NO_SUCH_USER`, `FOLLOWEE_IS_YOURSELF`, `ALREADY_FOLLOWING`, `BLOCKING`/`BLOCKED` |
| `following/delete` | `write:following`, 100/h | `userId*` | `UserLite` followee; `NO_SUCH_USER`, `FOLLOWEE_IS_YOURSELF`, `NOT_FOLLOWING` |
| `notes/reactions/create` | `write:reactions` | `noteId*`, `reaction?` (string ≤260; absent/null → reactor's `defaultReaction ?? ❤` per the superseding note below; DIVERGENCE — upstream `notes/reactions/create.ts:19-74` requires `reaction`) | `204 No Content`; `NO_SUCH_NOTE`, `ALREADY_REACTED`, `YOU_HAVE_BEEN_BLOCKED`, `CANNOT_REACT_TO_RENOTE` (M `notes/reactions/create.ts:19-74`; `likeOnly` coerces to the bare-heart fallback `❤` (U+2764, no VS16) and every other reaction is normalized (legacies map, FE0F strip unless ZWJ) per `ReactionService.ts:34-48,125-162,383-401` — see §15) |
| `notes/reactions/delete` | `write:reactions`, 60/h (min 3 s interval) | `noteId*` | `204 No Content`; `NO_SUCH_NOTE`, `NOT_REACTED` (M `notes/reactions/delete.ts:18-46`) |
| `notes/reactions` (2026-10-17; index 2026-10-20) | none (anonymous ok) | `noteId*`, `type?` (`:name@.:` = local `:name:`), `limit` 1..100 (default 10), `sinceId`/`untilId`/`sinceDate`/`untilDate` | `NoteReaction[]` `{id, createdAt, user: UserLite, type}`, newest first (DIVERGENCE: `sinceId`-only also reads newest-first; upstream `QueryService` orders ASC for that cursor shape); `NO_SUCH_NOTE` when missing or invisible/hidden to the viewer (M `notes/reactions.ts`). Gate is `notes/show` visibility **plus** mute/block/suspend hiding (stricter than Misskey, which applies visibility only; Sharkey instead filters reactors by mute/block with no note gate). Reads via the per-note `REACTIONS_BY_NOTE` range (`(note_id, reaction_id)`, backfilled at open; same key shape reserved for FDB); `type` packs verbatim (write-time normalization per §15 covers the upstream `convertLegacyReaction` cases). |
| `notes/polls/vote` (§15) | credential `write:votes` | `noteId*`, `choice*` (integer) | `204 No Content`; `NO_SUCH_NOTE`, `NO_POLL`, `INVALID_CHOICE`, `ALREADY_VOTED`, `ALREADY_EXPIRED`, `YOU_HAVE_BEEN_BLOCKED` (all exact upstream code/message/id) |
| `drive/files/create` | `write:drive`, 120/h | multipart/form-data (`file*`, `name?`, `comment?`, `isSensitive = false`, `force = false`, `folderId?`, `i?`) or JSON | `DriveFile` (packed `self: true`); `INVALID_FILE_NAME`, `MAX_FILE_SIZE_EXCEEDED` (413), `UNALLOWED_FILE_TYPE`. Public bytes served at `GET /files/:id` (and `/files/:id/:name` → 301 canonical, M `FileServerService.ts:93-99`, `DriveFileEntityService.ts:126`) |

## Superseding note — default like emoji (2026-10-16)

`notes/reactions/create` leaves `reaction` optional (DIVERGENCE from upstream Misskey, where `reaction` is required): an absent/null param resolves server-side to the reactor's `defaultReaction ?? ❤` (bare U+2764, no VS16), then runs the standard reaction pipeline (legacy-name map, FE0F strip unless ZWJ, `likeOnly` coercion) per `docs/misskey-parity-plan.md` §6. Set via `i/update {defaultReaction}` (nullable, 1..=260 chars), packed on `MeDetailed`. Federation: outbound `Like` renders emoji-aware (`Like` + `_misskey_reaction`) or plain per recipient-host software (lazy NodeInfo probe, ~7-day cache, unknown/failed → emoji-aware); inbound `Like` without `_misskey_reaction` maps to `❤`. Akkoma/Pleroma-family `EmojiReact` (2026-10-20): inbound accepted onto the same per-(user,note) row (`content`, else `Emoji`-tag `name`; `Undo(EmojiReact)` unreacts); outbound to akkoma/pleroma stays bare `Like` (favourite-only) until a live round-trip verifies an `EmojiReact` emit shape.

Timelines (all → `Note[]`, cursor pagination via `sinceId`/`untilId` + `sinceDate`/`untilDate`, `limit` 1–100 default 10):

| Endpoint | Auth | Params |
|---|---|---|
| `notes/timeline` (home) | `read:account` | `withFiles = false`, `withRenotes = true`, `includeMyRenotes/includeRenotedMyNotes/includeLocalRenotes = true`, `allowPartial = false` |
| `notes/local-timeline` | none | `withFiles = false`, `withRenotes = true`, `withReplies = false` |
| `notes/hybrid-timeline` | `read:account` | union of the above |
| `notes/global-timeline` | none | `withFiles = false`, `withRenotes = true` (no `withReplies` — replies included, per M `global-timeline.ts` paramDef; `allowPartial` accepted and ignored) |

## Superseding note — global timeline (2026-10-15)

`notes/global-timeline` joins the frozen surface: no auth, `public` + non-`local_only` notes local and remote (visibility-policy §5, `is_eligible_for_federated_timeline`), newest-first with the standard cursor params. Unknown params (e.g. `withReplies`) → 400 `UNSUPPORTED_FIELD` per §8.

## 5. Response shapes (fixed field sets)

- `Note`: all domain-types §2 fields + Misskey 2026.10 required keys (`id`, `createdAt`, `userId`, `user: UserLite`, `text`, `cw`, `visibility`, `localOnly`, `reactionAcceptance`, `renoteCount`, `repliesCount`, `reactionCount`, `reactions`, `reactionEmojis`, `fileIds`, `files`, `replyId`, `renoteId`, `reply`, `renote`, `mentions`, `visibleUserIds`, `tags`, `emojis`, `myReaction`), rendered per visibility policy (tombstones/deleted bodies never leak); `user` packed inline (`UserLite`), `files` from `file_ids`, `reply`/`renote` one level unless `detail`.
- `Note` quote extensions (Pinetree informational, non-standard; `docs/quote-posts.md`): present on quotes only, never on ordinary pure renotes. `quoteAuthorizationStatus` (`pending\|approved\|rejected\|revoked\|unapproved\|unknown`, `approvedByException` folds into `approved`); `renote`/`renoteId` keep the Misskey shape for the quoted note when safe and available; `quoteTargetUri` when the relationship is recognized but unresolved and the URI is safe to disclose (never fabricates `renoteId`); `quoteTargetUnavailable: true` when the target cannot safely be exposed (protected/invisible target — `renote` is null, no URI leak). Authorization never overrides visibility; rejected/unapproved quotes still render subject to normal visibility rules.
- `UserLite` / `UserDetailedNotMe` / `MeDetailed`: domain-types §1 + Misskey 2026.10 required keys (`id`, `name`, `username`, `host`, `avatarUrl`, `avatarBlurhash`, `avatarDecorations`, `isBot`, `isCat`, `emojis`, `onlineStatus`, plus detailed fields `url`, `uri`, `createdAt`, `updatedAt`, `lastFetchedAt`, `bannerUrl`, `bannerBlurhash`, `isLocked`, `isSilenced`, `isSuspended`, `description`, `location`, `birthday`, `lang`, `fields`, `verifiedLinks`, `followersCount`, `followingCount`, `notesCount`, `pinnedNoteIds`, `pinnedNotes`, `pinnedPageId`, `pinnedPage`, `publicReactions`, `followingVisibility`, `followersVisibility`, `chatScope`, `canChat`, `roles`, `memo`, and `MeDetailed` flags/counts/policies; secrets `email`/`emailVerified`/`securityKeysList` only when `includeSecrets` is true on first-party session/signup).
- `DriveFile`: Misskey 2026.10 required keys (`id`, `createdAt`, `name`, `type`, `md5`, `size`, `isSensitive`, `blurhash`, `properties`, `url`, `thumbnailUrl`, `comment`, `folderId`, `folder`, `userId`, `user`).
- Shapes are additive-only after freeze: new nullable fields may appear, existing fields never change type or vanish.

## 6. Streaming (Phase 0 subset)

> **Superseded 2026-10-20 (§39):** the `antenna` and `userList` channels are
> shipped (`src/streaming/server.rs:341-345`); `updated` is emitted
> (`server.rs:700-728`). The "explicitly unsupported in Phase 0" bullet below is
> historical.

- Connectorbidity: WebSocket `/streaming?i={token}` (or `Authorization: Bearer`); suspended → 403, bad token → 401 (M `StreamingApiServerService.ts:42-100`). Envelope client→server: `{ type, body }` with `connect/disconnect/readNotification/subNote(s,sr)/unsubNote(un)`; server→client channel envelope `{ type: 'channel', body: { id, type, body } }`, note updates as top-level `noteUpdated`. Limits: 32 channels/connection, 1536 captured notes (M `Connection.ts:47-48,62`); idle reap 2 min with 60 s pings.
- Supported channels (params → emits `note`, filtered by visibility policy + mute/block rules):

| Channel | Auth | Params |
|---|---|---|
| `main` | `read:account` | none → account events (`notification`, `mention`, `reply`, `renote`, `follow`, `meUpdated`, …) |
| `homeTimeline` | `read:account` | `withRenotes = true`, `withFiles = false` |
| `localTimeline` | none | `withRenotes = true`, `withReplies = false`, `withFiles = false` |
| `hybridTimeline` | `read:account` | union above |
| `globalTimeline` | none | `withRenotes = true`, `withFiles = false` |

- Note capture: `subNote { id }` / `unsubNote` (aliases `s`/`sr`/`un`); events `reacted/unreacted/deleted/updated` + `pollVoted { choice, userId }` (§15) and `pollEnded` (§15, armed at `expiresAt`); capture applies the light visibility gate (specified/followers checks, M `Connection.ts:231-254`).
- Explicitly unsupported in Phase 0 (→ `UNSUPPORTED_CHANNEL` on connect): `userList`, `hashtag`, `antenna`, `roleTimeline`, `channel`, `drive`, `chat*`, `reversi*`, `clip`, `admin`, `serverStats`, `queueStats`. Added in Phase 3 per client-test demand.

## 7. Rate limits (initial values, instance-configurable)

`i/update` 20/h · `notes/*` 300/h (delete: min 1 s interval) · `following/*` 100/h. Exceeding → rate-limit error with retry info, never silent drops.

## 8. Unsupported behavior (documented responses)

- Unlisted endpoint → 404. Unlisted channel → `UNSUPPORTED_CHANNEL`. Unlisted param on a listed endpoint → 400 `UNSUPPORTED_FIELD` naming it. Quotes (Phase 3+), channels, OAuth, Mastodon REST API → the corresponding documented error, never partial behavior. (Polls shipped in §15.)

## 9. Superseding note — Phase 3 compatibility profile (2026-10-16)

> **Superseded 2026-10-20 (§39):** the "`updated` is not emitted" sentence below
> is wrong (`src/streaming/server.rs:700-728`). The "explicitly out" list is
> stale for antennas (`mod.rs:5268`), clips (`:3489`), pages (`:3944`), lists
> (`:3152`), featured (`:8998`, `:9029`), mentions (`:7784`), and user-list
> timelines (`:4121`), which are all shipped. Still out: drive folders,
> channels, chat, Reversi, the hashtag timeline, and the `roleTimeline`,
> `serverStats`, and `queueStats` channels.

The declared profile lives in `docs/phase-3-profile.md` (frozen); it
extends this manifest without contradicting it:

- Streaming §6 is now implemented per profile §2 (authenticated WS
  `/streaming`, the five channels, note capture, notification events fed
  from `StreamingSink` jobs). Wire definition: unknown channel on connect
  → `{"type":"error","body":{"code":"UNSUPPORTED_CHANNEL","channel":name,
  "id":id}}`, channel not added. Auth-refused credential channels are
  silently ignored (Misskey `Connection.connectChannel` parity).
  `pollVoted`/`pollEnded` are now emitted (§15 supersedes the deferral);
  `updated` is not
  emitted (no note-editing operation exists).
- New endpoints per profile §3: `users/notes`, `notes/replies`,
  `notes/renotes`, `notes/children`, `notes/conversation`,
  `notes/unrenote`, `users/followers`, `users/following`
  (`Following[]` with `follower`/`followee` populated — Misskey shape),
  `drive/files`, `drive/files/show|update|delete`, extended
  `i/notifications` (cursor, `markAsRead`, type filters, default limit 10),
  `notifications/mark-all-as-read`, `i/revoke-token`, `notes/search`,
  `notes/search-by-tag`.
- Explicitly out (profile §5, as amended by §§13–18): antennas, drive folders,
  clips, channels, chat, pages, Reversi, lists, featured/mentions/user-list timelines,
  `signin-flow`, OAuth, Mastodon API. Polls/vote shipped in §15. (`notifications-grouped` shipped in §18 and is no longer out.)

## 10. Superseding note — branched replies (2026-10-16, `docs/replies.md`)

- `notes/replies` keeps its identical wire contract and paging (cursor + `limit` 1–100, default 10; flat `Note[]` of direct children only, `reply_id == noteId`, newest-first). It is now served by an indexed direct-children lookup with a bounded visibility-aware scan loop instead of a global scan. Unresolved (pending-parent) replies never appear in `notes/replies` of any parent until linked to a resolved local note ID.
- Pending replies (while `reply_parent_uri` is set) are withheld fail-closed from every ordinary read surface: `notes/show`, `users/notes`, `notes/search` / `notes/search-by-tag`, all timelines, and streaming channels. They are never rendered as root notes and their pending URI never appears in packs.
- Every `withReplies=false` filter (timelines, `users/notes`, streaming `localTimeline`/`hybridTimeline`) treats a pending reply as a reply (`is_reply()` covers resolved and pending), so pending replies are excluded wherever replies are excluded.
- `notes/children` (replies + quote-renotes) and `notes/conversation` (visible ancestor chain, oldest-first, cycle/depth-guarded) semantics are unchanged.

## 11. Superseding note — §0A blocking / muting (2026-10-16, `docs/misskey-parity-plan.md`)

New endpoints (Misskey params, error codes, and pack shapes; thread-muting stays out):

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `blocking/create` | `write:blocks`, 20/h | `userId*` | `UserDetailedNotMe` blockee; `BLOCKEE_IS_YOURSELF`, `NO_SUCH_USER`, `ALREADY_BLOCKING` |
| `blocking/delete` | `write:blocks`, 100/h | `userId*` | `UserDetailedNotMe` blockee; `BLOCKEE_IS_YOURSELF`, `NO_SUCH_USER`, `NOT_BLOCKING` |
| `blocking/list` | `read:blocks` | cursor, `limit` 1–100 default 30 | `Blocking[]` (`{id, createdAt, blockeeId, blockee}`) |
| `mute/create` | `write:mutes`, 20/h | `userId*`, `expiresAt?` (future ms, `null` = indefinite; past = silent no-op) | `204 No Content`; `MUTEE_IS_YOURSELF`, `NO_SUCH_USER`, `ALREADY_MUTING` |
| `mute/delete` | `write:mutes` | `userId*` | `204 No Content`; `MUTEE_IS_YOURSELF`, `NO_SUCH_USER`, `NOT_MUTING` |
| `mute/list` | `read:mutes` | cursor, `limit` 1–100 default 30 | `Muting[]` (`{id, createdAt, expiresAt, muteeId, mutee}`) |
| `renote-mute/create` | `write:mutes`, 20/h | `userId*` | `204 No Content`; same error set as `mute/create` |
| `renote-mute/delete` | `write:mutes` | `userId*` | `204 No Content`; same error set as `mute/delete` |
| `renote-mute/list` | `read:mutes` | cursor, `limit` 1–100 default 30 | `RenoteMuting[]` (`{id, createdAt, muteeId, mutee}`) |
| `users/relation` | `read:account` | `userId*` (string or string array) | always an array of `{id, isFollowing, hasPendingFollowRequestFromYou, hasPendingFollowRequestToYou, isFollowed, isBlocking, isBlocked, isMuted, isRenoteMuted}` |

`UserDetailedNotMe` relation flags (`isBlocking/isBlocked/isMuted/isRenoteMuted`) are now real lookups. `following/create` keeps its `BLOCKING`/`BLOCKED` errors (now enforced atomically in storage, both directions); `notes/reactions/create` keeps `YOU_HAVE_BEEN_BLOCKED` (block checked before visibility, Misskey `ReactionService` parity). Federation: local→remote blocks deliver `Block` (`{url}/blocks/{id}`) and unblocks deliver `Undo(Block)`; inbound `Block`/`Undo(Block)` apply the same atomic teardown via the dedup record path.

## 12. Superseding note — §0B full profile store (2026-10-16, `docs/misskey-parity-plan.md`)

`i/update` accepts all 39 upstream params (`i/update.ts:141-237`) plus DIVERGENCE `rejectQuotes` (§3) and `defaultReaction` (§6):

| Param | Rule (verified M `i/update.ts`, `models/User.ts`, `models/UserProfile.ts`) |
|---|---|
| `name` | 1–50 chars, trimmed, `""` → null; prohibited words → `YOUR_NAME_CONTAINS_PROHIBITED_WORDS` (422; shared instance list pending the name-specific list + moderator bypass) |
| `description`/`location`/`followedMessage`/`lang` | nullable; 1–1500 / 1–50 / 1–256 / 1–32 chars (`lang` permissive pending langmap enum) |
| `birthday` | nullable `YYYY-MM-DD` calendar date |
| `fields` | max 16 `{name, value}`; blank-after-trim rows dropped |
| `avatarId`/`bannerId` | nullable file id, must exist + be owned + `image/*` (`NO_SUCH_AVATAR`/`NO_SUCH_BANNER`, `AVATAR_NOT_AN_IMAGE`/`BANNER_NOT_AN_IMAGE`); null clears; `canUpdateBioMedia` role gate defaults to allow (no roles infra) |
| `avatarDecorations` | max 16, `angle` ±0.5 / offsets ±0.25; count over the default `avatarDecorationLimit` (1) → `RESTRICTED_BY_ROLE`; decoration-registry filtering deferred (no registry) |
| `isLocked`/`isBot`/`isCat`/`isExplorable`, `hideOnlineStatus`, `publicReactions`, `carefulBot`, `autoAcceptFollowed`, `noCrawle`, `preventAiLearning`, `requireSigninToViewContents`, `injectFeaturedNote`, `receiveAnnouncementEmail`, `autoSensitive` | bool |
| `followApprovalLocalSeconds`/`followApprovalRemoteSeconds` | nullable int 0–2592000 (30 days) |
| `makeNotesFollowersOnlyBefore`/`makeNotesHiddenBefore` | nullable int |
| `followingVisibility`/`followersVisibility` | `public`/`followers`/`private` |
| `chatScope` | `everyone`/`followers`/`following`/`mutual`/`none` |
| `pinnedPageId` | nullable; non-null → `NO_SUCH_PAGE` until the pages store exists; null clears |
| `mutedWords`/`hardMutedWords` | string / string-array entries; char-count over default `wordMuteLimit` (200) → `TOO_MANY_MUTED_WORDS`; single strings must be `/pattern/flags` + compile → else `INVALID_REGEXP`; setting `mutedWords` flips `enableWordMute` |
| `alwaysMarkNsfw` | bool; `RESTRICTED_BY_ROLE` guard applies when a role forces it on (default policy leaves it user-settable) |
| `mutedInstances`, `emailNotificationTypes` | string arrays |
| `notificationRecieveConfig` (upstream misspelling kept) | object of `{type: all\|never\|following\|follower\|mutualFollow\|followingOrFollower}` or `{type: list, userListId}` |
| `alsoKnownAs` | max 10 unique strings; blank → `NO_SUCH_USER`, self → `FORBIDDEN_TO_SET_YOURSELF`, resolved-without-URI → `URI_NULL`, unresolvable skipped; stored as actor URIs |
| `rejectQuotes` (DIVERGENCE) | bool, stored + packed on `MeDetailed`; enforced by §3 |
| `defaultReaction` (DIVERGENCE) | nullable 1–260 chars, stored + packed on `MeDetailed`; resolved server-side by §6 |

Every update resets `verifiedLinks` to `[]` (re-verification loop deferred: needs off-request HTTP). Unlocking (`isLocked: false` on a locked account) accepts all pending follow requests. Packer defaults fixed to the upstream column defaults (`preventAiLearning`/`injectFeaturedNote`/`receiveAnnouncementEmail` true, `chatScope` `mutual`, `publicReactions` local-only, `emailNotificationTypes` `["follow", "receiveFollowRequest"]`); `MeDetailed`/`UserDetailed` pack the stored profile (`lang`, `fields`, `verifiedLinks`, visibilities, mute lists, `notificationRecieveConfig`, `alsoKnownAs` as ids, `hideOnlineStatus` → `onlineStatus: unknown`, decorations without registry `url`s).

## 13. Superseding note — §1 private followers / following + §9 full list (2026-10-16, `docs/misskey-parity-plan.md`)

> **Superseded 2026-10-20 (§39):** the moderator bypass is live, not "always
> false". `viewer_is_moderator` reads `Account::has_moderator_powers()`
> (`src/application/queries.rs:1704-1713`), and the §1 gate calls it
> (`queries.rs:1737`). The `users/followers` UUID-sharing note below still
> applies.

`users/followers` + `users/following` keep their Misskey params (`userId` or `username`+`host`, cursor `sinceId`/`untilId`/`sinceDate`/`untilDate`, `limit` 1–100 default 10) and `Following[]` shape, and gain the upstream visibility gate (`users/followers.ts:115-135`, `users/following.ts:123-143`):

| Target visibility | Who reads | Else |
|---|---|---|
| `public` | everyone | — |
| `private` | only the list owner | `FORBIDDEN` (`Forbidden.`, `kind: client` → HTTP 400 per `ApiCallService` mapping) |
| `followers` | the owner or someone following the owner | `FORBIDDEN` (anonymous denied) |

DIVERGENCE (deferred): the moderator bypass (`RoleService.isModerator`) is wired through `AppServices::viewer_is_moderator`, which returns false until the roles store lands — every gate currently applies to every requester. Upstream assigns a different error UUID per endpoint for the same `FORBIDDEN` code; pinetree emits the `users/followers` UUID (`3c6a84db-…`) for both.

`UserDetailed` count masking (`UserEntityService.ts:465-473` parity): `followersCount`/`followingCount` pack the count when the matching visibility is `public`, or the viewer is the account, or (`followers` mode and the viewer follows the account); otherwise they pack `null` (never omitted).

ActivityPub `GET /users/{id}/followers` + `/following` (`ActivityPubServerService.ts:225-233,322-330` parity): any non-`public` visibility answers `403` with `Cache-Control: public, max-age=30`, for the index and every page alike, with no per-requester exception.

§9: no cap existed to remove — both lists page `limit` 1–100 default 10 newest-first, and the only missing piece was the §1 gate above. A 250-follow sweep test in both directions asserts cursor paging has no duplicates or gaps.

## 14. Superseding note — §2 private reactions (2026-10-16, `docs/misskey-parity-plan.md`)

> **Superseded 2026-10-20 (§39):** the moderator bypass on this endpoint is live
> via `viewer_is_moderator` (`src/application/queries.rs:1853`). The "always
> false" DIVERGENCE line below is stale.

New endpoint `users/reactions` (Misskey `users/reactions.ts` params, errors, and pack shape):

| Aspect | Rule |
|---|---|
| Auth | none (`requireCredential: false`) |
| Params | `userId*`, cursor `sinceId`/`untilId`/`sinceDate`/`untilDate`, `limit` 1–100 default 10 |
| Response | `NoteReactionWithNote[]` (`{id, createdAt, user: UserLite, type, note: Note}`) newest-first |
| Errors | remote target → `IS_REMOTE_USER`; `!publicReactions && requester != target` → `REACTIONS_NOT_PUBLIC` (both exact upstream message/code/id, `kind: client` → HTTP 400); target blocks the requester → silent `[]`; unknown `userId` → `NO_SUCH_USER` |

DIVERGENCE (deferred, shared with §13): moderators bypass all three gates via `AppServices::viewer_is_moderator` (always false until roles land).

Read filters mirror the endpoint's SQL + post-filter split: note visibility plus suspended/blocked-host authors always apply (`FeedFilter` sets where applicable); the blocked/muted post-filter (`isUserRelated` parity over note author, reply author, and renote author) is skipped for reactions to the target's own notes. Renote-mutes do not apply here (upstream runs no renote-mute query on this endpoint). A reaction whose note is gone cannot be packed and is skipped (upstream `leftJoin`s; invisible on the wire). `type` is the stored reaction string verbatim — legacy-emoji normalization rides with §6, which owns the reaction path.

Pack rule (`UserEntityService.ts:558` parity, verified no change needed): `publicReactions` packs `isLocal ? profile : false` — remote accounts always pack `false` regardless of the stored flag.

## 15. Superseding note — §4 polls + §6 wire compat (2026-10-16, `docs/misskey-parity-plan.md`)

> **Superseded 2026-10-20 (§39):** the `TODO(§8)` accept-and-ignore stub below
> is gone. Inbound `Update` routes `Note`/`Article`/`Page` to
> `handle_update_note` (`src/federation/inbox.rs:1796-1797`; body at `:1953`).
> The `notes/edit` arm is at `src/api/misskey/endpoints/mod.rs:4302`. Still
> divergent: string-URI `Update` objects are not fetched
> (`inbox.rs:1781-1786`).

Polls ship backend-only (no frontend work). Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`), verified this session against `notes/create.ts:168-184,230`, `NoteCreateService.ts:405-470,659-722,819-820`, `notes/polls/vote.ts` (full file), `Poll.ts`, `PollVote.ts`, `models/json-schema/note.ts:118-`, `ApRendererService.ts:459-470,594-680` (`renderQuestion`/`renderVote`), `ApQuestionService.ts` (full file), `PollService.ts` (full file), `ReactionService.ts:34-48,125-162,383-401`.

`notes/create` accepts `poll` (Misskey params, errors, and pack shapes by default):

| Aspect | Rule |
|---|---|
| Params | `poll: { choices[2..10] unique, each 1..50 chars, multiple = false, expiresAt? (ms) \| expiredAfter? (ms, minimum 1, wins over `expiresAt`) }`; unknown sub-fields → `INVALID_PARAM` |
| Expiry | must lie strictly in the future, else `CANNOT_CREATE_ALREADY_EXPIRED_POLL` (`Poll is already expired.`, `04da457d-b083-4055-9082-955525eda5a5`, exact upstream code/message/id) |
| Storage | `polls{noteId, choices, votes, multiple, expiresAt}` + `poll_votes UNIQUE(user, note, choice)` rows commit in the same transaction as the note (`PollStore` port; redb implements, FDB inherits — no second path); an expiring poll arms `JobKind::PollExpiry` (`not_before = expiresAt`, reusing the worker lease/backoff) |
| Text rule | a poll stands in for `text` (Misskey `(re)note with text, files and poll are optional` parity) |

`notes/polls/vote` (credential, `write:votes`; `noteId*`, integer `choice*`; `204` on success):

| Order | Gate → error |
|---|---|
| 1 | unknown/invisible/deleted note → `NO_SUCH_NOTE` |
| 2 | note carries no poll → `NO_POLL` (`The note does not attach a poll.`, `5f979967-52d9-4314-a911-1c673727f92f`) |
| 3 | §0A `AppServices::check_poll_vote_block` (author blocks voter, self-votes exempt) → `YOU_HAVE_BEEN_BLOCKED` |
| 4 | past expiry → `ALREADY_EXPIRED` (`The poll is already expired.`, `1022a357-b085-4054-9083-8f8de358337e`) |
| 5 | out-of-range choice → `INVALID_CHOICE` (`Choice ID is invalid.`, `e0cc9a04-f2e8-41e4-a5f1-4127293260cc`) |
| 6 | duplicate (`multiple` allows distinct choices, same choice conflicts) → `ALREADY_VOTED` (`You have already voted.`, `0963fc77-efac-419b-9424-b391608dc6d8`) |

All six idents are exact upstream code/message/id (`kind: client` → HTTP 400). A vote commits `poll_votes` + `votes[i] += 1` + the `pollVoted` streaming event + (for federated notes) the `deliverQuestionUpdate` job atomically; votes on remote notes additionally deliver the `Create` vote activity to the poll owner's inbox.

`Note` pack (`note.ts` parity): `hasPoll: bool` + `poll: { expiresAt (ISO|null), multiple, choices: [{ isVoted (per requester), text, votes }] } | null`. `pollVoted { choice, userId }` fires on the note stream per vote; `pollEnded` fires once at expiry via `PollExpiry`.

Federation (`Question` parse/render; `Update` routing is shared with §8):

- Outbound: poll notes render as `type: Question` with `oneOf`/`anyOf` tallies + `endTime`/`closed` (Misskey `renderNote ...asPoll` parity); votes on remote notes send `Create`-vote activities (`renderVote` parity); every local vote fans out `Update(Question)` to followers + relays (`deliverQuestionUpdate` parity, Misskey `renderUpdate(renderNote)` shape).
- Inbound: `Create` vote objects (`inReplyTo` + `name`, no other content) record votes on the targeted poll (expired/duplicate/unknown converge quietly, never become notes); cached remote polls seed origin tallies at ingest and refresh them from embedded `Update(Question)` documents by choice-name match with poll-owner attribution checks (Misskey `ApNoteService` vote branch + `ApQuestionService.updateQuestion` parity).
- §8 EXTENSION POINT: `src/federation/inbox.rs::handle_update` — §4 owns only the `Question` arm (`handle_update_question`); the sibling `Note`/`Article`/`Page` arm is a `TODO(§8)` accept-and-ignore stub where the `Update(Note)` edit branch lives. §4 implements no note editing.
- DIVERGENCE (documented): remote `Question`s violating the storage invariants (<2 or >10 choices, empty/>50-char names) attach as poll-less notes rather than failing ingest; string-URI (non-embedded) `Update(Question)` objects are not fetched.

§6 wire compat only (per-user `defaultReaction` resolution/federation is a separate later slice and is NOT implemented here):

- Fallback reaction is now the bare heart `❤` (U+2764, no VS16) everywhere (Misskey `FALLBACK` parity; previously pinetree stored `❤️` U+2764+FE0F).
- `likeOnly` (and `likeOnlyForRemote`-from-remote) forces the fallback; every other reaction is normalized: `legacies` map (`like→👍 love→❤ laugh→😆 hmm→🤔 surprise→😮 congrats→🎉 angry→💢 confused→😥 rip→😇 pudding→🍮 star→⭐`), FE0F stripped unless the grapheme contains ZWJ, anything else → fallback.
- DIVERGENCE (documented): `:name:`/`:name@host:` custom-emoji shapes pass through unchanged — pinetree has no custom-emoji registry, so the upstream permission/sensitivity/role/media-silenced-host checks have nothing to check against; unknown custom names are stored verbatim instead of falling back.

## 16. Superseding note — §5 drafts + scheduling (2026-10-16, `docs/misskey-parity-plan.md`)

New endpoints (Misskey `notes/drafts/create|list|update|delete|count.ts` params, errors, and pack shapes; there is no `notes/schedule/*` — scheduling is `scheduledAt + isActuallyScheduled` on a draft):

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `notes/drafts/create` | `write:account`, 300/h | `visibility` = public (default), `visibleUserIds[]`, `cw` 1..=100, `hashtag` ≤200, `localOnly` = false, `reactionAcceptance` (nullable enum), `replyId?`, `renoteId?`, `channelId?` (non-null → `NO_SUCH_CHANNEL`: channels do not exist), `text` 0..=3000 (empty allowed), `fileIds[]` 0..=16 unique, `poll` (`choices` 0..=10 unique 1..=50, `multiple`, `expiresAt?`, `expiredAfter?` ≥1), `scheduledAt?` (ms), `isActuallyScheduled` = false | `{createdDraft: NoteDraft}` |
| `notes/drafts/list` | `read:account` | cursor (`limit` 1–100 default 30), `scheduled?` tri-state | `NoteDraft[]` newest-first |
| `notes/drafts/update` | `write:account`, 300/h | `draftId*` + every create field optional; `poll: null`/absent leaves poll columns untouched, a `poll` object replaces them wholesale (absent sub-fields clear, upstream `.set` parity); `hasPoll` never toggles via update (upstream wart, mirrored) | `{updatedDraft: NoteDraft}` |
| `notes/drafts/delete` | `write:account`, 300/h | `draftId*` | void; foreign draft → `ACCESS_DENIED`, missing → `NO_SUCH_NOTE_DRAFT` |
| `notes/drafts/count` | `read:account` | none | number of own drafts |

Rules (Misskey `NoteDraftService.ts` parity): draft-count limit (`TOO_MANY_DRAFTS`) then, when newly scheduling, scheduled-count limit (`TOO_MANY_SCHEDULED_NOTES`); schedule clock rules on the (partial, for update) shape — scheduled without a time → `SCHEDULED_AT_REQUIRED`, non-future time → `SCHEDULED_AT_MUST_BE_IN_FUTURE`; poll expiry → `CANNOT_CREATE_ALREADY_EXPIRED_POLL`; files must exist and be owned (`NO_SUCH_FILE`); reply/renote targets must exist and be live (`NO_SUCH_REPLY_TARGET` / `NO_SUCH_RENOTE_TARGET`); a target whose author blocks the drafter → `YOU_HAVE_BEEN_BLOCKED`. Fine visibility-narrowing subtleties are deferred to post time, when the full `create_note` path re-validates. All three scheduling codes (and `TOO_MANY_DRAFTS` / `NO_SUCH_NOTE_DRAFT` / `ACCESS_DENIED`) use the exact upstream code/message; the create-endpoint UUID is canonical (the update endpoint reuses the same `code` with its own UUID upstream — pinetree emits the create UUID for both).

Policy limits (instance-configurable, defaults are the upstream `RoleService` values): `note_draft_limit` 10, `scheduled_note_limit` 1, wired from `Config` through `AppServices` and surfaced in the `MeDetailed`/`meta` `policies` (`noteDraftLimit`, `scheduledNoteLimit` alongside the existing `pinLimit: 5`).

Scheduling: `PostScheduledDraft` (`not_before = scheduledAt`, payload carries the armed time so a rescheduled draft converges quietly). `update`/`delete` clear every arming for the draft first (`clearSchedule` linear-scan parity), then re-arm from the merged row when still scheduled — row + jobs commit in one storage transaction. The worker posts through the full note-create path (mentions/notify/federation/poll-expiry jobs included), deletes the draft on success, and emits `scheduledNotePosted` (carries the posted `note`) or, on post failure, keeps the draft and emits `scheduledNotePostFailed` (carries the `draft`). Both kinds are first-class `i/notifications` filter values and stream as plain `notification` hints. DIVERGENCE (documented): upstream scheduling notifications carry no `notifierId`; ours set notifier = the draft owner (the `Notification` row requires a notifier, and self-notify keeps the pack shape uniform).

`NoteDraft` pack (upstream `note-draft.ts` parity): `id`, `createdAt` (from the draft id), `text`, `cw`, `userId`, `user` (UserLite), `replyId`/`renoteId`, `reply`/`renote` (one level, `null` when unresolvable), `visibility`, `visibleUserIds`, `fileIds`, `files`, `hashtag`, `poll` (`{choices, multiple, expiresAt (ISO|null), expiredAfter}` or `null`), `channelId: null`, `channel: null` (no channels), `localOnly`, `reactionAcceptance`, `scheduledAt` (ms|null), `isActuallyScheduled`.

## 17. Superseding note — §7 pinned / featured (2026-10-16, `docs/misskey-parity-plan.md`)

`pinnings UNIQUE(user, note)`; only the note's own author may pin it (anything else, including unknown ids, is `NO_SUCH_NOTE` — the upstream `findOneBy({id, userId})` parity); over the `pinLimit` (5) → `PIN_LIMIT_EXCEEDED`; duplicates → `ALREADY_PINNED` (all three exact upstream code/message/id; same-UUID-per-code convention as §16). Local public/home pins and unpins federate as `Add`/`Remove` of the note to/from `{actor}/collections/featured` to remote follower inboxes (upstream `deliverPinnedChange` parity, minus relays: pinetree has no relay infra, so delivery covers followers only).

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `i/pin` | `write:account` | `noteId*` | `MeDetailed` of caller (with live pins) |
| `i/unpin` | `write:account` | `noteId*` | `MeDetailed` of caller (a missing pinning row is a no-op success; foreign/unknown notes are `NO_SUCH_NOTE`) |

`UserDetailedNotMe`/`MeDetailed` now pack real `pinnedNoteIds` (pinning-id order, oldest first) and `pinnedNotes` (packed `Note`s; gone notes skipped).

Featured (`notes/featured`, `users/featured-notes {userId}`; no auth; both return `Note[]` newest-first): one documented ranking (compatible-simplified, NOT upstream-identical). Upstream ranks through Redis counters incremented on every reaction/renote and returns the ranked set newest-first; pinetree selects by score (`reactions + renotes`) over the trailing window (3 days global, 7 days per-user — the upstream window lengths) from the stored counters, then orders newest-first. Candidates are recent non-sensitive public notes (local + remote; non-sensitive = no CW and no sensitive attached file, gone/missing files fail closed; no channels exist so the no-channels rule holds trivially). Read filters mirror the upstream post-filter: visibility for the viewer plus muted-author and blocking-author skips (note, reply, and renote authors). `users/featured-notes` scopes the same ranking to one author (unknown authors rank empty, no `NO_SUCH_USER`; a viewer blocked by the target gets `[]`); a non-null `notes/featured` `channelId` ranks empty (no channels exist). Both endpoints share a 3600s timed cache (upstream `cacheSec: 3600`) keyed `global` / `user:{id}`, owned by `ApiState` (per-state, so tests get isolated caches).

## 18. Superseding note — §10 notification grouping (2026-10-16, `docs/misskey-parity-plan.md`)

New endpoint `i/notifications-grouped` with the SAME params as `i/notifications` (the profile §5 OUT entry for `notifications-grouped` is superseded). Pure pack-layer change; no storage change. Consecutive `reaction` notifications on the same note merge into `reaction:grouped` (`{id, createdAt, type, note, reactions: [{user: UserLite, reaction}]}` — `createdAt` stays the newest member's, `id` rolls to the oldest, upstream `prev.createdAt` + rolling-`id` parity); consecutive `renote` notifications on the same *target* merge into `renote:grouped` (`{id, createdAt, type, note, users: [UserLite]}` — the target resolves through the renote note's `renote_id` since our rows carry no `targetNoteId`; unresolvable sides pass through unmerged). Groups drop the top-level `user`/`userId`; lone rows pack exactly like `i/notifications`. The `reaction:grouped` / `renote:grouped` pseudo-types are valid `includeTypes`/`excludeTypes` entries that match nothing alone and exclude nothing (same observable outcome as upstream, whose DB query only matches stored rows); `markAsRead` defaults true and flips before grouping, like upstream.

## 19. Superseding note — §11 notification filtering + §12 timelines (2026-10-16, `docs/misskey-parity-plan.md`)

> **Verified 2026-10-20 (§39):** the home-timeline "no `withReplies`" decision
> is kept. `notes/timeline` rejects `withReplies` through `check_allowed_fields`
> (`src/api/misskey/endpoints/mod.rs:6132-6147`). Closed. The `list(userListId)`
> receive-config TODO below is stale: list configs deliver to members
> (`src/application/commands/notifications.rs:113-120`).

Backend only. Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`). Verified this session against `NotificationService.ts:97-148`, `UserProfile.ts:252-272`, `types.ts:28-57`, `notes/{timeline,local-timeline,hybrid-timeline,global-timeline}.ts` (full files), `QueryService.ts:92-390` (`generateBaseNoteFilteringQuery`, muted-user/host/instance/renote queries, visibility query), `NoteEntityService.ts:131-328` (`shouldHideNote`/`hideNote`).

### §11 — creation-time gate + read-time filters

Creation (`AppServices::notification_delivery_allowed`, called by the `Notify` job handler before inserting — the single choke point every local and inbound `Notify` job flows through; drops converge quietly: job completes, no row, no `main` hints):

| Check (upstream order) | Rule |
|---|---|
| Self-notify | `notifiee == notifier` drops |
| Mutes | active `mutings.has(notifier)` drops |
| `notificationRecieveConfig[type]` | absent/`all` = deliver; `never` drops; `following` = notifiee follows notifier; `follower` = notifier follows notifiee; `mutualFollow` = both; `followingOrFollower` = either |
| `list(userListId)` | drops — pinetree has no user-list store yet, so no list has members (upstream missing-list parity: members fetch returns empty). Never errors. TODO: wire to real memberships when user lists land |
| Missing notifiee | fails open (preserves pre-§11 behavior for edge cases) |

`NotificationKind::wire_type()` is the single type-name mapping (receive-config keys and `includeTypes`/`excludeTypes` both read it — no second list). Scheduler-direct inserts (`emit_scheduled_notification`) bypass the gate, like upstream notifier-less notifications; upstream `pollVote`/`groupInvited` stay dropped as obsolete.

Read-time: `i/notifications` and `i/notifications-grouped` share `parse_notification_query` (kept in sync by construction). All 20 upstream `notificationTypes` are accepted entries: 8 emittable kinds (`follow`, `receiveFollowRequest`, `mention`, `reply`, `renote`+`quote` both selecting renote rows, `reaction`, `scheduledNotePosted`, `scheduledNotePostFailed`); 11 unemitted match nothing (`note`, `pollEnded`, `followRequestAccepted`, `roleAssigned`, `chatRoomInvitationReceived`, `achievementEarned`, `exportCompleted`, `login`, `createToken`, `app`, `test`); 2 obsolete are silently dropped (`pollVote`, `groupInvited`); the `reaction:grouped`/`renote:grouped` pseudo-types are valid and match nothing alone / exclude nothing. `pollEnded` stays unemitted (§4 ended-poll-notifications exclusion stands); `exportCompleted` stays unemitted until §13 lands exports.

Requested presets map to existing machinery, no second list: blacklist = `mutings`, whitelist-only = `list(userListId)` pointing at a user-owned list, mutuals-only = `mutualFollow`, followed-only = `following`, followers-only = `follower`, either-direction = `followingOrFollower`. Per-type configs are set via `i/update {notificationRecieveConfig}` and shipped on `MeDetailed` (§0B columns, verified present).

### §12 — timelines: replies, blocks, mutes

- DIVERGENCE (kept, decided): home (`notes/timeline`) has no `withReplies` param and includes replies from followed users (Misskey parity would be `replyId IS NULL OR (reply AND replyUserId == me)`). Only the §0A/§12 filters below were added; reply behavior unchanged.
- Local/hybrid Misskey parity: `withReplies` default false, but self-replies (`replyUserId == authorId`) are kept — fixed in `local_timeline`, `hybrid_timeline` (merged-set filter, same as upstream's merged-query filter), and the streaming local/hybrid eligibility. Pending (unresolved-parent) replies carry no `replyUserId` and never count as self-replies; `is_visible` withholds them everywhere regardless.
- Global: `visibility = public AND channelId IS NULL`, no `withReplies`, carries replies including non-self ones (matches both Misskey and the pre-existing `global_timeline`). The `channelId` conjunct holds trivially: channels are rejected at creation (`UNSUPPORTED_FIELD`), so no stored note has a channel.
- §0A/§12 read filters (`FeedFilter`/`feed_filter_hides`, extended with `mutedInstances`) apply on every timeline/note read: home, local, hybrid, global, `users/notes`, `notes/show|replies|renotes|children|conversation|search|search-by-tag`, and `users/reactions` (visibility + suspended/host-blocked arm always; mute/block post-filter; no renote-mutes — upstream `users/reactions.ts` applies visibility + blocked-host + suspended only). `mutedInstances` semantics: notes whose author, reply author, or renote author sits on a muted instance are hidden (host compare is case-insensitive; local authors have no host and never match; the list is viewer-scoped, so anonymous/global reads are unaffected). Suspended authors hide for every viewer including anonymous; blocked/muted authors hide only for the affected viewer. `blocked_hosts()` is still the empty extension point (§0A: no instance-block config exists yet).
- Streaming parity closed (§0A known gaps): `note_eligible_for_channel` now computes the channel rules then applies the same feed filter once (hybrid recurses into the base rules so the filter applies exactly once on the merged verdict); `capture_gate` applies the same feed filter on top of the visibility rules.
- Word mutes are SERVER-ENFORCED (Batch N WS8, owner-approved DIVERGENCE
  from upstream Misskey parity, which enforces them only in the frontend):
  `hardMutedWords` drop notes in `FeedFilter` on every read;
  `mutedWords` pack viewer-specific `isSoftMuted` (true only when matched
  AND the note has no CW of its own). See `docs/word-mutes.md`; manifest
  §31. The old "client-side only" rule is superseded.

## 20. Superseding note — §13 profile import / export (2026-10-16, `docs/misskey-parity-plan.md`)

Backend only; no frontend. Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`). Verified this session against `i/export-following|blocking|mute|notes|antennas|clips|favorites|user-lists.ts`, `i/import-blocking|muting|following|antennas|user-lists.ts`, `QueueService.ts:322-587`, and every `Export*`/`Import*ProcessorService.ts`. (`customEmoji` has no dedicated `i/export|import` endpoint in this pin — out of scope.)

All 13 endpoints take a credential (`secure:true` parity: pinetree has no secure mode, so credential + a per-endpoint 1/hr rate bucket) and answer `204 No Content` immediately (upstream returns void); an `Export`/`Import` job (ids only, never file content) does the work off-request. `requiredRolePolicy` (`canImport*`) and `prohibitMoved` are not gated (no roles/moved infra).

| Endpoint | Params | File produced / consumed |
|---|---|---|
| `i/export-following` | `excludeMuting = false`, `excludeInactive = false` | `following-YYYY-MM-DD-HH-mm-ss.csv`: `{acct},withReplies={bool}` per followee, follow-row-id order, no header row |
| `i/export-blocking` | none | `blocking-*.csv`: `{acct}` per blockee, block-row-id order, no header |
| `i/export-mute` | none | `mute-*.csv`: `{acct}` per non-expiring mutee only (upstream `IsNull()` parity), mute-row-id order, no header |
| `i/export-notes` | none | `notes-*.json`: own notes newest-first, `{id,text,createdAt,fileIds,files,replyId,renoteId,poll,cw,visibility,visibleUserIds,localOnly,reactionAcceptance}` per note (poll = `{choices,votes,multiple,expiresAt}` or null) |
| `i/export-user-lists` | none | `user-lists-*.csv`, zero rows (no subsystem) |
| `i/export-antennas` | none | `antennas-*.json`, `[]` (no subsystem) |
| `i/export-clips` | none | `clips-*.json`, `[]` (no subsystem) |
| `i/export-favorites` | none | `favorites-*.json`, `[]` (no subsystem) |
| `i/import-following` | `fileId*`, `withReplies = false` | CSV rows: bare/`user@host` acct (+ row `withReplies=` wins) and the legacy 3-column (`id,username,host`) variant; unresolvable rows skip |
| `i/import-blocking` | `fileId*` | CSV rows: first column as acct; unresolvable rows skip |
| `i/import-muting` | `fileId*` | CSV rows: first column as acct, non-expiring mutes; unresolvable rows skip |
| `i/import-user-lists` | `fileId*` | CSV rows parsed + validated, zero applied (no subsystem) |
| `i/import-antennas` | `fileId*` | JSON array parsed + validated, zero applied (no subsystem); malformed JSON → `INVALID_PARAM` |

- Completion: every export stores its file in the caller's drive and emits `exportCompleted` (`exportedEntity` verbatim upstream tags `following/blocking/muting/note/userList/antenna/clip/favorite` + `fileId`). The export job's completion and the `Notify` enqueue commit in ONE transaction (`complete_with_followups`); the notification is a first-class `i/notifications` filter value and streams as a plain `notification` hint. Imports never notify.
- Import guards (`import-blocking.ts:53-86` shape on all five): foreign/missing file → `NO_SUCH_FILE`, empty → `EMPTY_FILE`, over 64KiB → `TOO_BIG_FILE`, all with the verbatim per-endpoint UUIDs (upstream mints one UUID per endpoint per code). The commented-out upstream `UNEXPECTED_FILE_TYPE` check stays unimplemented, like upstream. `i/import-antennas` has no size cap upstream and is guarded the same way here.
- Row application goes through the interactive ops (`follow`/`block`/`mute`), so §0A block gates and `ALREADY_*` dedup apply; replays converge (conflicts skip, never duplicate). One transaction per row keeps writes bounded; only storage faults retry the job.
- DIVERGENCEs (decided): `excludeInactive` accepted and ignored (no last-active tracking; a zero-notes heuristic would mislabel lurkers); locals export as bare `username` (upstream always appends `@host`) — imports accept both forms; remote users resolve from the local store only (no remote fetch — unresolvable rows skip, upstream would federate-resolve them); no-subsystem entities export empty valid files and import zero rows (client tooling still finds the endpoints); drive upload dedups on content (`force == false`, upstream `force: true`) so retried exports converge instead of orphaning duplicates; malformed antenna JSON is `INVALID_PARAM` (upstream throws 500); rate buckets are 1/hr on all 13 (upstream uses 1/day for notes/clips/favorites and 1/min for user-lists); exports larger than `max_file_size` fail loudly through the shared upload path (job quarantines after the retry budget); `exportCompleted` rows store the exporter as notifier (upstream is notifier-less; same convention as the §16 scheduling note) and only a `never` receive-config drops them.

## 21. Superseding note — Batch M §3 + WP1: roles, moderation log (2026-10-17, `docs/implementer-guide-batch-m.md`)

Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`), Sharkey `2025.4.7` (`./sharkey`). Status: LANDED (§3 foundations + WP1; later WPs append their own notes).

Roles (`is_admin`, `is_moderator` on `Account`; fixed built-in only, custom roles out): `pack_me_detailed` packs real `isAdmin`/`isModerator` (`isModerator` true for admins, `RoleService.isModerator` parity). Guard helpers `require_moderator` / `require_admin` (`api/misskey/auth.rs`) enforce the role in the handler — token scopes alone are not authorization. CLI `set-role --username=<name> (--admin|--moderator)[=false]` is the operator path (how the first admin is made; writes a `setRole` log row with `user`/`userId` = `null` — DIVERGENCE: upstream log rows always carry a user).

| Endpoint | Params | Behavior |
|---|---|---|
| `admin/show-moderation-logs` (new) | `limit` (1..=100, default 10), `sinceId`, `untilId`, `sinceDate`, `untilDate`, `type`, `userId`, `search` | Credential + `read:admin:show-moderation-log` + moderator. Newest-first cursor page of `[{id, createdAt, type, info, userId, user}]` (upstream `ModerationLogEntityService.pack`; `user` = `UserDetailedNotMe`, `null` for CLI rows). `type` unknown → `[]` (upstream `log.type = :type`). `userId` filters the acting moderator, `search` is a case-insensitive substring of `info`. DIVERGENCE: upstream requires admin (`requireAdmin`); pinetree lets moderators read (guide §3.2). |
| `admin/reject-quotes` (changed) | unchanged (`userId`, `rejectQuotes`) | Now requires the moderator role (was: any first-party credential — live auth hole, closed WP1). Moderator-target refusal (WP3 refusal rule) → `400 INVALID_PARAM` carrying upstream's `cannot moderate moderator account` message (DIVERGENCE: upstream throws an untyped `Error` here and its API layer answers `500 INTERNAL_ERROR` with a generic message, `ApiCallService.ts:#onExecError`; clients see a failure either way). Each state change writes a Sharkey-shaped moderation-log row `acceptQuotesUser`/`rejectQuotesUser` with payload `{userId, userUsername, userHost}` (`sharkey/…/types.ts:441-450`; guide §3.2's `updateUserRejectQuotes` label superseded by the match-upstream rule), committed atomically with the flag and the `MeUpdated` job. No-change calls stay no-ops (no log row), Sharkey parity. |

Moderation-log action strings match upstream type names where they exist (`ModerationLogService.ts` / Sharkey `types.ts`), so client log UIs can label rows: `suspend`, `unsuspend`, `updateUserNote`, `acceptQuotesUser`, `rejectQuotesUser`, `setRole`, `updateServerSettings`, `createInvitation`, `deleteInvite`, `resolveAbuseReport`, `forwardAbuseReport`, `updateAbuseReportNote`, `addCustomEmoji`, `updateCustomEmoji`, `deleteCustomEmoji`, `unsetUserAvatar`, `unsetUserBanner`, `deleteAccount`, `createAccount`. (`deleteAllFilesOfAUser` is pinetree-named: upstream `admin/delete-all-files-of-a-user` writes no log row.)

## 22. Superseding note — WP2: registration policy + invite codes (2026-10-17, `docs/implementer-guide-batch-m.md`)

Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`), Sharkey `2025.4.7` (`./sharkey`). Status: LANDED.

Model: one switch (Misskey's). `InstanceSettings.registration` open/closed; `meta.disableRegistration` is its inverse. Closed with no codes issued is a fully closed instance. `signup` (the public path) enforces the policy in the application layer (`AppServices::signup`, upstream `SignupApiService.signup`): open → a supplied code is ignored and NOT consumed; closed → `invitationCode` is required and claimed in the SAME transaction as the account create (conformance I1/I2). Registration-gate rejections answer upstream's plain empty `400` (`SignupApiService.ts` ~136: `reply.code(400); return;`) — that covers a missing, unknown, used, or expired code.

| Endpoint | Params | Behavior |
|---|---|---|
| `signup` (changed) | (effective `invitationCode`) | Policy-gated as above; response unchanged. |
| `admin/accounts/create` (changed) | `username*`, `password*`, `setupPassword` (accepted, never consulted) | Was a public signup alias (live auth hole); now credential + admin. Bypasses the registration gate (upstream calls the ungated `SignupService` there, not `SignupApiService`). Response: `MeDetailed` + `token`. Logs `createAccount` with `{userId: null, userUsername, userHost: null}`, committed in the account-create transaction. DIVERGENCES: upstream writes no moderation-log row (the action name is the guide's); `userId` is `null` because account ids are store-generated; upstream's first-setup bootstrap (`setupPassword` / `INCORRECT_INITIAL_PASSWORD` `97147c55-1ae1-4f6f-91d6-e1c3e0e76d62`) has no pinetree equivalent (CLI `create-account` bootstraps), so `setupPassword` is accepted-and-ignored exactly as upstream ignores it for an authenticated admin; upstream's per-endpoint `ACCESS_DENIED` UUID `1fb7cb09-d46a-4fff-b8df-057708cce513` is not copied (§16 one-UUID-per-code convention keeps `56f35758-…`). |
| `invite/create` (new) | none | Credential + `write:invite-codes` + moderator (§9.1 default: upstream `canInvite`, default `false`). One code; single packed `InviteCode`. No log row (upstream is silent here). |
| `invite/list` (new) | `limit` (1..=100, default 30), `sinceId`, `untilId`, `sinceDate`, `untilDate` | Credential + `read:invite-codes` + moderator. The caller's own codes (upstream filters `createdById = me` even for moderators), newest first. |
| `invite/delete` (new) | `inviteId*` | Credential + `write:invite-codes` + moderator. Upstream rules kept: a moderator may delete any code (including used ones), anyone else only their own unused code. `204`. Errors: `NO_SUCH_INVITE_CODE` (400, `cd4f9ae4-7854-4e3e-8df9-c296f051e634`), `CAN_NOT_DELETE_INVITE_CODE` (400, `ff17af39-000c-4d4e-abdf-848fa30fc1ce`), `ACCESS_DENIED` (403). DIVERGENCE: writes a `deleteInvite` log row (guide §3.2 name; upstream logs nothing here). |
| `invite/limit` (new) | none | Credential + `read:invite-codes` + moderator. `{remaining: null}` — the default policy has no cap (upstream `inviteLimit: 0` = unlimited), so `EXCEEDED_LIMIT_OF_CREATE_INVITE_CODE` (`8b165dd3-6f37-4557-8db1-73175d63c641`) is unused. |
| `admin/invite/create` (new) | `count` (1..=100, default 1), `expiresAt` (ISO string, nullable) | Credential + `write:admin:invite-codes` + **moderator** (upstream `requireModerator`, not admin). Array of packed codes. Unparsable `expiresAt` → `INVALID_DATE_TIME` (400, `f1380b15-3760-4c6c-a1db-5c3aaf1cbd49`). One `createInvitation` row `{invitations: [{code, createdBy, expiresAt}], invitationsCount}` commits with the batch (upstream logs the created tickets). |
| `admin/invite/list` (new) | `limit` (default 30), `offset`, `type` (`unused`\|`used`\|`expired`\|`all`, default `all`), `sort` (`+createdAt`\|`-createdAt`\|`+usedAt`\|`-usedAt`) | Credential + `read:admin:invite-codes` + moderator. All creators. The `sort` switch maps upstream verbatim: `+createdAt` → id DESC (newest first), `-createdAt` → id ASC, `+usedAt` → usedAt DESC NULLS LAST, `-usedAt` → usedAt ASC NULLS FIRST. |
| `admin/meta` (new) | none | Credential + `read:admin:meta` + admin. `MetaDetailed` + `blockedHosts`. WP6 completes the admin read view. |
| `admin/update-meta` (new) | `disableRegistration`, `blockedHosts` | Credential + `write:admin:meta` + admin. `204`. `disableRegistration` maps to `registration`; `blockedHosts` replaces the list (lowercased, trimmed, deduped, empties dropped — upstream `admin/update-meta.ts:260` normalization; stored from WP2, enforced from WP5). Settings + `updateServerSettings` row `{before, after}` commit together. Fields outside the subset are refused by the strict field check (400) — WP6 extends the set; silently ignoring them would be a lie. |

Packed `InviteCode` (upstream `InviteCodeEntityService.pack`): `{id, code, expiresAt, createdAt, createdBy, usedBy, usedAt, used}` with `createdBy`/`usedBy` `UserLite` or `null`. The ticket `id` is an aidx (upstream `RegistrationTicket.id`); `invite/delete` addresses it and list pagination is id-ordered. DIVERGENCE from the guide §3.3 store sketch (which omits `id`): both upstream shapes require it. Codes are generated as upstream (`misc/generate-invite-code.ts`): 8 chars from `23456789ABCDEFGHJKLMNPQRSTUVWXYZ` plus the current minute in base 32 over the same alphabet.

The consume claim and the account create are one storage transaction (`InviteStore::consume_and_create_account`): the code is marked used (`usedBy`/`usedAt`), its unique index row is removed, and the account row (with the password hash) commits together — or nothing does. Conformance Group I (I1 single-use under N retries, I2 claim+create transactional, I3 registration settings round-trip, I4 claim happy path + missing/used/expired refusal) is green against the redb store.

## 23. Superseding note — WP3: suspension (2026-10-18, `docs/implementer-guide-batch-m.md`)

Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`), Sharkey `2025.4.7` (`./sharkey`). Status: LANDED (this session; see `state/logs/2026-10-17-batch-m-wp3.md`).

Endpoints (moderator only; strict `userId` field; void → 204):

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `admin/suspend-user` | credential + `write:admin:suspend-user` + moderator | `userId*` | `204 No Content`; `NO_SUCH_USER` (missing target), `INVALID_PARAM` carrying `cannot suspend moderator account` (moderator/admin target) |
| `admin/unsuspend-user` | credential + `write:admin:unsuspend-user` + moderator | `userId*` | `204 No Content`; `NO_SUCH_USER` (missing target), `INVALID_PARAM` carrying `cannot unsuspend moderator account` (moderator/admin target) |

`signin` behavior change (WP3): a suspended account is refused at `do_signin` with `403 USER_SUSPENDED` — checked **after** the password verifies (pinetree's existence-hiding sign-in is preserved). Upstream checks before the password (`SigninApiService.ts:138`); DIVERGENCE.

Suspend is one transaction (`ModerationLogStore::set_suspended`, conformance H1/H2/H3):
1. set `is_suspended = true`;
2. delete follow requests in both directions (upstream `postSuspend`);
3. append the moderation-log row (`suspend` / `unsuspend`);
4. enqueue ≤3 jobs for local targets only: `StreamingPublish(UserSuspended)` (stream close), one `FederateDeliverPayload::SuspendBroadcast` sweep, one `JobKind::UnfollowAll` sweep. Remote targets take flag + log + follow-request teardown only (no sweeps).

Unsuspend flips the flag, logs `unsuspend`, and for a local user enqueues `SuspendBroadcast { undo: true }` (Undo(Delete(Person))). Removed follow requests stay removed; sign-in/token/stream/actor-fetch restore.

Bounded fan-out (guide §2 rule 5 / §11): the suspend transaction never fans out in-line. `SuspendBroadcast { account_id, activity_id, undo, inbox_override }` (`src/federation/outbox.rs` + `render_delete_person_activity` in `src/activitypub/objects.rs`) is `Delete(Person)` on suspend / `Undo(Delete(Person))` on unsuspend to every knowable shared inbox, reusing the `FeaturedChanged`-style `fan_out` (inline ≤ `FANOUT_INLINE_MAX`, one-inbox jobs beyond). The recipient set comes from the required port method `AccountStore::remote_shared_inboxes(after, max_rows)` (`conformance H5`), wrapped by `collect_known_shared_inboxes` (caller dedupes; accounts without `shared_inbox` are skipped — upstream-exact). `activity_id` is minted at enqueue so re-runs re-deliver stable ids.

`JobKind::UnfollowAll` + `UnfollowAllPayload { account_id, after }` (`src/application/ports.rs`, worker arm in `src/jobs/worker.rs`): each run silently removes ≤ `UNFOLLOW_ALL_PAGE` (100) outgoing follow rows; every row deletion commits in one transaction with its own `FederateDeliver` `Undo(Follow)` job (remote followees only; local followees just lose the edge). A full batch returns a continuation carrying the advanced `after` cursor. Deterministic undo ids `suspend-unfollow-{follow_id}`. Crash-safe: re-runs see removed rows gone; retries mint no second `Undo(Follow)`.

Activity shapes: `Delete(Person)` / `Undo(Delete)` match upstream (`object` = actor URI string, no `to`/`cc`, `published`) plus activity ids (`{base}/activities/{id}`, Undo id `{id}/undo` = upstream's `renderUndo` id rule) — upstream renders none; the ids make re-runs re-deliver stable ids for receiver-side dedupe.

Stream close: `StreamingEvent::UserSuspended` (`src/application/events.rs`) → `HubMessage::UserSuspended` (`src/streaming/hub.rs`) → `run_connection` breaks for that viewer (`src/streaming/server.rs`). Wired from the command, not the store (house rule: commands never touch sockets). DIVERGENCE: pinned upstream (Misskey `2026.10.1-alpha.0` AND Sharkey `2025.4.7`) does NOT drop sockets on `userChangeSuspendedState` — it only invalidates caches (`CacheService.ts:145`) and refuses new upgrades (`StreamingApiServerService.ts:80`); pinetree's drop is a deliberate hardening the deliverable required.

DIVERGENCES (recorded; do not "fix" back):
- Refusal rule applies to BOTH endpoints (guide §3.1 blanket rule): suspend carries upstream's verbatim `cannot suspend moderator account` (`suspend-user.ts:46`); unsuspend carries pinetree-constructed `cannot unsuspend moderator account`. Upstream `unsuspend-user.ts` has no check (apparent oversight); both upstream refusals throw untyped `Error`s → `500 INTERNAL_ERROR` (`ApiCallService.ts:#onExecError`), while pinetree answers `400 INVALID_PARAM` carrying the message (WP1 precedent).
- Missing target → `NO_SUCH_USER` (pinetree UUID `fcd2eef9-…`, one-UUID-per-code; do NOT copy the guide's `1acefcb5-…`). Upstream throws untyped `user not found` → 500.
- Sign-in ordering: pinetree checks `is_suspended` AFTER password verification (keeps existence-hiding sign-in); upstream checks before the password (`SigninApiService.ts:138`, 403 with per-endpoint id `e03a5f46-…` — NOT copied; pinetree's `USER_SUSPENDED` id `a8c724b3-…`).
- Fan-out shape: one sweep job per outbound concern (`SuspendBroadcast`, `UnfollowAll`), each expanding at run time — not literally one job — (guide §11 boundedness: `MAX_JOBS_PER_TXN` 500, 64 KiB payload cap, FDB ~5 s/10 MiB). Outgoing-follow rows are deleted at worker time in sweep batches (each row delete + its job = one transaction), never in the suspend transaction.
- No-change suspend/unsuspend calls are NOT no-ops (upstream `suspend()` re-logs and re-runs post-suspend work unconditionally) — unlike `admin/reject-quotes`.
- Remote targets take flag + log + follow-request teardown only; sweeps are local-only. (Upstream `unFollowAll` runs for remote targets too — deletes their edges to local accounts and delivers `Reject` per edge. Possible later revisit.)
- Sweep recipient set = `AccountStore::remote_shared_inboxes` (all known remote `shared_inbox` values; superset of upstream's `followings`-snapshot set — upstream `postSuspend`/`postUnsuspend` "知り得る全SharedInbox"). Accounts without `shared_inbox` are skipped (upstream-exact).
- Activity ids are minted (upstream renders none) so re-runs re-deliver stable ids for receiver-side dedupe.
- Stream close is a durable job wired from the command, not the store; upstream does NOT drop sockets (only invalidates caches and refuses new upgrades) — pinetree's drop is deliberate hardening.
- Semantics (upstream parity): outgoing follows removed (stay removed), incoming follows kept, follow requests removed in both directions (stay removed). Unsuspend restores sign-in/token/stream/actor-fetch but none of those rows.
- `NO_SUCH_USER` UUID for the missing-target path is pinetree's `fcd2eef9-…` (one-UUID-per-code convention); do not copy the guide's `1acefcb5-…`.

## §24 — Abuse reports (Batch M WP4; routes LANDED Batch N WS5 this session)

Endpoints (`users/report-abuse` answers the packed report; admin resolve/update/forward answer 204): `users/report-abuse` (credential, `write:report-abuse`), `admin/abuse-user-reports` (moderator, `read:admin:abuse-user-reports`), `admin/resolve-abuse-user-report` (moderator, `write:admin:resolve-abuse-user-report`), `admin/update-abuse-user-report` (moderator, same kind upstream), `admin/forward-abuse-user-report` (moderator, same kind upstream). List takes `limit` (default 10)/cursor + `state` (`resolved`/`unresolved`/absent = all) + `reporterOrigin`/`targetUserOrigin` (`combined`/`local`/`remote`). Pack shape: `{id, createdAt, comment, resolved, reporterId, targetUserId, assigneeId, reporter, targetUser, assignee, forwarded, resolvedAs, moderationNote}` (upstream `AbuseUserReportEntityService.pack` parity; assignee null when unset; gone users pack null). Inbound `Flag` (upstream `ApInboxService.flag` parity): the first mentioned LOCAL user becomes the report target, the remote actor the reporter, `content` + the JSON object-URI list the comment; remote-only/empty targets converge quietly (upstream `skip`); atomic dedup + insert (replays converge, no duplicates); blocked-host actors drop before any state write. Overlong inbound comments truncate to the 2048-char column bound (DIVERGENCE: no retry storm). — forwarded Flags from remotes are accepted-and-ignored by the catch-all).

Error codes:
- `NO_SUCH_USER` `1acefcb5-0959-43fd-9685-b48305736cb5` (target missing / self-report path uses pinetree's own `fcd2eef9-…`? DIVERGENCE: pinetree reuses `fcd2eef9-…` for missing target; report-abuse uses upstream `1acefcb5-…` for no-such-user).
- `CANNOT_REPORT_YOURSELF` `1e13149e-b1e8-43cf-902e-c01dbfcb202f`.
- `CANNOT_REPORT_THE_ADMIN` `35e166f5-05fb-4f87-a2d5-adb42676d48f`.
- `NO_SUCH_ABUSE_REPORT` `ac3794dd-2ce4-d878-e546-73c60c06b398` (resolve/update) and upstream ids `8763e21b-…` (forward) / `15f51cf5-…` (update).

Storage: `AbuseReportStore` port (required, no defaults) + redb `abuse_reports` table. `resolve`/`set_note`/`mark_forwarded` commit state + moderation-log row + jobs in one transaction.

Flag delivery: `FederateDeliverPayload::Flag` → `render_flag_activity` (type Flag, actor = system actor, content = report comment, object = reported account URI) → existing SSRF-guarded delivery path.

DIVERGENCES: notifications (admin stream/email/webhook) skipped; admins poll. Reporter host always null (local reports only). Origin filters are accepted but applied at storage scan level (no host column resolution beyond account id). Missing-target `NO_SUCH_USER` emits pinetree's UUID (`fcd2eef9-…`, one-UUID-per-code) instead of upstream `1acefcb5-…`; `CANNOT_REPORT_YOURSELF`/`CANNOT_REPORT_THE_ADMIN` carry verbatim upstream ids. Double-forward refusal answers `400 INVALID_PARAM` carrying upstream's `already been forwarded` message (upstream throws an untyped `Error` → 500). A comment-length violation answers `INVALID_PARAM` via the domain bound (upstream `minLength 1/maxLength 2048` schema parity).

## §25 — Instance blocks (Batch M WP5)

Behavior (no new endpoints; reuses `admin/update-meta` and `admin/meta`):
- `admin/update-meta` accepts `blockedHosts` (already wired in §22/§23): list is normalized via `InstanceSettings::normalize_blocked_hosts` (trim, lowercase, dedup, empty-drop). Replaces the whole list.
- `admin/meta` returns `blockedHosts` from the store.

Semantics (upstream `UtilityService.isBlockedHost`): a host is blocked when it equals a blocked entry OR is a subdomain (`host` ends with `.` + blocked entry). Matching is case-insensitive (entries stored lowercased). Subdomain matching applies consistently in:
- Read path: notes whose author, reply author, or renote author sits on a blocked host (or subdomain) are hidden (`queries.rs` `suspended_or_host_blocked`, all three author slots, upstream `FanoutTimelineEndpointService.ts:145–148`). Restored on unblock with no data loss.
- Inbound: activities from a blocked host (or subdomain) are dropped before any state write or dedup record (all activity types: Create, Update, Delete, Like, Follow, Block, actor update). No dedup row is written, so re-sending after unblock is processed.
- Outbound: delivery-time check in `federation/outbox.rs` `deliver_single`; deliveries to blocked hosts are skipped and the job completes (`Delivered`) rather than retrying.

Storage: `InstanceSettingsStore::get/update` (required, no defaults) + `InstanceSettings::blocked_hosts`. `queries.rs::blocked_hosts()` now reads the store (no second copy).

DIVERGENCES: none new. `federation/instances` endpoint (upstream) not implemented (optional; skipped, recorded here).

## §26 — Admin read and user views (Batch M WP6)

Endpoints:
- `admin/show-user` `{userId}` (moderator only): returns account details including `isSuspended`, `isAdmin`, `isModerator`, `moderationNote`, and `reportCount` (abuse reports whose target is the user). Error `NO_SUCH_USER` for missing.
- `admin/show-users` `{limit?, offset?, state?, origin?, username?, hostname?, sort?}` (moderator only): list users with upstream param subset (`state`: all/alive/available/admin/moderator/adminOrModerator/suspended; `origin`: combined/local/remote). Includes flags + report counts.
- `admin/update-user-note` `{userId, text}` (moderator only): sets the account's moderator note. Logged as `updateUserNote`.
- `admin/delete-account` `{userId}` (admin only): tombstones the account (handle reserved), removes follows and follow requests in both directions, logs `deleteAccount`, enqueues `Delete(Person)` to knowable shared inboxes for local accounts. Shares machinery with `i/delete-account`.
- `i/delete-account` `{password}` (self-service): password required; same machinery, actor = the account itself, logged `deleteAccount`.

Storage: `ModerationLogStore::delete_account` (required, no default). `Account.moderator_note` added (serde default). `AbuseReportStore::count_for_target` added (required). `UpdateAccountPatch.moderator_note` added.

DIVERGENCES:
- Note bodies are not deleted in-line; tombstoning leaves them filtered by the read path (transaction-size bound). A cleanup job is deferred.
- Self-delete logs a `deleteAccount` row with actor = the account itself (upstream logs via `DeleteAccountService`; pinetree records it for audit).
- `INCORRECT_PASSWORD` is answered as `PortError::Validation("INCORRECT_PASSWORD")` (upstream throws untyped "incorrect password").

## §27 — Favorites, note state, `followRequestAccepted` (Batch N WS2–WS3)

Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`). Status: LANDED (this session).

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `notes/favorites/create` | credential `write:favorites`, 20/h | `noteId*` | `204 No Content`; `NO_SUCH_NOTE` (missing, deleted, or invisible to the caller — visibility gate then error-maps to `noSuchNote`), `ALREADY_FAVORITED` (upstream `a402c12b-…` verbatim) |
| `notes/favorites/delete` | credential `write:favorites` (no rate limit, upstream parity) | `noteId*` | `204 No Content`; `NO_SUCH_NOTE` (existence only, no visibility gate — upstream asymmetry), `NOT_FAVORITED` (upstream `b625fc69-…` verbatim) |
| `i/favorites` | credential `read:favorites` | cursor `limit` 1–100 default 10, `sinceId`/`untilId`/`sinceDate`/`untilDate` | `Note[]` newest-first (favorite-id order); gone notes skipped (upstream `leftJoin` parity) |
| `notes/state` | credential `read:account` | `noteId*` | `{isFavorited, isMutedThread: false}`; unknown/invisible → `NO_SUCH_NOTE` (D8 indistinguishability); thread-muting stays out (guide §8), so `isMutedThread` is always `false` |

Storage: `FavoriteStore` port (required, no defaults) + redb `favorites` table keyed `(user_id, note_id)` → `NoteFavorite` row. Unique per (account, note); delete leaves the note. `i/export-favorites` now writes real rows (`{id, createdAt, note: {id, text, createdAt, fileIds, replyId, renoteId, poll, cw, visibility, visibleUserIds, localOnly, reactionAcceptance, uri, url, user: {id, name, username, host, uri}}}` per favorite, id-ASC order; upstream `ExportFavoritesProcessorService.serialize` parity; invisible/gone notes skipped).

Packed notes carry `isFavorited` (bool, always present) for the authenticated caller; anonymous callers see `false`.

Notifications: `followRequestAccepted` is now emitted — locally in `accept_follow_request` (notifiee = requester, notifier = acceptor) and on inbound `Accept(Follow)` (same direction). It is a first-class `i/notifications` filter value and streams as a plain `notification` hint (no side-channel, like scheduling/export notifications). Manifest §19's unemitted list no longer contains it; the remaining unemitted entries (`note`, `pollEnded`, `roleAssigned`, `chatRoomInvitationReceived`, `achievementEarned`, `login`, `createToken`, `app`, `test`) stay out per guide §8 (no subsystem to emit them from). `reaction:grouped`/`renote:grouped` remain packer-only pseudo-types; `pollVote`/`groupInvited` remain obsolete-and-dropped.

## §28 — Custom emoji + remote opt-in (Batch N WS1 + WP8b)

Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`). Status: LANDED (this session).

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `emojis` | none | `{}` | `{emojis: EmojiSimple[]}` local-only, ordered by category then name; 3600s client cache (`cacheSec` parity) |
| `emoji` | none | `name*` | `EmojiDetailed`; unknown → `NO_SUCH_EMOJI` (400; upstream throws `findOneOrFail` → 500 — pinetree answers the missing-resource shape like every other `NO_SUCH_*`) |
| `admin/emoji/add` | credential + `write:admin:emoji` + moderator | `name*` (`^[a-zA-Z0-9_]+$`), `fileId*`, `category?`, `aliases[]?`, `license?`, `isSensitive=false`, `localOnly=false`, `roleIdsThatCanBeUsedThisEmojiAsReaction[]?` | `EmojiDetailed`; `NO_SUCH_FILE` (add-endpoint UUID `fc46b5a4-…`), `UNSUPPORTED_FILE_TYPE`, `DUPLICATE_NAME` (all verbatim) |
| `admin/emoji/update` | credential + `write:admin:emoji` + moderator | `id` xor `name` (+ both = rename when different, `doNameUpdate` parity), optional `fileId`, `category?` (null resets), `aliases[]?`, `license?` (null resets), `isSensitive?`, `localOnly?`, `roleIds…?` | void (204); `NO_SUCH_EMOJI`, `NO_SUCH_FILE`, `SAME_NAME_EMOJI_EXISTS` (all verbatim; `NO_SUCH_FILE` emits the add-endpoint UUID for both — one-UUID-per-code) |
| `admin/emoji/delete` | credential + `write:admin:emoji` + moderator | `id*` | void (204); `NO_SUCH_EMOJI` |
| `admin/emoji/list` | credential + `read:admin:emoji` + moderator | `query?`, cursor, `limit` 1–100 (default 10) | `EmojiDetailed[]` local-only, newest-first; `query` matches name/aliases/category (case-insensitive substring) or exact `:name:` tokens (upstream parity) |

Storage: `EmojiStore` port (required, no defaults) + redb `emojis` (id → row) + `emojis_by_name` (lower shortcode → id; case-insensitive uniqueness, conformance J2) + `remote_emojis` ((lower host, lower shortcode) → row). `remote_emojis.media_id` is owned by the lazy-fetch job only.

Packing: note `emojis`/`reactionEmojis` carry the registry (bare shortcode → served drive URL locally; `name@host` → remote URL only for opted-in viewers, else absent). `UserLite.emojis` stays `{}` (profile-emoji attachment is not modeled — documented). `EmojiSimple` omits falsy `localOnly`/`isSensitive` and empty role ids (upstream parity); `url` prefers the served drive URL.

WP8b (DIVERGENCE, owner-approved): per-account `showRemoteEmojis` (default false), set via `i/update`, read back on `i` / `i/update`. Ingest stores metadata only (≤32 AP `Emoji` tags per note/actor object; no fetch; http(s) icon URLs only). Turning the setting on enqueues one bounded `RemoteEmojiFetch` job per known uncached remote row (≤200 per toggle); the job uses the §2 rule 6 SSRF-guarded fetcher, 2 MiB cap, `image/*` content-type + sniff gate, and stores bytes + a drive metadata row, recording the local copy. Packing prefers the cached copy once present; until then the packed URL points at the remote host (documented privacy trade-off). Turning the setting off hides remote emoji without deleting metadata or cached bytes. Inbound drops (blocked hosts) write no metadata (ingest runs after the drop checks).

DIVERGENCES: moderator role stands in for upstream `canManageCustomEmojis` (no custom-role policies); `roleIdsThatCanBeUsedThisEmojiAsReaction` stored but never enforced (same reason); stream broadcasts (`emojiAdded/Updated/Deleted`) not emitted (no broadcast bus — readers poll `emojis`); profile emoji attachment not modeled (`UserLite.emojis` `{}`); remote fetch caching behavior beyond the packed URL (served from the drive row once cached).

Out of scope (guide §8): bulk ops, `import-zip`, `copy`, `list-remote`. `export-custom-emojis` has no dedicated endpoint in this pin and stays out.

## §29 — Antennas (Batch N WS4; guide WP15)

Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`). Status: LANDED (this session).

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `antennas/list` | credential `read:account` | none | `Antenna[]` of caller, newest-first |
| `antennas/show` | credential `read:account` | `antennaId*` | `Antenna`; owner-scoped, else `NO_SUCH_ANTENNA` |
| `antennas/create` | credential `write:account` | `name*` (1–100), `src*` (`home\|all\|users\|list\|users_blacklist`), `userListId?`, `keywords*`, `excludeKeywords*`, `users*`, `caseSensitive*`, `withReplies*`, `withFile*`, `localOnly=false`, `excludeBots=false`, `excludeNotesInSensitiveChannel=false` (accepted, stored, no effect — no channels) | `Antenna`; `EMPTY_KEYWORD`, `TOO_MANY_ANTENNAS` (both verbatim), `NO_SUCH_USER_LIST` for `src=list` (see below) |
| `antennas/update` | credential `write:account` | `antennaId*` + any create field | `Antenna`; same error set + `NO_SUCH_ANTENNA`; sets `isActive=true`, bumps `lastUsedAt`; id+renaming-name renames (`doNameUpdate` parity, `SAME_NAME_EMOJI_EXISTS` does not apply — antenna names are not unique) |
| `antennas/delete` | credential `write:account` | `antennaId*` | void (204); `NO_SUCH_ANTENNA`; feed cascade in the same transaction |
| `antennas/notes` | credential `read:account` | `antennaId*`, cursor, `limit` 1–100 (default 10) | `Note[]` newest-first; reactivates the antenna; read-time visibility/mute/block filtering; gone notes skipped |
| `antennas/remove-note` | credential `write:account` | `antennaId*`, `noteId*` | void (204); owner-scoped (`NO_SUCH_ANTENNA`); missing rows are no-op |
| `i/export-antennas` / `i/import-antennas` | credential (1/hr) | export: none; import: `fileId*` | Real now: export writes `[{name, src, keywords, excludeKeywords, users, userListAccts: null, caseSensitive, localOnly, excludeBots, withReplies, withFile, excludeNotesInSensitiveChannel}]` (upstream shape); import validates rows (upstream `exportedAntennaSchema` rules), skips empty-keyword rows (upstream verbatim rule), rewrites `src=list`+`userListAccts` to `src=users` (upstream parity; `src=list` without accts skips), and applies the rest through `create_antenna`. Endpoint pre-check `count + batch >= limit` → `TOO_MANY_ANTENNAS` (import-endpoint UUID `600917d4-…`); malformed JSON → `INVALID_PARAM` (upstream throws 500) |
| `antenna` stream channel | credential + owned `antennaId` | `params.antennaId*` | `note` frames (detail packs) for matches; anonymous/foreign/unknown ids silently refused (credential-channel parity) |

Storage: `AntennaStore` + `AntennaNoteStore` ports (required, no defaults) + redb `antennas` (id → row), `antennas_by_owner` ((owner, id)), `antenna_notes` ((antenna, note)) tables. `antenna_notes` trims to the newest 200 per antenna in the same append transaction.

Matching (`AntennaService.checkHitAntenna` order): sensitive-channel/bots → `localOnly` → `withReplies=false` drops all replies → visibility (`specified` needs owner-or-in-list; `followers` needs owner-follows-author) → src (`all` matches; `users`/`users_blacklist` compare lowercased `user[@host]` accts; `list` never matches) → `keywords` OR-of-AND over `text + "\n" + cw` (case-insensitive unless `caseSensitive`; empties stripped; null text+cw never matches a non-empty list) → `excludeKeywords` drop → `withFile` drop. Architecture (no in-process cache): note creation enqueues one bounded `AntennaMatch` job (note id) in the same transaction (local + federated notes); the worker matches active antennas and appends feed rows, then publishes the `antenna` event post-commit.

DIVERGENCES (recorded; do not "fix" back): `home` = owner's own notes + notes from followed accounts (upstream `home` is a TODO matching everything, `AntennaService.ts:137-139`); no 7-day auto-deactivation (upstream `CleanProcessorService.ts:51-56`; pinetree caps at 5 antennas per instance instead, enforced = advertised `antennaLimit: 5`); `src=list` refused with `NO_SUCH_USER_LIST` (WP16 user lists pending — no list can exist yet); no `antennaCreated/Updated/Deleted` broadcast events (no broadcast bus; readers poll, stream subscribers get `note` frames); export `userListAccts` always null (no memberships to resolve); read-time filtering uses `FeedFilter` + visibility (upstream uses QueryService visibility + channel-mute brackets — the channel arm is vacuous here).

## §30 — Moderation/admin remainder audit (Batch N WS6)

> **Superseded 2026-10-20 (§39):** `reset-password` is shipped
> (`src/api/misskey/endpoints/mod.rs:7311`, §35), so the needs-decision row for
> it is stale. `server-info` (`mod.rs:7759`) and `stats` (`mod.rs:7764`) are
> shipped too. `queue/*` remains out: no dispatcher arm exists.

Upstream `admin/*` (~90 files) vs pinetree's routed set. Verdicts:
implement / out (with reason) / needs-owner-decision. Reference pins:
Misskey `2026.10.1-alpha.0` (`./misskey`).

| Upstream group | Verdict | Reason |
|---|---|---|
| `abuse-user-reports`, `resolve/update/forward-abuse-user-report`, `users/report-abuse` | **implemented (WS5)** | §24 contract frozen; routes landed Batch N |
| `abuse-report/notification-recipient/*` (5) | **out** | no email/webhook infra; §24 DIVERGENCE (admins poll) |
| `unset-user-avatar`, `unset-user-banner` | **implemented (WS6)** | moderator + scope; refusal rule on mod/admin targets (DIVERGENCE — upstream checks the role only); `unsetUserAvatar`/`unsetUserBanner` log rows; no-op when unset |
| `delete-all-files-of-a-user` | **implemented (WS6)** | `requireAdmin` parity; loops existing `delete_media` machinery; pinetree-named `deleteAllFilesOfAUser` log row with count (upstream logs nothing) |
| `drive/files`, `drive/show-file` | **implemented (WS6)** | moderator, read-only; new required `MediaStore::list_all_media` port (global newest-first scan; filters in the op); `type` MIME-prefix, `origin` combined/local/remote, `hostname` (null never matches locals, upstream parity); `show-file` `fileId` xor `url` (url matches served `url` or cached `source_url`), `NO_SUCH_FILE` `caf3ca38-…` verbatim |
| `roles/*` (9), `update-proxy-account`, `captcha/*` (2) | **out** | no custom-roles/captcha/proxy-account subsystems; fixed built-in roles only (§22/§23) |
| `emoji/*` (12) | **implemented (WS1)** | registry + admin management landed Batch N (§28); bulk/import-zip/copy/list-remote stay out (guide §8) |
| `ad/*` (4), `announcements/*` (4), `avatar-decorations/*` (4), `promo/create` | **out** | no ads/announcements/decoration/promo subsystems (profile §5 out) |
| `relays/*` (3) | **out** | no relay infra (stated §17) |
| `system-webhook/*` (6), `send-email` | **out** | no webhook/email infra |
| `queue/*` (13), `get-index-stats`, `get-table-stats`, `server-info` | **out** | no BullMQ/admin-queue UI; observability via logs |
| `accounts/delete` (vs routed `delete-account`) | **out** | duplicate of `delete-account`; one suffices |
| `federation/update-instance`, `federation/refresh-remote-instance-metadata` | **needs-owner-decision** | touches WP5 blocked-host surface; manual instance edit could be useful, auto-refresh needs fetch infra |
| `federation/remove-all-following`, `federation/delete-all-files` | **out** (default) | destructive bulk ops; confirm owner really wants them |
| `drive/clean-remote-files`, `drive/cleanup` | **out** | cleanup needs job infra beyond the delete loop |
| `accounts/find-by-email`, `get-user-ips`, `reset-password`, `unset-mfa` | **needs-owner-decision** | privacy/email decisions (IP/email lookup, password reset needs email or mint-and-display) |

Suggested default if owners don't weigh in: the WS6 five implemented above stay; everything else stays out with the reasons recorded here.

## §31 — Server-side word mutes (Batch N WS8)

Owner-approved DIVERGENCE from upstream (which enforces word mutes only
in the frontend). Full semantics + conformance index: `docs/word-mutes.md`.

- Matcher (`src/domain/word_mute.rs`, Misskey-exact): haystack
  `(cw ?? '') + '\n' + (text ?? '')` trimmed (CW first); self-exempt;
  empty-text exempt; case-sensitive substring AND-groups (empties dropped,
  emptied groups never match); `/pattern/flags` bare-string regexes
  (`i/m/s` honored, `g/d/u` inert, `y` start-anchored, anything else —
  notably `v` — never matches); self → reply → renote first-match-wins.
- **Hard** (`hardMutedWords`): `FeedFilter` drop on every timeline/note
  read (timelines, threads, search, `users/notes`, `notes/show`,
  `users/reactions`, `antennas/notes`, streaming eligibility + capture).
- **Soft** (`mutedWords`): no drop; viewer-specific `isSoftMuted` packed
  on the note when matched AND the note has no CW of its own.
- Notifications are NOT gated on word mutes.
- Perf: per-user sets compile once per read/pack (200-char write-time
  cap); reply/renote bodies resolve through a per-read note cache.
  Read-time only — FDB-safe (no transaction growth).

## §32 — Sharkey scheduling shim (Batch N tail)

Sharkey `notes/schedule/create|list|delete` as a thin alias over the
Misskey-model drafts. No new tables, job kinds, or ports. Reference pins:
Sharkey `2025.4.7` (`./sharkey`).

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `notes/schedule/create` | credential (any; upstream kind `write:notes-schedule` honored as a permission string, first-party passes), drafts 300/hr bucket (DIVERGENCE — no second rate-kind for an alias) | `visibility` (default `public`), `visibleUserIds?`, `cw?`, `reactionAcceptance?`, `noExtract*?` (accepted, ignored — same as drafts), `replyId?`, `renoteId?`, `text?`, `fileIds?`/`mediaIds?` (accept both, 1–16 ids each; `fileIds` wins — upstream `??` parity), `poll?`, `scheduleNote: {scheduledAt!}` (ms epoch, must be future) | Packed schedule-note (see list shape, single entry) |
| `notes/schedule/list` | credential (`read:notes-schedule`) | `sinceId?`, `untilId?`, `limit` 1–100 (default 10) | Caller’s scheduled drafts, newest-first, upstream shape (`id`, `userId`, `scheduledAt` ISO, `note: {text (default `''`), cw, fileIds + packed `files`, visibility (default `public`), visibleUsers, reactionAcceptance (default null), user, createdAt: scheduledAt-ISO, isSchedule: true, id, renote/reply (packed or null), renoteId/replyId, poll?}`) |
| `notes/schedule/delete` | credential (`write:notes-schedule`) | `noteId!` (the schedule entry id = draft id) | void (204); unknown → `NO_SUCH_NOTE` (`a58056ba-…`, Sharkey’s UUID); foreign → 403 `PERMISSION_DENIED` (`c0da2fed-…`). Deletes via the draft path (publish job cleared exactly once) |

Rules: the `anyOf` content rule (one of text/renoteId/fileIds/mediaIds/poll; blank text absent) is enforced up front (`INVALID_PARAM`); up-front target checks carry verbatim Sharkey ids (`NO_SUCH_RENOTE_TARGET`, `CANNOT_RENOTE_TO_A_PURE_RENOTE` `fd4cc33e-…` via a dedicated constructor since the shared code maps to Misskey’s `CANNOT_RENOTE` UUID, `NO_SUCH_REPLY_TARGET`, `CANNOT_REPLY_TO_A_PURE_RENOTE` `3ac74a84-…`, `CANNOT_RENOTE_DUE_TO_VISIBILITY`, `YOU_HAVE_BEEN_BLOCKED`, `NO_SUCH_FILE`, `CANNOT_CREATE_ALREADY_EXPIRED_POLL` judged against the *scheduled* time); `expiredAfter` converts to `expiresAt = scheduledAt + expiredAfter` (upstream parity; `expiresAt` wins when both present — DIVERGENCE from the drafts `expiredAfter`-wins rule, which applies only to the drafts endpoints); cap pre-check `count >= scheduled_note_limit` → `SCHEDULE_NOTE_MAX` (`168707c3-…`). No `channelId` exists upstream (nothing to reject); `CANNOT_RENOTE_OUTSIDE_OF_CHANNEL` stays unimplemented with no channel model. No `notes/schedule/update` upstream — edits go through `notes/drafts/update` on the underlying draft. Publish path untouched (`PostScheduledDraft` fires and posts through the normal path).

## §33 — MiAuth hardening (Batch N WS7)

DIVERGENCEs (upstream `2026.10.1-alpha.0` has none of these; all hardening):

- 30-minute approval window (`MIAUTH_APPROVAL_WINDOW_MS`) on MiAuth and legacy sessions (`expires_at_ms`, serde-default `None` for pre-expiry rows). Expired rows read as missing and are deleted on read (check polls `ok: false`; submit re-arms on re-approve; `userkey`/`show`/`accept` see `NO_SUCH_SESSION`/`NO_SUCH_*`).
- `GET /miauth/:session` never persists (used to upsert a row for any attacker-chosen session id on every view; the page renders from query params, submit persists on approve).
- Submit refuses suspended approvers (error page; no row/token minted — tokens would die at `authenticate` anyway).
- `miauth/gen-token` requires `session` + `permission` (upstream `paramDef.required` parity; was optional-with-defaults).
- Kept lax (upstream parity): permission strings are stored verbatim (upstream filters client-side only; no backend allowlist); `auth/session/generate` accepts any `appSecret` (no app registry; verified late at `userkey`, which answers `authentication_failed` on mismatch — documents `NO_SUCH_APP` as unemittable); the legacy approve URL stays the MiAuth page (legacy approval runs through `auth/accept`, not the URL).
- Rate limits: submit throttle + `miauth-check` per-IP bucket kept; `miauth/gen-token` rides the API dispatch with no dedicated bucket (upstream also none).

## §34 — Batch O: self-service, follower removal, drive extras, url-preview, user lists (handoff-batch-o)

Status: LANDED 2026-10-20 (`handoff-batch-o.md`; four investigation subagents A–D, serial build WP10 → WP9 → WP13+WP12 → WP16).

WP10 — follower removal + remote lookup. `following/invalidate {userId!}` (credential + `write:following`, 100/h): self → `FOLLOWER_IS_YOURSELF` (`07dc03b9-…`); unknown → `NO_SUCH_USER` (invalidate UUID `b77e6ae6-…`); not-a-follower → `NOT_FOLLOWING` (`918faac3-…`); else the edge is removed in the unfollow txn discipline and a remote follower gets a followee-authored `Reject(Follow)` (direction-aware federation: never a forged `Undo`; local-follower case keeps the `Undo` shape). Returns packed follower `UserLite`. `users/search-by-username-and-host {username?, host?, limit?, detail?}` (credential optional, 60/h): ≥1 of username/host required; thin alias over the `users/show` username/host resolution (incl. remote WebFinger+fetch); 1-element array (or `[]`), `detail=false` → Lite. `ap/get {uri!}`: safe-subset alias of `ap/show` (credential + `read:account` required, 30/h, packed output — never upstream's raw-object return; SSRF-guarded fetch, `NO_SUCH_OBJECT` on any failure).

WP9 — self-service. `i/change-password {currentPassword!, newPassword! (min 1), token?}` (first-party only, 10/h; wrong password → `AUTHENTICATION_FAILED` 401 + auth-failure throttle, like signin; 2FA `token` accepted-and-ignored — no 2FA exists; sessions NOT revoked, upstream parity; 204). `i/regenerate-token {password!}` (first-party only, 10/h; wrong password → 401 + throttle; rotates the native token = deletes ALL first-party grants of the caller, app grants survive per owner decision §9.7; 204 — caller re-signs-in). `i/apps` (first-party, 60/h; non-first-party grants; `±createdAt`/`±lastUsedAt` accepted, `lastUsedAt` always null → lastUsedAt sorts fall back to creation order) and `i/authorized-apps` (`limit`/`offset`/`sort`, one app-shaped row per grant). `i/revoke-token {tokenId}` is real (grant must belong to the caller; third-party callers restricted to their own row like the `token` branch; unknown/foreign ids stay silent no-ops). `i/signin-history` (first-party, 60/h; cursor `limit`/`sinceId`/`untilId`/`sinceDate`/`untilDate`; `{id, createdAt, ip, headers, success}`). New `SigninRecord` store + `SigninStore` port (insert + newest-first page, per-account cap `MAX_SIGNIN_ROWS_PER_USER = 100` trimmed oldest-first in the same txn); hooks in password-signin success, known-account failure, and signup success (MiAuth/legacy approvals write no row — upstream parity). `ApiTokenRecord.id` (aidx, backfilled at open with `API_TOKENS_BY_ID` index) backs `tokenId` revoke + `i/apps`.

WP13 + WP12 — drive extras + preview. `drive/files/upload-from-url {url!, folderId?, isSensitive?, comment? (≤512), marker?, force?}` (credential + `write:drive`, 60/h): SSRF-guarded fetch + `upload_media` dedup path (`force` semantics identical; `comment == name` nulled; sync in-request fetch returning the packed file). `folderId` must be null (no folders); `marker` opaque, accepted-and-ignored (no `urlUploadFinished` stream event exists). `drive/files/find-by-hash {md5!}` (credential + `read:drive`, 60/h): owner-scoped lookup over the ingest `hash` column (md5 stored alongside SHA-256 at every ingest; never matched against blob keys; miss → `NO_SUCH_FILE`). `url-preview {url!}` (first-party Keystone shape `{title?, description?, siteName?, image?, icon?}`; credential + `read:account` REQUIRED — anonymous preview is an open proxy; 60/h; SSRF + 256 KiB + HTML-only parse of `og:*`/`<title>`/icon link; http(s)-only asset URLs; in-process cache ~100 entries/1h TTL, NOT in the store).

WP16 — user lists. `users/lists/create|update|delete|list|show|push|pull|get-memberships|update-membership|favorite|unfavorite` (write endpoints credential + `write:account`; list/show/get-memberships `read:account` when credentialed, anonymous allowed for the public shapes; push 1h/30 like upstream). Verbatim upstream ids throughout (one UUID per endpoint per code: see `errors.rs` `LIST_*` constructors; `update.ts 796666fe-…`, `delete.ts 78436795-…`, `show/get-memberships 7bc05c21-…`, `pull/update-membership 7f44670e-…`, `push NO_SUCH_USER a89abd3d-…`, `TOO_MANY_USERLISTS 0cf21a28-…`, `ALREADY_ADDED 1de7c884-…`, `YOU_HAVE_BEEN_BLOCKED 990232c5-…`, `TOO_MANY_USERS 2dd9752e-…`, `favorite 7dbaf3cf-…/6425bba0-…`, `unfavorite baedb33e-…` + `notFavorited` quirk `835c4b27-…` on wire code `ALREADY_FAVORITED`, `REMOTE_USER_NOT_ALLOWED 53858f1b-…`, anonymous `list{}` → `INVALID_PARAM ab36de0e-…`). Limits enforced + advertised: `userListLimit 10`, `userEachUserListsLimit 50` (`packers.rs` fixed 5 → 10). Visibility: public readable, private owner-only, `list{userId}` refuses remote users. Timeline is pull (merge members' newest-first, bounded 50 members × 100 notes, per-membership reply rule + include flags, shared visibility/mute/block page; `channelId` n/a — no channels). `notes/user-list-timeline` owner-only (timeline UUID `8fb1fbd5-…`). Remote members queue a system-actor Follow (removal leaves it). `userList` stream channel (credential + owner match up front, membership re-read per event; `userAdded`/`userRemoved` lite-packed; note fan-out with the read-time rule). List receive-configs now deliver from members only (dangling/foreign → nothing, never error). CSV export/import real (`listName,acct,withReplies=`). `create-from-public` stays 404. New `UserListStore` required port (redb tables `user_lists*` + favorites; cap checks + mutation + jobs in one txn). DIVERGENCEs: `i/authorized-apps` emits one row per grant (no app registry to dedupe by); `headers` in signin rows keeps user-agent only; preview cache successes-only; `get-memberships` rows carry generated `id`/`createdAt` (upstream `MiUserListMembership.id`, same shape).

## §35 — Batch P: email identity, account move, registry, small reads, timelines, notifications, discovery, versions, translate (handoff-batch-p)

Status: LANDED 2026-10-20 (`handoff-batch-p.md`; solo build, upstream pins Misskey `2026.10.1-alpha.0` + Sharkey `2025.4.7` read from the reference checkouts).

§1 — email identity loop. `i/update-email {password!, email?, token?}` (credential + secure, 3/h; wrong password → `INCORRECT_PASSWORD` `e54c1d7e-…` 400 + `auth/update-email` failure throttle; 2FA `token` accepted-and-ignored; malformed/taken address → `UNAVAILABLE` `a2defefb-…` — shape check + verified-address uniqueness, unverified rows do not block like upstream `validateEmailForAccount`; returns MeDetailed with real `email`/`emailVerified` secrets). `request-reset-password {username!, email!}` (no credential, 3/h): unknown user / mismatch / unverified are silently absorbed (upstream leak posture), then the send fails loudly and UNIFORMLY with first-party `EMAIL_NOT_CONFIGURED` `c41d9a2e-…` (no oracle distinguishes the cases). `reset-password {token!, password!}`: unknown token → 500, >30 min (TTL off the aidx id) → 500 + row dropped (upstream bare-throw parity); success rotates the hash and consumes the row (single-use, replay → 500). `verify-email {code!}`: unknown → `NO_SUCH_CODE` `97c1f576-…` 404; success verifies + clears. Owner decision: NO SMTP anywhere — the loop is code-complete and dormant (state transitions real, sends loud no-ops, verified-code path exercisable). New `PasswordResetStore` port (token + id indexes, redb `password_resets*`; conformance W1) and account columns `email`/`email_verified`/`email_verify_code`.

§2 — account move (federation-sensitive). `i/move {moveToAccount!}` (credential + secure, 5/day): oldest-admin refusal (`NOT_ROOT_FORBIDDEN` `4362e8dc-…` — pinetree stores no `rootUserId`; root = lowest account id among admins, DIVERGENCE), already-moved → `ALREADY_MOVED` `b234a14e-…`, unparseable/unresolvable → `NO_SUCH_USER` `fcd2eef9-…` (shared id), remote destination without URI → `URI_NULL` `bf326f31-…`, missing alias or moved destination → `DESTINATION_ACCOUNT_FORBIDS` `b5c90186-…` (upstream substring-`includes` parity). One txn commits `movedToUri`/`movedAt`/alias-merge + `ActorUpdated` + `MoveDelivered` jobs (id `{url}/moves/{src}/{dst}`, minted once so replays dedupe); local followers migrate inline (paged, one txn per edge). Inbound `Move`: forged (object ≠ signer) → 401; blocked hosts drop before dedup; replay converges (dedup id recorded before edge work); local followers migrate the same way. Actor docs render/parse `alsoKnownAs` + `movedTo` (upstream `renderPerson`/`ApPersonService` parity — previously absent, which broke the handshake); remote upserts refresh both columns (create path rebinds the seeded row — first sight previously returned the stale struct). `alsoKnownAs` editing needed no new params (§0B confirmed) beyond the local-actor-URL fix in `resolve_alias_target` (local rows carry no stored `uri`). `movedTo` packs the real URI (was hardcoded null). Live two-pinetree verification: follow → alias → move → refollow, replay, forgery (`tests/federation.rs federation_account_move`).

§3 — `i/registry`. Full family `get|set|get-all|get-detail|keys|keys-with-type|remove|scopes-with-domain` (reads `read:account`, writes `write:account`; scope segments `^[a-zA-Z0-9_]+$`, `set` key min 1; `get/get-detail` miss → `NO_SUCH_KEY` `ac3ed68a-…` 404). Domain = caller token id when credentialed, else explicit `domain`/null (upstream quirk parity). New required `RegistryStore` port (one encoded composite row per (user, domain, scope, key); redb `registry`; conformance W2). No federation, no jobs. `registryUpdated` stream events not emitted (no such channel consumer — recorded).

§4 — small reads. `get-online-users-count` (no credential): "online" = live authenticated streaming sockets seen within 10 min (`Hub` presence map, marked on connect/message, unmarked on close; Misskey `USER_ONLINE_THRESHOLD` window parity, Redis-heartbeat definition DIVERGENCE — documented on the hub). `pinned-users` → `[]` (no admin surface for pinning users exists; shape-compatible). `fetch-external-resources {url!, hash!}` (credential + secure, 50/h): SSRF-guarded GET + JSON `{type, data}` schema check (`EXT_RESOURCE_RETURNED_INVALID_SCHEMA` `bb774091-…`) + sha512-hex over CRLF-normalized data (`EXT_RESOURCE_HASH_DIDNT_MATCH` `693ba8ba-…`). `fetch-rss {url!}` (no credential, 300/min): SSRF-guarded 1 MiB fetch; minimal RSS2 + Atom subset (`title/link/description/feedUrl/items[]` with `title/link/guid/pubDate/creator/summary/content/isoDate/categories`; `image/paginationLinks/itunes/enclosure/contentSnippet` omitted — recorded); bad scheme → `INVALID_URL` `89b7ee05-…` 400, fetch/parse failure → `FETCH_RSS_FAILED` `8db5d3d8-…` 422 (`FETCH_RSS_UNAVAILABLE` reserved — no concurrency cap needed in-process). `server-info` (no credential): `{machine, cpu{model,cores}, mem{total}, fs{total,used}}` from consts + `available_parallelism` + `/proc` (non-Linux → arch/unknown + zeros); `fs` totals are 0 (no statvfs without libc — DIVERGENCE). `stats` (no credential): real store counts `{notesCount, originalNotesCount (local), usersCount, originalUsersCount (local), reactionsCount, instances (distinct remote hosts), driveUsageLocal/Remote (media bytes by owner locality)}` — upstream hardcodes both drive usages to 0 (DIVERGENCE, ours are real). New defaulted-then-redb count methods (`count_accounts/notes/local_notes/reactions`). No hashtag timeline upstream (`hashtags/` has list/search/show/trend/users only) — `notes/search-by-tag` stays the answer, no new endpoint.

§5 — `notes/mentions {following=false, limit, sinceId/untilId/sinceDate/untilDate, visibility?}` (credential + `read:account`): mentions + `visibleUserIds` addressed to the viewer; `following` restricts authors to followees + self (upstream branch direction verified in the handler); `visibility` is an exact-match narrowing (unknown values match nothing — upstream free-string parity); frozen visibility + mute/block/thread filters on top (mentioned viewers see followers notes per frozen §3).

§6 — notification utilities. `notifications/test-notification` (credential + `write:notifications`, 10/min): type `test` through the normal `Notify` path (self-notified; worker notifier-less gate, so only a `never` test-config drops it; grouped rendering untouched). `test` added to the emittable type parser (it was wrongly in the unemitted list). `notifications/flush` (credential + `write:notifications`): closest honest equivalent of `flushAllNotifications` — durable rows have no Redis timeline to drop, so flush marks all read + emits upstream's `notificationFlushed` main event (new `MainEventKind`, wire-safe).

§7 — discovery. `users/recommendation {limit (1..=100), offset}` (credential + `read:account`): upstream shape (`UserDetailed[]`); documented-simplified scoring — unlocked + explorable local, minus self/followed/muted/blocked/suspended/deleted, by `followersCount` desc (upstream's 7-day `updatedAt` recency has no pinetree column — DIVERGENCE). `notes/recommendations {limit (1..=30), offset, refresh?, lang?, withSensitive=false, withRenotes=true}` (credential optional): Sharkey SHAPE parity (`Note[]`) over the same scorer (newest visible notes by suggested authors; anonymous falls back to all visible); Sharkey's `RecommendationService` queue/lanes/re-rank is an explicit non-goal — `refresh`/`lang` accepted-and-ignored, `withSensitive` drops sensitive-attachment notes, `withRenotes` drops pure renotes (DIVERGENCEs).

§8 — versions + translate. `notes/versions {noteId!}` (credential optional, 10/5s): Sharkey SHAPE parity (`{oldDate, updatedAt, text}[]` newest-first, `text = oldText ?? newText`) over our `note_edits` rows; visibility first (miss/invisible → shared `NO_SUCH_NOTE` 400-house-convention), then `SIGNIN_REQUIRED` `8e75455b-…` for signin-gated authors. `notes/translate {noteId!, targetLang!}` (credential + `read:account`): upstream order + errors (`NO_SUCH_NOTE`, `CANNOT_TRANSLATE_INVISIBLE_NOTE` `ea29f2ca-…`, empty text → null, `targetLang` truncated at `-`); no backend → loud `UNAVAILABLE` (first-party `TRANSLATE_NOT_CONFIGURED` `d52ea1f3-…`; DeepL-compatible keys reserved, never wired). Never proxies arbitrary fetch.

DIVERGENCE index (all decided, reasons inline): dormant email/translate transports (no SMTP/keys in repo); uniform reset error (no oracle); root = oldest admin; online = streaming presence; flush = mark-read + event; stats drive sums real (upstream 0) + instances = distinct hosts; fs totals 0; recommender scoring simplified (users) / thin alias (notes); versions over `note_edits`; `pinned-users` empty; RSS subset; registry without `registryUpdated` events.

## §36 — Federated clips + pages (handoff-federated-clips-pages.md)

Status: LANDED 2026-10-20 (Phase 0 fetch gate + Phase 1 local clips + Phase 3 local pages + Article/collection wire shapes + delivery/inbox paths + remote-page materialization; two-pinetree protocol tests green; `admin/update-meta` gains `federateClips`/`federatePages`).

Phase 0 — generic authenticated-fetch gate (`src/federation/fetch_gate.rs`, shared with the private-community track `docs/community-planning.md`): `authenticate_fetch_viewer` (HTTP-Signature verify on inbound GETs, same key-lookup discipline as the inbox; every failure mode collapses to an anonymous viewer — never 401/403) + `decide_fetch` (`Serve` → 200, `Forbidden` + identified → 403, `Forbidden` + anonymous → 404, `Hidden` → 404). No object-type logic below the authorization callback. Gate matrix test (`fetch_gate.rs` tests: serve/403/404 across public/unlisted/approved-remote/stranger/unsigned + live sign/verify + tamper cases) green. Interface contract: `docs/federated-collections.md` §1 (the community implementer codes against it).

Phase 1 — local clips (Misskey parity, 11 endpoints): `clips/create|update|delete|list|show|notes|add-note|remove-note|favorite|unfavorite|my-favorites` (verbatim params incl. `name` 1–100, `description` ≤2048 nullable with `''→null`, limits 1–100 default 10, `add-note` 20/h; verbatim per-endpoint error UUIDs — `CLIP_*` constructors in `errors.rs`, all 30 clip/page UUIDs verified against the upstream checkout). `show`/`notes`/`favorite` hide private clips as `NO_SUCH_CLIP` from anonymous/non-owners; `unfavorite` skips the visibility check (upstream parity). `notes` packs visibility/mute/block-filtered notes with per-word text/cw search. Caps: 10 clips/owner (`TOO_MANY_CLIPS`), 200 notes/clip (`TOO_MANY_CLIP_NOTES`), unique pairs (`ALREADY_CLIPPED`), unique favorites, `my-favorites` unpacked. New `ClipStore` required port (redb `clips*` tables; cap checks + mutation + `lastClippedAt` touch + jobs in one txn; conformance Group X) + packer `pack_clip` (`notesCount` only for the owner, `isFavorited` only authed, federation columns never packed).

Phase 2/3 wire (DIVERGENCE — Misskey has no AP representation): clips as `OrderedCollection` at `/clips/{id}` (note URIs only, per-item visibility gating for the verified fetcher, page size 200) + advertised index at `/users/{id}/clips` (listed + federate-enabled only) + `clips` actor-doc reference (omitted while the global switch is off); pages as `Article` at `/pages/{id}` (`source` carries content blocks + variables; **`script` structurally excluded** — serializer has no code path that emits it; block renderer escapes text + fixed tag whitelist + server-constructed image URLs). Privacy tiers shared (`Private → LocalApproved → Unlisted → Listed → RemoteApproved`, `CollectionTier`); per-object `federate` flags (clips default OFF, pages default ON) + admin `federateClips`/`federatePages` switches (default OFF; either off ⇒ 404 + no advertisement). Membership sync: full-state paging + 10-min TTL (`resolve_remote_clip` + `resolve_remote_clip_with_ttl` seam; bounded process-memory cache) + `Add`/`Remove` promptness (outbound to remote note authors' inboxes; inbound verifies owner by collection fetch and evicts the cache — never mutates local clips). Page create/update fan out `Create`/`Update(Article)` to followers (listed tiers only); likes on materialized remote pages federate back as `Like` page-target (inbound accepted + counted, owner notified via `pageLiked`; co-located likes need no delivery). Inbound well-formed `Article`s materialize as remote pages with audience-mapped visibility (canonical `uri`, updates/deletes/likes by URI, script never stored); only malformed objects keep the note pipeline (Phase 2 §38 supersedes the public-only line). Test harness: each `FedNode` runs its own Tokio runtime (production-faithful isolation; a shared test runtime wedged under blocking client I/O — see state log).

Phase 3 local pages (Misskey parity, 7 endpoints): `pages/create|update|delete|show|like|unlike` (verbatim params incl. slug `pageNameSchema` + per-user uniqueness, `summary`/`eyeCatchingImageId` nullable, `font serif|sans-serif`, `update` partial-merge with null-clears, `update` 300/h, `create` 10/h; verbatim per-endpoint UUIDs — `PAGE_*` constructors) + `pages/featured` (implemented as the upstream pure query top-10 by `likedCount`, NOT 404: the handoff assumed an admin surface, but upstream `featured.ts` has none — evidence-backed override recorded here). `show` by id or `(name, username)` (local users only); `delete` owner-or-moderator (moderator writes `deletePage` log). Page pack matches `packedPageSchema` (`attachedFiles` from image blocks, `eyeCatchingImage`, `isLiked` only authed). New `PageStore` required port (redb `pages*` tables incl. `(owner, name)` uniqueness; conformance Group Y).

Export/import: `export-clips` writes real upstream `serializeClip` shape (invisible/gone notes skipped); NEW pinetree-only `i/import-clips` consumes it (find-or-create by name, visible-note attach; no upstream endpoint exists — pinetree-fixed UUIDs). Round-trip green.

DIVERGENCEs: no per-clip stream channel (no profile client subscribes); no `clippedCount` counter (derived at read); `featured` implemented per upstream evidence against the handoff SKIP; `i/import-clips` is pinetree-only; `script` stored locally but never federated/executed; remote-approved trust caveat (same as DMs); page likes notify the owner (`pageLiked`; Phase 2 §38 supersedes the silent-likes line).

## §37 — Batch Q Phase 1: audit-fix slice (dispatch half; app/federation halves land with their snippets)

Status: LANDED 2026-10-20 (all nine items; Phase 0 verified each still open at HEAD — items 6/7 partially).

- Item 1 — `following/requests/list` permission kind: was `read:account`, now `read:following` (upstream `following/requests/list.ts:18`). A `read:account`-only token is `PERMISSION_DENIED`; a `read:following`-only token succeeds.
- Item 4a — `i/notifications` + `i/notifications-grouped` rate buckets: 30 requests per 30 s each (upstream `i/notifications.ts:23-26`, `i/notifications-grouped.ts:28-31`). 31st request in the window → 429 `RATE_LIMIT_EXCEEDED`.
- Item 4b — empty-filter ordering: `includeTypes: []` (or excluding every emittable kind) returns `[]` *before* the mark-as-read step on both endpoints (upstream `notifications.ts:72-79` vs `:93-95`). An empty filter with default `markAsRead: true` leaves everything unread.
- Item 5 — `withReplies` default: omitted `withReplies` is now `false` on `following/create` and `i/import-following` (upstream declares no default; the value flows into the relationship row whose column default is false — `Following.ts:49-52`, `FollowRequest.ts:48-51`; the import processor treats omitted as falsy unless a CSV row overrides). The old `§13` DIVERGENCE (`true` default) is removed, not kept.
- Item 6 — `notes/create` + `notes/edit` file keys: `fileIds ?? mediaIds ?? []` (upstream `notes/create.ts:226`). `mediaIds`-alone still attaches as the legacy fallback; both-keys no longer unions.
- Item 9 — `meta.mediaProxy` is now truthful: `GET /proxy` (+ cosmetic `/proxy/{*rest}`, mirroring upstream `FileServerService.ts:106`) serves the `?url=` target through the SSRF-guarded federation HTTP client (private/reserved IPs refused), 8 MiB cap, inline-safe media only (`image/*` except SVG, `audio/*`, `video/*`), `Cache-Control: max-age=31536000, immutable`, sandboxed no-sniff headers. No auth (clients embed proxied URLs in `<img>` tags); User-Agent required, `misskey/` UA refused (loop protection, upstream `FileServerProxyHandler.ts` parity); fetch failure/non-2xx → plain 404.
- DIVERGENCEs (decided): `/proxy` has no thumbnailing/transcoding and no `emoji`/`avatar`/`static` variants (media-pipeline workstream); no `dummy.png` fallback (plain 404); proxied SVG refused rather than sanitized (never served as active content in the site origin); `/proxy` carries no rate bucket (matches upstream; abuse surface bounded by SSRF guard + size cap + media allowlist).
- Item 2 — `notes/delete` moderator delete: the manifest §4 promise was correct (non-owner/non-moderator → `ACCESS_DENIED`), the code was wrong (`delete_note` rejected every non-author). Moderators (admin implied, via `Account::has_moderator_powers`) may now delete others' notes; the `ACCESS_DENIED` code is untouched. `unrenote` inherits the bypass.
- Item 3a — `users/show` suspension: single `userId`/`username` paths return verbatim 404 `NO_SUCH_USER` (`users/show.ts:52-57` id `4362f8dc-…`) for suspended users unless the viewer is a moderator; the `userIds` batch silently omits missing/suspended rows for non-moderators (moderators see suspended rows), matching `users/show.ts:136-142`.
- Item 3b — `notes/show` content gates (upstream `notes/show.ts:69-79`): anonymous + author `requireSigninToViewContents` → verbatim `CONTENT_RESTRICTED_BY_USER`; new `InstanceSettings.ugc_visibility_for_visitor` (`all|local|none`, admin-writable via `admin/update-meta`, readable via `admin/meta`, upstream `admin/meta.ts:590-594` shape) drives verbatim `CONTENT_RESTRICTED_BY_SERVER` (`none` + anonymous, or `local` + anonymous + remote author). `notes/conversation` returns `[]` for anonymous + `none` (upstream `conversation.ts:62`). The pre-existing fail-closed pending-reply withhold is ordered *before* both gates so a pending note never leaks existence through a gate error.
- DIVERGENCE (decided): pinetree default is `all`, upstream column default is `local` — our public local/global timelines serve remote notes to visitors today, so gating only detail/show while timelines leak the same notes would be incoherent; revisit when timeline-level ugc filtering lands (upstream also gates timelines/streaming/pack at `FanoutTimelineEndpointService.ts:154-158`, `NoteEntityService.ts:136-137,606-607`, `NoteStreamingHidingService.ts:50-51`, `QueryService.ts:116` — all currently out). The upstream-dead `ugc === 'none'` branch in `users/show.ts` (commented out) is not implemented.
- Item 7 — home + hybrid timelines honor `includeMyRenotes`/`includeRenotedMyNotes`/`includeLocalRenotes` (defaults true, upstream `notes/timeline.ts:48-50` + `getFromDb :220-248` bare-reshare rule shared with the user-list path; hybrid filters the merged home ∪ local-public set so a viewer's own renote cannot leak via the local arm).
- Item 8a — inbound featured `Add`/`Remove` (target exactly `{signer}/collections/featured`) pins/unpins through the same `NoteStore` pinning the local path uses; other `*/collections/featured` targets are accepted-without-effect; authority = target-is-signer's-own-collection + note-belongs-to-signer; duplicates converge (`Conflict` → success, missing-row remove → no-op).
- Item 8b — inbound `Undo(Accept(Follow))` (embedded shape) tears down the follow edge via the same path as `Follow`-undo, with signer/authority checks; duplicate-safe.
- Item 8c — inbound `Delete(Person)`: known remote account URI (+ embedded-actor verification for unknown URIs) tombstones via `moderation.delete_account` (follows/requests torn down both ways); local URIs are never touched.
- DIVERGENCE (decided): bare-IRI (by-ID) `Undo(Accept)` converges accept-without-effect — there is no activity-ID index to resolve it against, and embedded shape is what Misskey sends; adding the index is a schema/ports change, deferred.

## §38 — Federated clips + pages, phase 2 (handoff-federated-clips-pages-phase2.md)

Status: LANDED 2026-10-20 (pages visibility + gated ingest, clip sync + follow + follower-wide push, Article fidelity small wins; two-pinetree clip + page lifecycle tests green).

- Owner decisions (recorded here, per the handoff): no new object type — everything shippable fits in `Article` + the existing `source` extension (a custom type is revisited only if a second implementation wants block-level interop); gated (non-public) `Article`s materialize as pages, not notes (page-level read gating landed first); followers-list = owner-only list + self-status for others (`isFollowing` in the clip pack); notification noise = quieter (membership edits → note authors only; delete/restrict → followers + member authors; followers NOT spammed on every membership edit); `clips/follow` permission = `write:clip-favorite` (symmetry with clip favorites); clip tombstones kept forever in v1 (tiny rows, no prune, no retention setting).
- Pages read gate: `Page.visibility` (note enum reuse; local creates default `Public`) + `visible_user_ids`, both `#[serde(default)]`. Unauthorized ⇒ `NO_SUCH_PAGE`-hiding on `pages/show` (id + name forms), AP `GET /pages/{id}`, `pages/like`, `pages/featured` (public only), and the pack path. Owner always passes; moderators bypass. Remote-followers rule: a local viewer passes a remote author's followers-only page iff they follow that author; a remote viewer of a cached copy fails closed (same posture as federated DMs). New params `pageVisibility` + `visibleUserIds` (pinetree extensions, DIVERGENCE — the existing `visibility` param keeps meaning tier). Ingest mapping `to`/`cc` → visibility (Public-in-`to` ⇒ Public; Public-in-`cc` ⇒ Public, explicit collapse; followers collection ⇒ Followers; else Specified with best-effort ids, fail-closed). Update path carries visibility forward. Full contract: `docs/federated-collections.md` §§2/4.
- Clip sync: `updated` on the collection document (served + compared before full pulls); `CLIP_TOMBSTONES` index (deleted URIs serve `Tombstone`/`formerType: OrderedCollection` as 410 while the switch is on, 404 with the switch off); `Update(Collection)` (embedded full doc, one round trip) for metadata edits; `Delete(Collection)` on delete and tier-restriction flips (receivers evict + prune; replays idempotent; forged owner-mismatches ignored; tier-restrict also prunes remote follow rows, delete cascades all follows). Fan-out rule tightened to `Listed` + flag + switch (documented behavior change: unlisted-with-flag loses broadcast, keeps capability fetch). Recipients: note authors (existing) + owner followers (new) + clip followers (new), sharing one activity id per event.
- Clip follow (pinetree-first, DIVERGENCE, pinetree-fixed UUIDs): `clips/follow|unfollow` (idempotent; `write:clip-favorite`), `clips/followers` (owner-only, else 403 `ACCESS_DENIED`), `isFollowing` in the clip pack. Federation `Follow`/`Undo(Follow)` on collection URIs → owner personal inbox (authority = signer == follower; readable-clip gate; Misskey id-reuse: both ids forgotten on Undo so re-follow converges). No new stream channel (notifications only — recorded).
- Notifications: `clipUpdated` + `pageLiked` wire types (pinetree-only, `ExportCompleted` precedent); nullable `clip_id` + `page_id` on rows and `NotifyJobPayload`; packed as `clipId`/`clip` + `pageId`/`page`; `includeTypes`/`excludeTypes` accept both; `ALL_NOTIFICATION_KINDS` covers them (14 entries incl. `test`). Emit: membership → note author; metadata → followers; delete/restrict → followers + member authors; page like → owner. Worker side-channels: body-only like `exportCompleted`/`test`.
- Article fidelity (inside `Article`, no new type): origin timestamps (`remote_published_ms`/`remote_updated_ms`, served in place of row times); outbound `Delete(Article)` on local page delete; `source` schema documented + honored (`content` + `variables` + `presentation {font, alignCenter, hideTitleWhenPinned}` + `slug`; no new top-level fields); audience audit (emitted `to`/`cc` matches visibility per the §4 table).
- Stores: `ClipStore` gains follows + tombstones, `update_clip`/`delete_clip` take jobs (atomic fan-out), `remove_clip_note` takes `now_ms` (same-txn `updated` bump; adds bump too), `PageStore.delete_page` takes jobs (atomic `Delete(Article)`). Conformance Group Z (follows converge/cascade + tombstone round-trip) + Group Y4 (visibility + origin timestamps); suite now 118 green on redb.
- Keystone contract (no new stream channel): new endpoints `clips/follow|unfollow|followers`, new params `pageVisibility`/`visibleUserIds`, new pack fields `isFollowing`/`clipId`+`clip`/`pageId`+`page` on notifications, new wire types `clipUpdated`/`pageLiked` for `notificationRecieveConfig` + include/exclude filters.

## §39 — Freshness pass addendum (2026-10-20, code-verified)

Status: ADDENDUM only. Frozen text above is not rewritten; the superseding notes at the top of §§6, 9, 13, 14, 15, 19, and 30 point here. Verified against the dispatcher arms in `src/api/misskey/endpoints/mod.rs`, `src/streaming/server.rs`, `src/federation/inbox.rs`, `src/federation/delivery.rs`, `src/application/queries.rs`, and `src/application/commands/notifications.rs`. Each row cites the code that proves it.

### 39.1 Corrections (supersede the cited body text)

| Claim (section) | Code truth 2026-10-20 | Evidence |
|---|---|---|
| Streaming `updated` "not emitted" (§9) | **Emitted.** `HubMessage::NoteUpdated` pushes `updated {cw, text}` via `push_note_updated` | `src/streaming/server.rs:700-728` |
| `antenna` / `userList` channels "unsupported in Phase 0" (§6) | **Shipped.** Owner-checked antenna; list channel with membership re-read | `server.rs:341-345`, `:358`; manifest §29, §34 |
| `pollEnded` on note stream (§6, §15) | Emitted as a note-capture `pollEnded` frame; the notification path is separate | `server.rs:694` |
| Main stream events (profile §2 list) | Subset is correct. Also emitted: `edited`, `unfollow`, `receiveFollowRequest`, `notificationFlushed` | `src/application/events.rs:67-99` |
| Out-list: antennas, clips, pages, lists, featured, mentions, user-list timeline (§9) | **Shipped** | antennas `mod.rs:5268`; clips `:3489-3906`; pages `:3944-4085`; lists `:3152-3463`; featured `:8998`, `:9029`; mentions `:7784`; user-list timeline `:4121` |
| `notes/featured`, `users/featured-notes`, `i/notifications-grouped`, `notes/reactions` (list) listed "out" (profile §5) | **Shipped** | `mod.rs:8998`, `:9029`, `:9067`, `:6278` |
| `reset-password` under needs-decision (§30) | **Shipped** (uniform 500 on unknown/expired = upstream parity) | `mod.rs:7311`; §35 |
| `server-info` out (§30) / `stats` | `server-info` and `stats` **shipped** | `mod.rs:7759`, `:7764` |
| Moderator bypass "wired but always false" (§13, §14) | **Live.** `viewer_is_moderator` → `Account::has_moderator_powers()`; used by the §1 gate and the reactions gate | `src/application/queries.rs:1704-1713`, `:1737`, `:1853` |
| `notes/timeline` `withReplies` "kept, decided" (§19) | **Confirmed absent** → `UNSUPPORTED_FIELD` via `check_allowed_fields`. Closed | `mod.rs:6132-6147` |
| `notes/search` `offset`/`rangeStartAt`/`rangeEndAt` rejected (profile §5 last bullet) | **Accepted.** `channelId` still rejected | `mod.rs:8146-8178` |
| `Update(Note)` `TODO(§8)` accept-and-ignore (§15, ~line 272) | **Edit branch landed.** `Note`/`Article`/`Page` → `handle_update_note` | `src/federation/inbox.rs:1796-1797`, `:1953`; `notes/edit` `mod.rs:4302` |
| `list(userListId)` notification receive-config "drops, no lists" (§19; parity plan §11) | **Live.** Members deliver; dangling/foreign lists drop silently | `src/application/commands/notifications.rs:113-120` |
| `i/revoke-token` `tokenId` "no-op" (profile §7) | **Implemented.** Grant must belong to caller; unknown/foreign ids are silent 204 | `mod.rs:6770-6799` |
| `notes/search` anonymous status (profile §3 table says `read:account`) | **Public** (profile §7 is right). `search-by-tag` also public | `mod.rs:8162` ("public endpoint"), `:8202-8203` |
| `i/notifications` scope (profile §3 table says `read:account`) | **`read:notifications`** (profile §7 is right) | `mod.rs:6475-6476` |
| `i/update-email` "needs §9.2 decision" (profile §5) | **Shipped** (§35), dormant SMTP | `mod.rs:7193` |

### 39.2 Still open or out (verified absent or divergent)

- `users/lists/create-from-public`: **no dispatcher arm** (grep of `src/` → 0 hits) → 404. Deferred, unchanged.
- No dispatcher arm, so 404 `UNKNOWN_API_ENDPOINT`: `queue/*` (and `admin/queue*`), `channels/*`, `chat/*`, `hashtags/*`, `drive/folders/*`, `i/2fa/*`, `my/apps`, `app/*`, `notes/clips`, `users/update-memo`, `notes/thread-muting/*`, `notes/polls/recommendation`, `following/update|update-all|list`, `following/requests/sent`, `announcements*`, `i/read-announcement`, `drive/files/{find,move-bulk,check-existence,attached-notes}`. Dispatcher arm list checked against `grep ^\s+"…" =>` on `mod.rs`.
- `notes/reactions` cursor: `sinceId`-only still reads **newest-first** (`queries.rs:1944` always sorts descending). The DIVERGENCE in the §4 row stands; the TODOS verify item "confirm still diverges" is confirmed.
- Streaming: `reacted` body is `{reaction, userId}` only (no `emoji` detail; `server.rs:642-643`). Channels `hashtag`, `drive`, `clip`, `admin`, `roleTimeline`, `serverStats`, `queueStats`, `chat*`, `reversi*` fall through to `UNSUPPORTED_CHANNEL` (`server.rs:405-420`).
- Streaming `userList` ignores `withFiles`/`withRenotes`: per the inventory (`server.rs:399`), not re-read in this pass.
- Authentication for streaming: a token without `read:account` gets 401 (`server.rs:119-121`), matching §4 of the profile.

### 39.3 Not individually verified this pass

- Drive `sort` variants, `users/show` `userIds` batch, and `users/search` defaults (not re-traced).
- Exact `NO_SUCH_USER` UUID unification (`fcd2eef9-…`) across endpoints (sampled via §§31, 34, 35 notes; not re-grepped).

## §40 — Custom-emoji completion (handoff `docs/handoff-emoji-completion.md`, 2026-10-20)

Status: ADDENDUM only. Closes the §28 out-of-scope row and the TODOS
streaming `emojiAdded/Updated/Deleted` hole. Every item below is implemented
and executed-green (lib + conformance + the two new contract files), except
the two recorded DIVERGENCEs. Evidence is `file:line` against the 2026-10-20
tree.

### 40.1 New endpoint arms (`src/api/misskey/endpoints/mod.rs`)

| Endpoint | Auth | Params → result |
|---|---|---|
| `admin/emoji/add-aliases-bulk` (`:5264`) | credential + `write:admin:emoji` + moderator | `ids*` (≤100), `aliases*` (≤32) → 204; union per row, unknown ids skipped |
| `admin/emoji/remove-aliases-bulk` (`:5282`) | same | `ids*`, `aliases*` → 204; subtraction per row, unknown ids skipped |
| `admin/emoji/set-aliases-bulk` (`:5300`) | same | `ids*`, `aliases*` → 204; replace per row |
| `admin/emoji/set-category-bulk` (`:5318`) | same | `ids*`, `category?` → 204; absent AND explicit null both reset to null (upstream `ps.category ?? null` parity) |
| `admin/emoji/set-license-bulk` (`:5338`) | same | `ids*`, `license?` → 204; same null-reset rule |
| `admin/emoji/delete-bulk` (`:5358`) | same | `ids*` → 204; per-row `deleteCustomEmoji` log row |
| `admin/emoji/copy` (`:5372`) | same | `{host*, name*}` → `EmojiDetailed`; missing remote row `NO_SUCH_EMOJI`, duplicate local name `DUPLICATE_NAME` |
| `admin/emoji/list-remote` (`:5390`) | credential + `read:admin:emoji` + moderator | `query?`, `host?` (null/absent = all remote), `limit` 1–100 default 10, `sinceId`/`untilId` accepted-but-ignored, `sinceDate`/`untilDate` → `EmojiDetailed[]`, newest-first by `first_seen_ms` |
| `admin/emoji/import-zip` (`:5420`) | same as copy | `fileId*` (must exist + belong to caller, non-empty, ≤32 MiB) → 204; queues `ImportCustomEmojis` |
| `export-custom-emojis` (`:6005`) | any authenticated caller + `write:account` (self-service, `i/export-*` parity — NOT moderator-gated) | `{}` → 204; queues `ExportCustomEmojis`; 1/hr bucket (`response.rs`, upstream `1hour/1` parity) |

### 40.2 Streaming broadcasts (closes the §28 "no broadcast bus" DIVERGENCE)

`create/update/delete` + all six bulk ops enqueue `StreamingEvent::EmojiBroadcast`
(`src/application/events.rs`, op `Added/Updated/Deleted`) in the same store
transaction as the state change; the worker packs snapshots to `EmojiDetailed`
(`src/jobs/worker.rs:handle_emoji_broadcast`, via `push_broadcast`) and the
server sends `{type: emojiAdded|Updated|Deleted, body: {emoji}|{emojis}}` to
EVERY connection with no channel filter (`src/streaming/server.rs`,
Misskey `Connection.onBroadcastMessage` parity). Delete snapshots the row
before the delete; update/add pack post-commit rows, skipping rows deleted
between commit and run.

### 40.3 Reaction gate (closes handoff Gap 4 as implemented, not documented-out)

`AppServices::react` resolves local `:name:` / `:name@.:` shapes through
`resolve_custom_emoji_reaction` (`src/application/commands/notes.rs`):
unknown names, role-gated rows (no role subsystem — permission unprovable),
`localOnly` rows on remote notes, and sensitive rows on `nonSensitive*`
notes all fall back to ❤, never an error (Misskey `ReactionService.create`
parity). `:name@other-host:` shapes never originate locally and stay
verbatim.

### 40.4 Import/export jobs (new `JobKind::ExportCustomEmojis|ImportCustomEmojis`)

Worker entry `handle_custom_emoji_transfer`
(`src/application/commands/emoji_transfer.rs`): export zips ALL local
emojis in memory (`meta.json` `{metaVersion:2, host, exportedAt, emojis[]}`
+ per-emoji files, stored compression), stores the zip in the caller's
drive, fans out `exportCompleted` with `exportedEntity: "customEmoji"`
verbatim. Import validates the archive (32 MiB total, 500 entries, 32 MiB
per-entry checked BEFORE extraction, 64 MiB cumulative, `enclosed_name()`
traversal guard, 16 MiB `meta.json` cap, upstream fileName/emoji-name
regexes, `downloaded`-only, `image/*` sniff) and applies rows
delete-then-add through `create_emoji`.

### 40.5 DIVERGENCEs (decided, supersede §28)

- §28 "bulk ops, import-zip, copy, list-remote stay out (guide §8)" and
  "`export-custom-emojis` has no dedicated endpoint in this pin and stays
  out" are SUPERSEDED: all ten are implemented above. (The parity-plan §13
  "no dedicated endpoint" note was factually wrong against the pin —
  `export-custom-emojis.ts` exists in Misskey `2026.10.1-alpha.0`.)
- `admin/emoji/copy` takes `{host, name}`, not upstream `{emojiId}`
  (pinetree remote rows have no global ids); the copy carries no
  category/aliases/license/flags (remote rows don't store them) and a new
  `CANNOT_COPY_EMOJI` 400 covers unusable remotes.
- Bulk alias/category/license touches write per-row `UpdateCustomEmoji`
  moderation-log rows though upstream logs nothing (the port requires one
  log per `update_emoji` call to keep the atomic state+log+job commit).
- Remote `EmojiDetailed.id` is the composite `{host}:{shortcode}` (remote
  rows have no global ids); no `category` key; `sinceId`/`untilId` on
  `list-remote` are accepted but ignored (sort is by `first_seen_ms`).
- Export zip is built in memory and content-deduped (`force=false`, unlike
  upstream `force:true`); import rows attribute the importing user as
  moderator (upstream adds with none); `meta.json` capped at 16 MiB
  (upstream 64 MiB).

## §41 — `notes/like` (Sharkey/Catodon parity, 2026-10-20)

Status: ADDENDUM only. Closes the TODOS "Like-vs-favorite split" item.
Sharkey `notes/like.ts` and Catodon `notes/like.ts` (both read in the
reference checkouts) are thin wrappers over `ReactionService.create` with
`override ?? serverSettings.defaultLike` — there is NO separate like table,
NO separate activity, and NO `notes/unlike` in either reference. A like
IS a reaction row; "Sharkey like = Mastodon favourite" is emergent: the
reaction fan-out already sends bare `Like` to Mastodon-class hosts
(`host_caps.rs`) and inbound bare `Like` already stores ❤ (parity plan §6).

| Endpoint | Auth | Params → result |
|---|---|---|
| `notes/like` (`mod.rs`, before `"notes/reactions/create"`) | credential + `write:reactions` | `noteId*`, `override?` (empty string rejected like `reactions/create`) → 204. Resolution: `override ??` reactor's `defaultReaction ??` instance `default_like` config key `??` ❤ (`AppServices::resolve_like_emoji`, shared with the heart-click path). Sharkey toggle: re-press with the stored reaction unlikes; a different emoji replaces via the upsert path (`AppServices::like_note`; never surfaces `ALREADY_REACTED`). Federates as a normal `Like`; removed with `notes/reactions/delete`. 2 calls/sec bucket (`response.rs`, both references' `1000ms/2` parity). |

`default_like` config key: optional JSON string, 1..=260 chars (same rule as
`defaultReaction`; custom `:name:` spellings allowed and gated at use time),
`--default-like=` CLI override, covered by `Config::default_like_bounds` +
`file_round_trip`. Absent (default) = ❤. Documented here; there is no
separate config reference doc.

DIVERGENCEs (decided): per-user `defaultReaction` takes precedence over the
instance `default_like` (neither reference has the per-user setting, so no
upstream order exists to mirror); unknown/custom-gated `override` values fall
back to ❤ through the Gap-4 reaction gate instead of erroring; the toggle
catch in Sharkey's `like.ts` targets an error id its own `create` no longer
throws — pinetree implements the toggle the endpoint advertises (same →
unlike, different → replace, matching Catodon's documented replace rule).

## §42 — Web `signin-flow` + CORS (handoff `HANDOFF_auth-signin.md`, 2026-10-21)

Status: ADDENDUM only. Closes the handoff: Kissme (`split-project`)
password login works with no client workaround. Code: `signin_flow` /
`do_signin_flow` / `preflight` / `cors_middleware` (`mod.rs`), four new
`ApiError` constructors (`errors.rs`).

### 42.1 Endpoint inventory (Misskey `2026.10.1-alpha.0` reference)

`ApiServerService` mounts the whole API plugin at `/api`, so the web
routes below are served under `/api/*` upstream (same as pinetree's
`/api/{*endpoint}` wildcard + dedicated aliases):

| Route (upstream) | pinetree | Notes |
|---|---|---|
| `POST /api/signin-flow` (`SigninApiService.signin`) | ✅ `signin-flow` dispatcher arm + `POST /signin-flow`, `POST /api/signin-flow` aliases | Full state machine below; error ids verbatim |
| `POST /api/signin-with-passkey` (`SigninWithPasskeyApiService`) | ⚠️ stub → `400 INVALID_PARAM` ("passkey sign-in is not supported") | Pinetree stores no WebAuthn credentials; only hit when the user clicks passkey |
| `POST /api/signup`, `POST /api/signup-pending` | ✅ `signup` / ❌ `signup-pending` (no approval flow; stays 404) | Unchanged by this handoff |
| `POST /api/signin` (legacy) | ✅ unchanged (pinetree compat extension — upstream `develop` has no `endpoints/signin.ts` and no `/signin` web route; `endpoint-list.ts` + `ApiServerService.ts` verified 2026-10-21) | "Add, don't alter" — byte-identical behavior kept |
| `auth/session/generate|show|userkey`, `auth/accept` | ✅ (pre-existing) | DIVERGENCE kept: any `appSecret` accepted at generate, verified at userkey (`NO_SUCH_APP` has nothing to check against — no app registry); approval window 30 min (upstream: none) |
| `miauth/gen-token`, `GET/POST /miauth/:session`, `/miauth/:session/check` (+ `/api` alias) | ✅ (pre-existing) | Unchanged |
| `app/create`, `app/show` | ❌ 404 (no app registry) | Open; legacy `auth/*` flow does not need it |
| `/oauth/*` (`OAuth2ProviderService`, prefix `/oauth`: `GET /authorize`, `POST /decision`, `POST /` token exchange `authorization_code` + PKCE, `ALL /*` → 404 `UNKNOWN_OAUTH_ENDPOINT`) | ❌ no `/oauth` routes | Open gap, documented only — out of this handoff's DoD (no reference client exercises it) |

### 42.2 The `signin-flow` state machine (upstream `SigninApiService.ts`)

1. `{ username }` (no `password`) → `200 { finished: false, next }` where
   `next = 'captcha'` (pinetree accounts never have 2FA/security keys, so
   upstream's `'password'`-for-2FA branch is unreachable; both hints render
   the same password page).
2. `{ username, password, [captcha responses…] }` → `200 { finished: true, id, i }`;
   `i` is a freshly minted first-party app token, identified by `POST /api/i`
   exactly like the legacy `signin` token (DIVERGENCE: upstream
   `SigninService.signin` returns the account's stored `user.token`, so
   repeated logins yield the same `i`; pinetree mints a new token per login.
   Both resolve via the same `authenticate()` path, so no client observes a
   difference except token rotation).
3. Unknown username → `404` id `6cc579cc-885d-43d8-95c2-b8c7fc963280` (no
   history row); suspended account → `403` id
   `e03a5f46-d309-4865-9b69-56282d94e1eb` **before** the password check
   (upstream order — the legacy arm checks after, kept as-is); wrong
   password → `403` id `932c904e-9460-45b7-9ce6-7ed33be7eb2c` + failure
   history row; throttled → `429` code `TOO_MANY_AUTHENTICATION_FAILURES`
   id `22d05606-fbcf-421a-a2db-b32610dcfd1b`; missing/non-string `username`,
   non-string `password`, or non-string non-null `token` → plain empty 400
   (an empty-string `username` is a string, so upstream looks it up and
   answers 404 — pinetree matches since 2026-10-21).
4. `token` (totp) and `credential` (passkey) are accepted and ignored —
   upstream's `!twoFactorEnabled` branch does exactly this, and it is the
   only reachable branch here. Captcha verification is skipped the same way
   upstream skips it with no provider configured. Success mints the token,
   appends the success history row, and sets **no** cookie (upstream
   `SigninService.signin` sets none). Only failures are throttle-counted, so
   success is never blocked by earlier successes.

### 42.3 Token semantics (unchanged)

`signin` → `{ id, token, i, user: MeDetailed }` + session cookie;
`signin-flow` → `{ finished, id?, i? }` (no `user`, no cookie — the client
fetches it via `i`); both `i` values are first-party tokens resolved by the
same `authenticate()` path (`Authorization: Bearer` → body `i` → cookie).
MiAuth mints non-first-party scoped tokens; legacy `auth/*` mints
unscoped tokens on accept.

### 42.4 CORS (upstream `@fastify/cors { origin: '*' }` parity)

Every response carries `Access-Control-Allow-Origin: *` (`cors_middleware`);
`OPTIONS` on `/api/{*endpoint}`, `/signin`, `/api/signin`, `/signup`,
`/api/signup`, `/signin-flow`, `/api/signin-flow` answers `204` with
`Allow-Methods: GET, POST, OPTIONS` and
`Allow-Headers: Content-Type, Authorization` (`preflight`). Upstream's
per-route `Allow-Origin: <config.url>` + `Allow-Credentials` on the signin
routes is not copied — the frontend sends `credentials: 'omit'`, so `*`
without credentials is the working subset.

### 42.5 Contract tests

`mod.rs` tests: `signin_flow_password_login_roundtrip` (hint → mint → `i`
resolves), `_unknown_user_is_404_no_such_user`,
`_empty_username_is_404_no_such_user` (empty string looks up → 404, upstream
parity),
`_wrong_password_is_403_incorrect_password`,
`_missing_or_misshaped_fields_are_empty_400` (also pins the empty body),
`_suspended_is_403_before_password_check`,
`_ignores_totp_token_without_2fa`,
`_repeated_failures_throttle_with_upstream_id` (progressive-delay 429 then
cap 429, both with the upstream id; window expiry restores login),
`signin_with_passkey_is_400_unsupported`,
`preflight_answers_204_with_cors_headers`.
