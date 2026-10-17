# Misskey endpoint + streaming manifest (initial — Phase 0)

**Status:** Frozen (Phase 0 freeze review 2026-10-14), extended by
superseding Phase 3 decision below (2026-10-16)
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
| `fileIds` / `mediaIds` | 1–16 unique ids |
| `localOnly`, `noExtractMentions`, `noExtractHashtags`, `noExtractEmojis` | bool, default false |
| `reactionAcceptance` | null + 4 values (domain types §2), default null |
| `poll` | §4 polls (`docs/misskey-parity-plan.md` §4): `{ choices[2..10] unique 1..50 chars, multiple = false, expiresAt? \| expiredAfter? (ms, `expiredAfter` wins) }`; past expiry → `CANNOT_CREATE_ALREADY_EXPIRED_POLL`; a poll stands in for `text` (see §15) |

Errors (stable idents): `NO_SUCH_RENOTE_TARGET`, `NO_SUCH_REPLY_TARGET`, `CANNOT_RENOTE`, `YOU_HAVE_BEEN_BLOCKED`, `NO_SUCH_FILE`, `CONTAINS_PROHIBITED_WORDS`, `CANNOT_CREATE_ALREADY_EXPIRED_POLL`, plus `UNSUPPORTED_FIELD`/`VALIDATION`.

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `notes/show` | none | `noteId*` | `Note` (detail pack); invisible → `NO_SUCH_NOTE`-class error, never a null body |
| `notes/delete` | `write:notes`, 300/h | `noteId*` | void; non-owner/non-moderator → `ACCESS_DENIED` |
| `following/create` | `write:following`, 100/h | `userId*`, `withReplies?` | `UserLite` followee; `NO_SUCH_USER`, `FOLLOWEE_IS_YOURSELF`, `ALREADY_FOLLOWING`, `BLOCKING`/`BLOCKED` |
| `following/delete` | `write:following`, 100/h | `userId*` | `UserLite` followee; `NO_SUCH_USER`, `FOLLOWEE_IS_YOURSELF`, `NOT_FOLLOWING` |
| `notes/reactions/create` | `write:reactions` | `noteId*`, `reaction?` (string ≤260; absent/null → reactor's `defaultReaction ?? ❤` per the superseding note below; DIVERGENCE — upstream `notes/reactions/create.ts:19-74` requires `reaction`) | `204 No Content`; `NO_SUCH_NOTE`, `ALREADY_REACTED`, `YOU_HAVE_BEEN_BLOCKED`, `CANNOT_REACT_TO_RENOTE` (M `notes/reactions/create.ts:19-74`; `likeOnly` coerces to the bare-heart fallback `❤` (U+2764, no VS16) and every other reaction is normalized (legacies map, FE0F strip unless ZWJ) per `ReactionService.ts:34-48,125-162,383-401` — see §15) |
| `notes/reactions/delete` | `write:reactions`, 60/h (min 3 s interval) | `noteId*` | `204 No Content`; `NO_SUCH_NOTE`, `NOT_REACTED` (M `notes/reactions/delete.ts:18-46`) |
| `notes/reactions` (2026-10-17) | none (anonymous ok) | `noteId*`, `type?` (`:name@.:` = local `:name:`), `limit` 1..100 (default 10), `sinceId`/`untilId`/`sinceDate`/`untilDate` | `NoteReaction[]` `{id, createdAt, user: UserLite, type}`, newest first; `NO_SUCH_NOTE` when missing or invisible/hidden to the viewer (M `notes/reactions.ts`). Scans the note's reactions then pages in the application layer (no per-note reaction index yet). |
| `notes/polls/vote` (§15) | credential `write:votes` | `noteId*`, `choice*` (integer) | `204 No Content`; `NO_SUCH_NOTE`, `NO_POLL`, `INVALID_CHOICE`, `ALREADY_VOTED`, `ALREADY_EXPIRED`, `YOU_HAVE_BEEN_BLOCKED` (all exact upstream code/message/id) |
| `drive/files/create` | `write:drive`, 120/h | multipart/form-data (`file*`, `name?`, `comment?`, `isSensitive = false`, `force = false`, `folderId?`, `i?`) or JSON | `DriveFile` (packed `self: true`); `INVALID_FILE_NAME`, `MAX_FILE_SIZE_EXCEEDED` (413), `UNALLOWED_FILE_TYPE`. Public bytes served at `GET /files/:id` (and `/files/:id/:name` → 301 canonical, M `FileServerService.ts:93-99`, `DriveFileEntityService.ts:126`) |

## Superseding note — default like emoji (2026-10-16)

`notes/reactions/create` leaves `reaction` optional (DIVERGENCE from upstream Misskey, where `reaction` is required): an absent/null param resolves server-side to the reactor's `defaultReaction ?? ❤` (bare U+2764, no VS16), then runs the standard reaction pipeline (legacy-name map, FE0F strip unless ZWJ, `likeOnly` coercion) per `docs/misskey-parity-plan.md` §6. Set via `i/update {defaultReaction}` (nullable, 1..=260 chars), packed on `MeDetailed`. Federation: outbound `Like` renders emoji-aware (`Like` + `_misskey_reaction`) or plain per recipient-host software (lazy NodeInfo probe, ~7-day cache, unknown/failed → emoji-aware); inbound `Like` without `_misskey_reaction` maps to `❤`.

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
- Word mutes are CLIENT-SIDE ONLY (decided Misskey parity): the backend stores `mutedWords`/`hardMutedWords`/`mutedInstances` (§0B) and ships them via `MeDetailed`, but no timeline, note read, packer, or streaming path consults the word lists (verified: no `muted_words` use outside profile store/pack; pack-level hiding covers specified/followers visibility only, matching `NoteEntityService.hideNote` scope; nested reply/renote expansion nulls invisible targets). Upstream word-mute enforcement lives in the frontend (`check-word-mute.ts` + `use-note.ts`), out of scope here.

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
| `i/import-following` | `fileId*`, `withReplies = true` | CSV rows: bare/`user@host` acct (+ row `withReplies=` wins) and the legacy 3-column (`id,username,host`) variant; unresolvable rows skip |
| `i/import-blocking` | `fileId*` | CSV rows: first column as acct; unresolvable rows skip |
| `i/import-muting` | `fileId*` | CSV rows: first column as acct, non-expiring mutes; unresolvable rows skip |
| `i/import-user-lists` | `fileId*` | CSV rows parsed + validated, zero applied (no subsystem) |
| `i/import-antennas` | `fileId*` | JSON array parsed + validated, zero applied (no subsystem); malformed JSON → `INVALID_PARAM` |

- Completion: every export stores its file in the caller's drive and emits `exportCompleted` (`exportedEntity` verbatim upstream tags `following/blocking/muting/note/userList/antenna/clip/favorite` + `fileId`). The export job's completion and the `Notify` enqueue commit in ONE transaction (`complete_with_followups`); the notification is a first-class `i/notifications` filter value and streams as a plain `notification` hint. Imports never notify.
- Import guards (`import-blocking.ts:53-86` shape on all five): foreign/missing file → `NO_SUCH_FILE`, empty → `EMPTY_FILE`, over 64KiB → `TOO_BIG_FILE`, all with the verbatim per-endpoint UUIDs (upstream mints one UUID per endpoint per code). The commented-out upstream `UNEXPECTED_FILE_TYPE` check stays unimplemented, like upstream. `i/import-antennas` has no size cap upstream and is guarded the same way here.
- Row application goes through the interactive ops (`follow`/`block`/`mute`), so §0A block gates and `ALREADY_*` dedup apply; replays converge (conflicts skip, never duplicate). One transaction per row keeps writes bounded; only storage faults retry the job.
- DIVERGENCEs (decided): `excludeInactive` accepted and ignored (no last-active tracking; a zero-notes heuristic would mislabel lurkers); locals export as bare `username` (upstream always appends `@host`) — imports accept both forms; remote users resolve from the local store only (no remote fetch — unresolvable rows skip, upstream would federate-resolve them); no-subsystem entities export empty valid files and import zero rows (client tooling still finds the endpoints); drive upload dedups on content (`force == false`, upstream `force: true`) so retried exports converge instead of orphaning duplicates; `i/import-following` `withReplies` defaults `true` when absent (upstream flows `undefined`; matches the `following/create` default); malformed antenna JSON is `INVALID_PARAM` (upstream throws 500); rate buckets are 1/hr on all 13 (upstream uses 1/day for notes/clips/favorites and 1/min for user-lists); exports larger than `max_file_size` fail loudly through the shared upload path (job quarantines after the retry budget); `exportCompleted` rows store the exporter as notifier (upstream is notifier-less; same convention as the §16 scheduling note) and only a `never` receive-config drops them.
