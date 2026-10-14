# Misskey endpoint + streaming manifest (initial — Phase 0)

**Status:** Frozen (Phase 0 freeze review 2026-10-14)
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
| `i/update` (supported subset) | `write:account`, 20/h | `name?` (1–50), `description?` (1–1500), `location?` (1–50), `birthday?` (YYYY-MM-DD), `avatarId?`/`bannerId?` (id), `isLocked?`/`isBot?`/`isCat?` (bool) | updated `MeDetailed` + `meUpdated` stream event |
| `users/show` | none | exactly one of `userId`, `userIds[]`, or `username` (+ optional `host`, null = local) | `UserDetailed` (or array) |
| `users/search` | none | `query*`, `offset = 0`, `limit` 1–100 (= 10), `origin` = combined, `detail` = true | `User[]` (Detailed or Lite) |
| `username/available` | none | `username*` (`^\w{1,20}$`) | `{ available }` (free + untombstoned + unpreserved) |

`i/update` fields outside the subset (profile `fields[]`, decorations, mute lists, notification config, `alsoKnownAs`, …) → 400 `UNSUPPORTED_FIELD` naming the field (§8). This resolves domain-types open decision #3: extended profile storage arrives in Phase 3; the subset above is the frozen v0 surface. `description`/`location`/`birthday` join the Account type at implementation (bounds above).

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
| `poll` | rejected (`VALIDATION`, polls deferred per proposal §7) |

Errors (stable idents): `NO_SUCH_RENOTE_TARGET`, `NO_SUCH_REPLY_TARGET`, `CANNOT_RENOTE`, `YOU_HAVE_BEEN_BLOCKED`, `NO_SUCH_FILE`, `CONTAINS_PROHIBITED_WORDS`, plus `UNSUPPORTED_FIELD`/`VALIDATION`.

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `notes/show` | none | `noteId*` | `Note` (detail pack); invisible → `NO_SUCH_NOTE`-class error, never a null body |
| `notes/delete` | `write:notes`, 300/h | `noteId*` | void; non-owner/non-moderator → `ACCESS_DENIED` |
| `following/create` | `write:following`, 100/h | `userId*`, `withReplies?` | `UserLite` followee; `NO_SUCH_USER`, `FOLLOWEE_IS_YOURSELF`, `ALREADY_FOLLOWING`, `BLOCKING`/`BLOCKED` |
| `following/delete` | `write:following`, 100/h | `userId*` | `UserLite` followee; `NO_SUCH_USER`, `FOLLOWEE_IS_YOURSELF`, `NOT_FOLLOWING` |
| `notes/reactions/create` | `write:reactions` | `noteId*`, `reaction*` (string ≤260) | `204 No Content`; `NO_SUCH_NOTE`, `ALREADY_REACTED`, `YOU_HAVE_BEEN_BLOCKED`, `CANNOT_REACT_TO_RENOTE` (M `notes/reactions/create.ts:19-74`; `reactionAcceptance` mismatch coerces to fallback `❤️` per `ReactionService.ts:127,149`) |
| `notes/reactions/delete` | `write:reactions`, 60/h (min 3 s interval) | `noteId*` | `204 No Content`; `NO_SUCH_NOTE`, `NOT_REACTED` (M `notes/reactions/delete.ts:18-46`) |
| `drive/files/create` | `write:drive`, 120/h | multipart/form-data (`file*`, `name?`, `comment?`, `isSensitive = false`, `force = false`, `folderId?`, `i?`) or JSON | `DriveFile` (packed `self: true`); `INVALID_FILE_NAME`, `MAX_FILE_SIZE_EXCEEDED` (413), `UNALLOWED_FILE_TYPE`. Public bytes served at `GET /files/:id` (and `/files/:id/:name` → 301 canonical, M `FileServerService.ts:93-99`, `DriveFileEntityService.ts:126`) |

Timelines (all → `Note[]`, cursor pagination via `sinceId`/`untilId` + `sinceDate`/`untilDate`, `limit` 1–100 default 10):

| Endpoint | Auth | Params |
|---|---|---|
| `notes/timeline` (home) | `read:account` | `withFiles = false`, `withRenotes = true`, `includeMyRenotes/includeRenotedMyNotes/includeLocalRenotes = true`, `allowPartial = false` |
| `notes/local-timeline` | none | `withFiles = false`, `withRenotes = true`, `withReplies = false` |
| `notes/hybrid-timeline` | `read:account` | union of the above |

## 5. Response shapes (fixed field sets)

- `Note`: all domain-types §2 fields + Misskey 2026.10 required keys (`id`, `createdAt`, `userId`, `user: UserLite`, `text`, `cw`, `visibility`, `localOnly`, `reactionAcceptance`, `renoteCount`, `repliesCount`, `reactionCount`, `reactions`, `reactionEmojis`, `fileIds`, `files`, `replyId`, `renoteId`, `reply`, `renote`, `mentions`, `visibleUserIds`, `tags`, `emojis`, `myReaction`), rendered per visibility policy (tombstones/deleted bodies never leak); `user` packed inline (`UserLite`), `files` from `file_ids`, `reply`/`renote` one level unless `detail`.
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

- Note capture: `subNote { id }` / `unsubNote` (aliases `s`/`sr`/`un`); events `reacted/unreacted/deleted/updated` (+ `pollVoted` reserved, polls deferred); capture applies the light visibility gate (specified/followers checks, M `Connection.ts:231-254`).
- Explicitly unsupported in Phase 0 (→ `UNSUPPORTED_CHANNEL` on connect): `userList`, `hashtag`, `antenna`, `roleTimeline`, `channel`, `drive`, `chat*`, `reversi*`, `clip`, `admin`, `serverStats`, `queueStats`. Added in Phase 3 per client-test demand.

## 7. Rate limits (initial values, instance-configurable)

`i/update` 20/h · `notes/*` 300/h (delete: min 1 s interval) · `following/*` 100/h. Exceeding → rate-limit error with retry info, never silent drops.

## 8. Unsupported behavior (documented responses)

- Unlisted endpoint → 404. Unlisted channel → `UNSUPPORTED_CHANNEL`. Unlisted param on a listed endpoint → 400 `UNSUPPORTED_FIELD` naming it. Polls, quotes (Phase 3+), channels, OAuth, Mastodon REST API → the corresponding documented error, never partial behavior.
