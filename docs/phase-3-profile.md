# Phase 3 — Declared compatibility profile

**Status:** Frozen (declared 2026-10-16; changes require a logged superseding decision)
**Phase exit (proposal §9):** every endpoint/channel in this profile passes
contract tests against the target client versions below.

## 1. Target clients

| Client | Version | Role in verification |
|---|---|---|
| Misskey web client | `2026.10.1-alpha.0` (pinned checkout `./misskey`) | streaming channels, note/user pages, drive, notifications |
| `misskey-js` | `2026.10.1-alpha.0` (same checkout) | `Stream` protocol + REST contract tests (bun harness) |
| Keystone frontend | `/root/keystone` HEAD, `VITE_KEYSTONE_BACKEND=pinetree` target | REST profile sweep (Keystone is REST-only, no streaming) |
| Sharkey web client | `2025.4.7` (checkout `./sharkey`) | interop parity via the four-server smoke; no separate contract suite |

## 2. Streaming (in scope)

`GET /streaming?i={token}` (or `Authorization: Bearer`; see §4 for the
exact handshake). Text JSON frames only.

Client→server: `connect {channel,id,params?}` / `disconnect {id}` /
`channel`+`ch` alias (per-channel messages; only `readNotification`
interpreted, others ignored) / `readNotification` /
`subNote`+`s`+`sr` aliases `{id}` / `unsubNote`+`un` alias `{id}`.
Server→client: `channel {id,type,body}` wraps channel events;
`noteUpdated {id,type,body}` is top-level (broadcast).
Channels: `main` (auth `read:account`), `homeTimeline` (auth),
`localTimeline`, `hybridTimeline` (auth), `globalTimeline`.
Timeline params: `withRenotes=true`, `withFiles=false`
(`local`/`hybrid` also `withReplies=false`).
`main` emits: `notification`, `mention`, `reply`, `renote`, `follow`,
`followed`, `unfollow`, `meUpdated`, `readAllNotifications`.
Note capture emits `reacted {reaction,userId}` / `unreacted
{reaction,userId}` / `deleted {deletedAt}` with the Misskey light
visibility gate (specified/followers checks).
Limits: 32 channels/connection, 1536 captured notes (LRU-evict oldest),
2 min idle reap with 60 s pings.
Unknown channel on connect → `{"type":"error","body":{"code":
"UNSUPPORTED_CHANNEL","channel":name,"id":id}}`, channel not added
(wire definition of manifest §8 `UNSUPPORTED_CHANNEL`).

## 3. REST endpoints (in scope)

Frozen Phase 0/1 surface (manifest §§1–5) plus:

| Endpoint | Auth | Params | Response |
|---|---|---|---|
| `users/notes` | none | `userId*`, `withReplies=false`, `withRenotes=true`, `withChannelNotes=false` (accepted, always false — no channels), `limit` 1–100 (=10), `sinceId/untilId/sinceDate/untilDate`, `allowPartial=false` (accepted, ignored), `withFiles=false` | `Note[]` newest-first, visibility-filtered for viewer |
| `notes/replies` | none | `noteId*`, cursor + `limit` 1–100 (=10) | direct replies (`reply_id == noteId`), `Note[]` |
| `notes/renotes` | none | `noteId*`, cursor + `limit` 1–100 (=10) | all renotes incl. quotes, `Note[]` |
| `notes/children` | none | `noteId*`, `limit` 1–100 (=10), cursor | replies + quote-renotes (renotes with text/files), `Note[]` |

Superseding note — quote posts (2026-10-16, `docs/quote-posts.md`): quote notes project through the same `renote`/`renoteId` shape plus Pinetree extensions `quoteAuthorizationStatus` (`pending|approved|rejected|revoked|unapproved|unknown`), `quoteTargetUri` (unresolved-but-disclosable target), and `quoteTargetUnavailable` (protected/invisible target, `renote` null, no URI leak). Extensions appear on quotes only, never on pure renotes; authorization never overrides visibility; rejected/unapproved quotes still render.
| `notes/conversation` | none | `noteId*`, `limit` 1–100 (=10), `offset=0` | ancestor chain via `replyId`, oldest-first, `Note[]` |
| `notes/unrenote` | credential `write:notes` | `noteId*` | void; deletes all caller's renotes of the note (`NO_SUCH_NOTE` when unknown) |
| `users/followers` | none | `userId` xor (`username`+`host`) `*`, cursor + `limit` 1–100 (=10) | `Following[]` (`{id,createdAt,followerId,followeeId,follower, followee?}` — follower populated; Misskey shape) |
| `users/following` | none | same as followers | `Following[]` (followee populated; Misskey shape) |
| `drive/files` | credential `read:drive` | `limit` 1–100 (=10), cursor, `folderId` (accepted, must be null — no folders), `type` (`image/*` prefix match), `sort` (`-createdAt` only; others → `INVALID_PARAM`) | `DriveFile[]` of caller, newest-first |
| `drive/files/show` | credential `read:drive` | `fileId*` | `DriveFile` (owner only → else `NO_SUCH_FILE`) |
| `drive/files/update` | credential `write:drive` | `fileId*`, `isSensitive?`, `comment?`, `name?` | updated `DriveFile` |
| `drive/files/delete` | credential `write:drive` | `fileId*` | void; removes metadata + bytes |
| `i/notifications` | `read:account` (extended) | `limit` 1–100 (=**10**, aligned to Misskey), cursor, `markAsRead=true`, `includeTypes[]`/`excludeTypes[]` | `Notification[]` newest-first |
| `notifications/mark-all-as-read` | credential `write:notifications` | none | void; marks all read + `readAllNotifications` main event |
| `i/revoke-token` | credential | `token?`/`tokenId?` (both absent → revoke caller token) | void |
| `notes/search` | credential `read:account` | `query*`, `limit` 1–100 (=10), cursor, `userId?`, `host?` (`.` = local only) | `Note[]` substring match over local notes (bounded scan; documented limit) |
| `notes/search-by-tag` | credential `read:account` | `tag*`, `limit` 1–100 (=10), cursor | `Note[]` with the tag (case-insensitive) |

`Following.createdAt` derives from the follow-record id timestamp
(`aidx` ids are time-ordered). Private follow-list visibility
(`followersVisibility`/`followingVisibility`) is NOT modeled — lists are
public (documented deviation; Misskey `FORBIDDEN` cases do not apply).

## 4. Handshake / auth rules (streaming + `drive/files/create` parity)

- WS upgrade: token from `?i=` or `Authorization: Bearer`. No token →
  anonymous (non-credential channels only). Bad token → HTTP 401 at
  upgrade. Suspended user → 403. Token without `read:account` →
  401 (Misskey parity). First-party tokens (empty permissions) pass.
- Misskey-js connects `${ws(s)://host}/streaming?i={token}&_t={ts}`;
  `_t` is ignored.

## 5. Explicitly OUT of the profile (non-goals)

- `antennas/*` (Keystone renders honest 404 states), polls +
  `notes/polls/vote` (no votable polls can exist; Keystone poll UI is
  hidden on the pinetree target), `drive/folders/*` + non-null `folderId`
  (no folder model; `drive/files/create` ignores `folderId`, documented),
  `clips`, `channels/*`, `chat*`, `pages`, Reversi, `user-list(s)`,
  `hashtag`, `roleTimeline`, `serverStats`, `queueStats`, `admin`,
  `drive` streaming channel, `i/notifications-grouped`,
  `notes/featured`, `users/featured-notes`, `notes/mentions`,
  `notes/user-list-timeline`, `notes/reactions` (list),
  `notifications/flush|test-notification`, `signin-flow`
  (Misskey-family-only; pinetree target uses `signin`), OAuth, RFC 9421
  signatures, Mastodon API.
- `notes/search` `offset`/`channelId`/`rangeStartAt|EndAt`: rejected as
  `UNSUPPORTED_FIELD` (Keystone never sends them).

## 6. Known cross-repo follow-up (NOT pinetree scope)

Keystone `routes/Profile.tsx:38-39` types `users/followers|following` as
`UserLite[]`. The profile fixes the Misskey `Following[]` shape, so
Keystone must map `x.follower!` / `x.followee!` (same as Misskey
`follow-list.vue`) in its own repo. Until then Keystone follow lists
will render follow-record rows. Logged, not fixed here.

## 7. Superseding audit fixes (2026-10-17)

Five-finder audit (`state/logs/2026-10-16-audit-fixes.md`) verified the
profile against Misskey `2026.10.1-alpha.0` and tightened these rows
(behavior changes vs §§2–4 above; rationale in each case is Misskey
client parity):

- `notes/search`, `notes/search-by-tag`: public (no credential), per
  Misskey `requireCredential: false`. Anonymous viewers see only what
  the visibility policy allows.
- `notes/search` corpus: no `host` filter means the global scope (all
  cached hosts); `host: "."` stays local-only. New params `offset`
  (applies before `limit`), `rangeStartAt`/`rangeEndAt` (ms timestamps,
  same bounds as `sinceDate`/`untilDate`). `channelId` still rejected
  (no channels exist).
- `users/notes`: `withReplies` + `withFiles` together is now
  `BOTH_WITH_REPLIES_AND_WITH_FILES` (Misskey code + id); unknown
  `userId` is `NO_SUCH_USER` (was `[]`).
- `drive/files` `sort`: all six Misskey variants honored
  (`+createdAt` newest-first through `-size`); non-null `folderId`
  still rejected (no folder model).
- `Note` gains optional `uri`/`url` (+ always-null `channel`).
  `Notification` emits `note` only on note-bearing variants and
  `reaction` only on the reaction variant.
- `i/notifications` requires `read:notifications` (was `read:account`).

Kept as documented deviations (audited, accepted): public follow lists
(no visibility fields exist to gate on and no client can set
non-public); `tokenId` revoke no-op; empty search `query` rejected;
`search-by-tag` `tag`-only (no target client sends the `query[][]`
form); read rate-limit buckets and timeline/scan indexes (Phase 5
scale work); `notes/renotes` + conversation `NO_SUCH_NOTE`/empty
semantics are Misskey-exact and unchanged.
