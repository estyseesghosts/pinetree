# Client API

Status: living page, 2026-10-21. Sources: `docs/endpoint-manifest.md` §§1-43 (+§39 code-verified addendum), `docs/phase-3-profile.md`, `src/api/misskey/endpoints/mod.rs`, `src/streaming/server.rs`, `src/api/misskey/{auth,errors,packers}.rs`. Contracts win on disagreement.

Pinetree speaks Misskey's REST + streaming wire for the features it supports. Transport: `POST /api/{endpoint}` JSON `{i, ...}` (`i` = token; also `Bearer`, also `pinetree_session` cookie), `GET /streaming?i=`, `GET /files/{id}`, `GET /proxy?url=`, `POST /api/drive/files/create` (multipart/JSON). CORS `*`, `OPTIONS → 204`. Unknown endpoint ⇒ 404 `UNKNOWN_API_ENDPOINT`; unknown field ⇒ 400 `UNSUPPORTED_FIELD` naming `info.param`; void ⇒ 204; 1 MiB JSON ⇒ 413; throttle ⇒ 429 + `Retry-After`. Pagination `sinceId/untilId/sinceDate/untilDate`, `limit 1-100 d10`, `offset` only where upstream has it.

## 1. How much of Misskey we implement

Core (all present, real logic): `meta, miauth/gen-token, auth/session/generate|show|userkey, auth/accept, signup(+admin/accounts/create now admin-only), signin, signin-flow, i, i/update(39+rejectQuotes+defaultReaction), users/show|search|search-by-username-and-host|relation|notes|followers|following|reactions|featured-notes|recommendation|report-abuse, username/available, following/*|requests/*, blocking/*, mute/*, renote-mute/*, notes/create|show|delete|edit|timeline|local|hybrid|global|user-list|replies|renotes|children|conversation|unrenote|search|search-by-tag|mentions|state|featured|reactions/*|polls/vote|drafts/*|schedule/*(Sharkey shim)|versions|translate(UNAVAILABLE)|recommendations|thread-muting/*, i/pin|unpin|favorites|notifications|grouped|mark-all|flush|test-notification|revoke-token|change-password|regenerate-token|apps|authorized-apps|signin-history|registry/*|move|update-email|delete-account|export-*|import-*(+import-clips pinetree-only)|export-custom-emojis, drive/files*|stream|folders/*|cache/stats, ap/show|get, fetch-external-resources|rss, url-preview, get-online-users-count, pinned-users→[], server-info|stats+admin/*, emojis|emoji, admin/emoji/*(full+bulk+copy+list-remote+import-zip)|v2/list, antennas/*, users/lists/*(+create-from-public), clips/*(+follow|unfollow|followers), pages/*(+featured|like), gallery/posts/*, flash/*, hashtags/*, announcements|show|admin/*, promo/read, admin/ad/*|relays/*|federation/*|federation/instances/*, admin/abuse*|report-abuse, admin/show-user(s)|moderation-logs|suspend|unsuspend|reject-quotes|invite/*, admin/meta|update-meta(blockedHosts, federateClips/Pages, ugc_visibility), admin/proxy|update-proxy|unset-mfa|reset-password|get-user-ips|find-by-email|roles/*, admin/queue/stats|queue/stats(+loud-400 delay/clear), admin/send-email, abuse-recipient(dash-spelling), system-webhook/*(no admin/ prefix), webhooks/*(no i/ prefix), i/2fa/*(8), my/apps, app/create|show, sw/*, ping, retention(get/set), reset-db(loud 400), endpoint|endpoints`.

Streaming ships: `main, home/local/hybrid/globalTimeline, antenna, userList, hashtag, roleTimeline(silent), drive, serverStats/queueStats(snapshot), admin, clip`. Limits 32 ch/conn, 1536 captures LRU, 2-min reap + 60 s ping. `pollEnded`-as-`noteUpdated` compat extra; `reacted` includes `emoji{name,url}|null`.

Missing (404 or `UNSUPPORTED_CHANNEL`): `channels/*` (16), `chat/*` (~25), `reversi/*` (7)+bubble-game, `roles/*` public, `charts/*` (12), `federation/followers|following|users|stats|update-remote-user`, `admin/queue/*` BullMQ full, `admin/drive/clean*`, `admin/captcha/*`, `admin/avatar-decorations/*`, `admin/promo/create`, `get-avatar-decorations`, `drive/files/attached-chat-messages`, `users` root, `achievements`, `flashs`, `gallery/posts|likes`, `page-likes`, `claim-achievement`, `email-address/available`, `test`, `page-push`, `notifications/create`, `signup-pending`, `oauth/*`, `app/*` beyond create/show. Streaming `channel|chatUser|chatRoom|reversi|reversiGame|bubbleTimeline` unsupported; `roleTimeline` silent; stats snapshot-only (no 1 s tick).

## 2. Where we differ from vanilla Misskey (intentional)

Auth/session: cookie credential; `secure` without secure-mode; gen-token hardening (session+permission, 30-min, unsafe-scheme block); `auth/session/generate` accepts any `appSecret` (verified at `userkey`); signin order (legacy pw-before-suspension for existence-hiding, flow suspension-before-pw for upstream parity); TOTP accepted-ignored where no 2FA; signin-flow mints new token per login.

Notes: `reaction` optional → defaultReaction else ❤; `fileIds??mediaIds??[]`; `channelId→UNSUPPORTED_FIELD`; `home` no `withReplies`; `users/notes withReplies+withFiles→BOTH_...`; `notes/reactions` sinceId-only newest-first; featured simplified 3d/7d scorer 3600 s cache; edit/versions + recommendations simplified; `notes/like` toggle extension; `schedule/*` `expiredAfter→expiresAt` shim.

Visibility/mute/block (`visibility-policy.md`): single `is_visible` + `FeedFilter` on every read + streaming/capture gates, fail-closed pending; `specified/followers` matrix; `local_only` orthogonal; suspended/host-blocked hide even anon; muted/blocked/renote-mute/word-mute hard-drop + soft `isSoftMuted` server-enforced (upstream frontend-only); `mutedInstances` suffix-match viewer-scoped; instance blocklist inbound/read/outbound; private follow-lists + mod bypass; notifications gated at Notify choke incl `list(userListId)`; scheduler/export notifier=self.

Counts/packs: `followersCount` null-masking; `publicReactions isLocal?profile:false`; `driveUsage*` real (upstream 0); `fs` zeros; `pinned-users []`; `fetch-rss` subset; `translate UNAVAILABLE`; `drive/files folderId` + short pages; `proxy` no thumb/transcode/SVG/dummy; `retention` readable; `my/apps` per-grant; `app/*` registry; `webhookId` name; `TOO_MANY_WEBHOOKS`; dash-spelling + prefix strips; `admin/emoji/copy{host,name}` remote id `{host}:{shortcode}`; `deleteAccount` tombstone-no-inline-delete; suspend drops sockets; `Update(Note)` fetch-and-apply; `Undo(Accept)` by-ID; `EmojiReact` 3-way by software; `Question type=="Note"` tolerant; relay refused; stamp exact-host + neg-cache.

Streaming: `pollEnded`-as-`noteUpdated` compat extra, `reacted emoji{name,url}|null`, userList filters honored, bad hashtag `q` silent-refuse, role/drive/stats/admin/clip snapshot/silent semantics.

## 3. Sharkey features (subheader)

Pinetree includes a Sharkey-flavored subset, not full Sharkey parity (`docs/sharkey-parity.md`, `docs/handoff-sharkey-scheduling-shim.md`):

- Ships: `notes/edit|versions`, `notes/schedule/*` shim (`expiredAfter→expiresAt=Scheduled+after`, `SCHEDULE_NOTE_MAX`), `notes/like` toggle (Sharkey/Catodon parity extension), simplified recommendation scorers, `Update(Note)` string-URI fetch-and-apply (Sharkey parity), `drive` flat + folders (see differences — folders now ship per §43.1, matrix row is stale).
- Does not ship: Sharkey-only timelines/filters, version-specific admin screens, Sharkey `chat`/`channels` extras beyond Misskey SKIP. Claims in `sharkey-parity.md` need re-check against checkouts; Mastodon/Glitch fixtures unpinned.

If your client targets Sharkey-only fields, test against pinetree's packers (`packers.rs`) — additive-only (`Note +hasPoll/poll, uri/url, channel:null, isFavorited/isSoftMuted, emojis, quote extensions on quotes only`; `UserLite/Detailed/MeDetailed 39+2`; `DriveFile flat+folderId, thumbnailUrl=self url`).

## 4. Client implementer checklist (kissme-fe proven)

Password flow works zero-workaround: `users/show{username}` → `signin-flow{username,password,...}` → hint→`{finished,id,i}` → `i`. Full token bypasses scope checks. Suspended 403 / deleted 401 / bad token 401 / noscope 401. Passkey button will 400 — hide it. 2FA token ignored — do not block. `signup-pending`/OAuth dead — hide.

Timelines/notes/lists/antennas/clips/pages/drive/emoji/admin-moderation all work (§1 table). Dead surfaces to hide/guard: channels, chat, reversi, `bubbleTimeline`, achievements, bubble-game score, avatar-decorations, `charts/*`, `admin/queue/queues|jobs`, `get-table-stats`, `captcha`, `email-address/available`, `notes/show-partial-bulk` (fallback to N×`show`), `notes/clips` (use `clips/notes`), `drive`/`drive/folders` bare (use `drive/files*`), `serverStats/queueStats` live tick (single snapshot), `roleTimeline` (silent).

Media: `drive/files/create` multipart/JSON, size cap `max_file_size` + 1 MiB overhead, MD5-per-owner dedup, `GET /files/:id` (+`/:name` 301), `GET /proxy?url=` 8 MiB SSRF-guarded, no thumbs/transcode/S3, full-body buffering.

See also: [Federated Clips](../federated-clips.md), [Federated Pages](../federated-pages.md), [Intentional Differences](Intentional-Differences.md).
