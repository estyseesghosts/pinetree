# Pinetree native communities — plan (2026-10-20)

## Shape: optional extension + minimal core seal
- Communities = optional extension. Core (notes, follows, timelines, DMs) runs without it. Community-tagged posts degrade to plain addressed notes on core-only servers.
- Core owns exactly one thing: `restrictedTo` seal-check. Any post with the tag is invisible by default (no timeline/search/preview/relay/direct fetch) unless the extension verifies membership proof. No extension or error = hidden (safe direction). ~50 lines, no groups/mods/UI in core. Generalizes later to followers-only, invites, DMs.
- Extension owns: community + member tables, policy service, Group federation, backfill/repair, API/UI, Lemmy shim (if kept).

## Model: native, not bolted-on (Catodon reference only)
- `community(id, actor_uri UNIQUE, webfinger_lower UNIQUE, host, software, visibility, posting_policy, listed, nsfw, languages[], counts JSONB, also_known_as[])` — stable id across rename/move.
- `community_member(community_id FK CASCADE, user_id FK CASCADE, role member|mod|admin, state active|pending|banned, banned_by, reason, expires_at, PK(community_id,user_id), IDX(user_id))` — one lookup for all checks; explicit owner (not row-order seniority).
- `note.community_id FK SET NULL` (migrate from Catodon forumId/forumScoped via dual-write then cutover).
- Single `CommunityPolicyService.canPost/canView/canModerate`; no scattered isGroup checks. Fanout `communityTimeline:{id}` + home fill on write; adoption = same path.
- Counts/catalog materialized + invalidated on write (not 5-min cache). Modlog per community. Notification prefs first-class.

## Visibility / privacy
- public | unlisted (hidden unless subscribed) | local_only_public (unfederated, anon OK) | local_only_private (unfederated + login) | private (follow -> ApprovalRequired -> Accepted, enforced reads+writes).
- Private full enforcement only extension-to-extension (reader checks + signed instance `is_follower` probe). Vs core-only: degrades to unlisted-ish (addressable, not advertised, not gate-kept). True secrecy = local-only.
- Flip to non-federating: tombstone then update. LocalOnly HTTP 404; private fetch requires follower-instance signature.

## Roles / moderation
- Create: any non-banned (or admins-only if flag). Edit: mod/admin. Delete: top-mod/owner. Remove/purge: site admin.
- Add-mod needs seniority; transfer = atomic owner update (federates). Ban/unban + optional wipe; approvals queue (approve/deny); posting_restricted_to_mods announcements mode; pins/locks/distinguish/warn; reports; shared modlog.
- Day-to-day + plumbing per prior decisions (TransFem one-clicks + DVD spam/CSAM/purge + set-root/restart-migration/roles clone+annotate/vapid/shared-access).

## Federation wire (Lemmy-compatible)
- Group actor (!name@host, inbox/followers/outbox/moderators/featured, sharedInbox, keys, languages, manuallyApprovesFollowers=private, discoverable=!unlisted).
- audience=community on every community activity. Public must contain Public in to/cc; private must not.
- Post Create(Page) + Announce(Create) + compat Announce(Page); comments Create(Note) w/ inReplyTo chain; votes bare Like/Dislike + audience; Delete (self, no summary) vs Remove (summary + withReplies); Block(+removeData), Add(target moderators|featured), Lock, Update(Group/Page), Flag/Resolve/Warn.
- Inbox accepts wrapped + raw; re-announces local mail. Outbox newest ~20 Announce(Create(Page)). Backfill: paged resumable outbox walk (not 10-cap). Lenient parsing (one-or-many, skip bad tags). Deterministic Create ids, UUID announces. RFC 9421 + legacy verify-if-present permissive-first.

## Migration (Catodon -> native)
1. community row per isGroup user (uri->actor_uri, username/host->webfinger, forum*->new cols).
2. note.community_id dual-write with forumId; batch copy.
3. members from follows of groups; bans from forum_ban.
4. Cut reads to community_id; drop forumId/forumScoped + user.forum* after verify.

## Deferred / open
- Lemmy /api/v3 shim: extension endpoint, v1 or later (TBD).
- Exact DDL + policy code: implementation pass.
- Translate composition, rec MVP scope: separate tracks.
