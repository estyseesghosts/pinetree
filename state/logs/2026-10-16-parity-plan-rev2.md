# 2026-10-16 Misskey parity plan rev.2

What changed (`docs/misskey-parity-plan.md`, still Draft, now marked rev.2):
- §0A: required-by fixed to §§1–2, 4, 11–12; thread-muting declared out of scope.
- §0B: expanded from column list to behaviors: verifiedLinks reset + verifyLink loop, alsoKnownAs remote resolution, mute-word wordMuteLimit + RE2 validation + enableWordMute, avatarDecorations role gating, alwaysMarkNsfw/canUpdateBioMedia guards, unlock→acceptAllFollowRequests; drive/files/create noted as avatar/banner test prerequisite; rejectQuotes/defaultReaction columns linked to §§3/6.
- §3: added DIVERGENCE self-service `i/update {rejectQuotes}` (upstream Sharkey is admin-only: only writers are `admin/reject-quotes.ts` and `admin/federation/update-instance.ts`); instance-level rejectQuotes out of scope.
- §4: kept has_poll fix; split inbound `Update` ownership with §8 (poll-vote/Question branch here, Update(Note) edit branch in §8).
- §5: fixed error code to `SCHEDULED_AT_MUST_BE_IN_FUTURE`; added `drafts/count` to ship list.
- §6 (decided per user): two-part — Misskey wire compat (bare-U+2764 FALLBACK, `src/domain/reaction.rs:6-43` normalization) + DIVERGENCE per-user `profile.defaultReaction` (no upstream hits in Misskey/Sharkey backend+frontend): `i/update` param, MeDetailed pack, heart-click fallback `defaultReaction ?? FALLBACK`, EmojiReaction to emoji-aware servers vs plain Like to Mastodon/Lemmy, inbound bare Like → FALLBACK.
- §7: featured ranking defined (compatible-simplified: recent non-sensitive public notes scored by reactions+renotes, newest tie-break) for both `notes/featured` and `users/featured-notes`, 3600s cache.
- §8 (decided per user): commit to Sharkey `notes/edit.ts` + `NoteEditService.ts` shape — endpoint/errors, author-only, replyId immutable, visibility/localOnly forced to original, poll reset, trim/truncate, no-change fast path, `note_edits` history row, note-stream `updated` + `edited` notifications + AP Update(Note) deliver; delete-and-redraft stays client-only.
- §9: fixed stale path to `src/application/queries.rs:753-771`; sweep test covers both directions.
- §11: split creation-time receiveConfig vs read-time includeTypes/excludeTypes; whitelist-only = `list(userListId)`, full preset map, no second list.
- §12 (decided per user): home DIVERGENCE keeps followed-replies (current `src/application/queries.rs:149-190` behavior + §0A filters); local/hybrid Misskey parity with self-reply exception (fix `191-302` over-filtering); word mutes client-side only (upstream `frontend/src/utility/check-word-mute.ts`), no server CW injection; mutedInstances + renote-mutes applied on reads.
- §13: corrected to actual 8-export/5-import endpoint set (`core/QueueService`), secure 1/hr, export-following params; customEmoji has no dedicated endpoint in this pin (out of scope, enums only); profile fields move via §0B.
- Build order: §0B feeds §§3/6, §4→§8 ordering with shared Update dispatcher, §6 split across §4/§0B.

Verified:
- `git diff --check` clean (no whitespace errors).
- `git status --short`: only pre-existing worktree state (`M docs/README.md`, untracked plan + audit files); no `src/`/`tests/` touched by this revision.
- Grep: no bare `queries.rs:` refs remain (all `src/application/queries.rs:`); `SCHEDULED_AT_MUST_BE_IN_FUTURE` present; 6 DIVERGENCE markers (all decided except the documented home-timeline extension, which is intentional).
- Upstream evidence re-read live: Sharkey `notes/edit.ts` (484 lines, meta/errors/paramDef/handler), `NoteEditService.ts edit()` (author check, reply/visibility forcing, quote-permission check, history insert, postNoteEdited deliver), `checkQuotePermissions` downgrade, `ReactionService FALLBACK`, `src/activitypub/objects.rs:884-900` Like+`_misskey_reaction`, `src/application/queries.rs:149-302,753-771`, `i/update.ts:141-237` (39 params).

Open questions:
- Featured trailing-window length (e.g. 7d vs 30d) left to implementation slice.
- `defaultReaction` validation of custom-emoji permissions (reuse reaction-create checks?) left to slice.
- `notes/edit` channel-edit edge cases (reply/channel scope forcing per NoteEditService) to be carried into slice verbatim.
