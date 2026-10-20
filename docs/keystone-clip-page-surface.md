> **Status (2026-10-20 freshness pass):** Phase-2 clip and page surface is landed: `clips/follow|unfollow|followers`, `isFollowing`, `pageVisibility`/`visibleUserIds`, gated pages (`NO_SUCH_PAGE`), and the `clipUpdated`/`pageLiked` notification types. Client-side rendering guidance is not verifiable from the backend.
> Tags: `[LANDED — evidence]` verified in code at HEAD; `[OPEN]` not in code; `[SUPERSEDED by …]`; `[DECISION — still binding]`; `[PLAN HISTORY]`. Original text is unchanged; tag blocks are additions. Short paths are under `src/`; `mod.rs` = `src/api/misskey/endpoints/mod.rs`.

# Keystone surface: federated clips + pages phase 2

Status: Draft (2026-10-20; tracks `docs/endpoint-manifest.md` §38).

Keystone is REST-only (no streaming), so every phase-2 signal arrives via
REST packs or the `main:notification` body (which Keystone already polls
through `i/notifications`). No new stream channel exists and none is
planned — membership events surface as notifications only (recorded
decision in `docs/federated-collections.md` §3).

## New endpoints (all 200/204; pinetree-first, DIVERGENCE)

> **[LANDED — mod.rs:3877 (clips/follow), :3890 (clips/unfollow), :3906-3916 (clips/followers, owner-only, ACCESS_DENIED via errors.rs:954-957)]**


- `clips/follow {clipId!}` → 204 (idempotent; needs `write:clip-favorite`).
- `clips/unfollow {clipId!}` → 204 (idempotent; unknown clip →
  `NO_SUCH_CLIP`).
- `clips/followers {clipId!}` → 200 `UserLite[]`, owner-only (non-owner →
  403 `ACCESS_DENIED`, pinetree-fixed UUID). Anyone else reads self-status
  via `clips/show.isFollowing`.

## Changed packs / params

> **[LANDED — packers.rs:753 (isFollowing in the clip pack path); mod.rs:3959-3997 (pageVisibility/visibleUserIds on pages/create|update); pages.rs:130-156 (NO_SUCH_PAGE for gated pages)]**


- `clips/show` gains `isFollowing` (boolean, only when authed).
- `pages/create|update` gain `pageVisibility`
  (`public|home|followers|specified`, default `public`) and
  `visibleUserIds` (≤100 ids, only meaningful with `specified`).
  The pre-existing `visibility` param keeps meaning the federation tier.
- `pages/show|like|featured` hide gated pages as `NO_SUCH_PAGE`
  (`pages/featured` lists public pages only).

## Notification wire types (new, pinetree-only)

> **[LANDED — mod.rs:1130-1131 (clipUpdated, pageLiked accepted in notificationRecieveConfig); domain/notification.rs:41,44; emitted at commands/clips.rs:124 and commands/pages.rs:304]**


- `clipUpdated`: `clipId` + packed `clip` (metadata edited, clip deleted
  or restricted; membership edits notify the note's author instead and
  never spam followers).
- `pageLiked`: `pageId` + packed `page` (a remote `Like` landed).
- Both work in `notificationRecieveConfig` values and in
  `i/notifications` / `i/notifications-grouped` `includeTypes` /
  `excludeTypes`.

## Rendering notes

> **[DECISION — still binding]** Client-side guidance. The Tombstone-drop behavior is not verifiable from the backend.


- Followers lists never advertise to non-owners; gate clip-member UI on
  `isFollowing`, not on `followers`.
- Tombstoned clip URIs are gone: drop any cached copy on `clipUpdated`
  naming a deleted clip (the AP surface serves `Tombstone`, not the
  collection).
