# Keystone surface: federated clips + pages phase 2

Status: Draft (2026-10-20; tracks `docs/endpoint-manifest.md` §38).

Keystone is REST-only (no streaming), so every phase-2 signal arrives via
REST packs or the `main:notification` body (which Keystone already polls
through `i/notifications`). No new stream channel exists and none is
planned — membership events surface as notifications only (recorded
decision in `docs/federated-collections.md` §3).

## New endpoints (all 200/204; pinetree-first, DIVERGENCE)

- `clips/follow {clipId!}` → 204 (idempotent; needs `write:clip-favorite`).
- `clips/unfollow {clipId!}` → 204 (idempotent; unknown clip →
  `NO_SUCH_CLIP`).
- `clips/followers {clipId!}` → 200 `UserLite[]`, owner-only (non-owner →
  403 `ACCESS_DENIED`, pinetree-fixed UUID). Anyone else reads self-status
  via `clips/show.isFollowing`.

## Changed packs / params

- `clips/show` gains `isFollowing` (boolean, only when authed).
- `pages/create|update` gain `pageVisibility`
  (`public|home|followers|specified`, default `public`) and
  `visibleUserIds` (≤100 ids, only meaningful with `specified`).
  The pre-existing `visibility` param keeps meaning the federation tier.
- `pages/show|like|featured` hide gated pages as `NO_SUCH_PAGE`
  (`pages/featured` lists public pages only).

## Notification wire types (new, pinetree-only)

- `clipUpdated`: `clipId` + packed `clip` (metadata edited, clip deleted
  or restricted; membership edits notify the note's author instead and
  never spam followers).
- `pageLiked`: `pageId` + packed `page` (a remote `Like` landed).
- Both work in `notificationRecieveConfig` values and in
  `i/notifications` / `i/notifications-grouped` `includeTypes` /
  `excludeTypes`.

## Rendering notes

- Followers lists never advertise to non-owners; gate clip-member UI on
  `isFollowing`, not on `followers`.
- Tombstoned clip URIs are gone: drop any cached copy on `clipUpdated`
  naming a deleted clip (the AP surface serves `Tombstone`, not the
  collection).
