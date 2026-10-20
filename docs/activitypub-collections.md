# ActivityPub collections (outbox, followers, following)

**Status:** Frozen (Phase 2.6 slice 11, 2026-10-15)
**Sources:** `sns-server-proposal.md` §7; `docs/identity-rules.md` §4 (URL shapes); `docs/phase2-6.md` slice 11 (M7); Misskey 2026.10.1-alpha.0 `ActivityPubServerService.ts` and `ApRendererService.ts` (collection and page shapes, read-only reference); Sharkey 2025.4.7 for the same paths. Implementation: `src/activitypub/actor.rs`.

This contract covers the three paged actor collections. `featured` (`/users/{id}/collections/featured`) stays an empty `OrderedCollection` and is not paged.

## 1. Endpoints

| Path | Collection | Items | Order |
|---|---|---|---|
| `GET /users/{id}/outbox` | outbox | `Create` or `Announce` activity per visible note | newest first (note id descending) |
| `GET /users/{id}/followers` | followers | actor URI of each follower | peer account id ascending |
| `GET /users/{id}/following` | following | actor URI of each followee | peer account id ascending |

`{id}` is the local account ID (identity-rules §4). Remote or unknown accounts keep the existing responses (404, or 410 for deleted accounts). Suspended accounts return 404.

Both shapes are served with `Content-Type: application/activity+json`.

## 2. Index (`OrderedCollection`)

`GET {coll}` without `page=true` returns:

```json
{
  "@context": "<ap context>",
  "id": "{base}/users/{id}/followers",
  "type": "OrderedCollection",
  "totalItems": 250,
  "first": "{base}/users/{id}/followers?page=true"
}
```

- `id` is the canonical collection URL (`{public_url}/users/{account_id}/{name}`). It never contains a node hostname.
- `totalItems` for followers and following is the account counter (`followers_count` / `following_count`), not a recount.
- `totalItems` is **omitted** for the outbox. Misskey's `notesCount` counts every note, including non-public ones, so it is not the visible count. A visible-public count would need a full scan per request, which we do not do. Omitting it is the documented choice; the field is optional in ActivityStreams.
- `first` is always present and points at the first page.
- The index has no `orderedItems` and no `next`.
- A `cursor` query parameter on the index is ignored.

## 3. Page (`OrderedCollectionPage`)

`GET {coll}?page=true` and `GET {coll}?page=true&cursor={cursor}` return:

```json
{
  "@context": "<ap context>",
  "id": "{base}/users/{id}/followers?page=true&cursor={cursor}",
  "type": "OrderedCollectionPage",
  "partOf": "{base}/users/{id}/followers",
  "totalItems": 250,
  "orderedItems": [ "https://.../users/...", "..." ],
  "next": "{base}/users/{id}/followers?page=true&cursor={next-cursor}"
}
```

- The first page has `id` `{coll}?page=true`; later pages have `id` equal to the request URL that produced them.
- `partOf` is the index URL.
- `orderedItems` holds at most **40** items (`PAGE_SIZE`). It may hold fewer when the store is exhausted or items were skipped (see §6).
- `next` is present **if and only if** more items exist after this page. The last page has no `next`. An empty collection returns a page with `orderedItems: []` and no `next`.
- `prev` is **never emitted**. Clients page forward only. Adding `prev` would need a reverse cursor, which the store does not provide.
- `totalItems` on a followers/following page is the same counter as the index. On an outbox page `totalItems` is omitted, as in §2.
- `next` is built as `{coll}?page=true&cursor={cursor}`, with `cursor` as in §4. Its query string is exactly `page=true&cursor=…`, which matches Misskey.

### 3.1 Page fetch strategy

Followers and following fetch `PAGE_SIZE + 1` rows from the store, starting after the cursor. If `PAGE_SIZE + 1` rows come back, the first `PAGE_SIZE` are returned and `next` is set from the last of those rows. Otherwise the page is final.

Outbox pages are built from the note store by scanning in batches of `PAGE_SIZE` raw notes, newest first, starting before the cursor note. Visibility filtering (`local_only`, `Public`/`Home`, `is_visible`) and render skips can shrink a batch, so the loop keeps reading batches until either `PAGE_SIZE + 1` visible items have been collected or the store is exhausted. The loop is bounded at 25 batches (1000 raw notes) per request. If the bound is reached, the page is returned with whatever was collected and `next` is set from the last raw note scanned, so no note is skipped and no `next` is wrong. The page may then be short or empty while `next` is present; clients must follow `next` until it is absent.

## 4. Cursor

- A cursor is an **opaque** string: URL-safe base64 (RFC 4648 §5 alphabet, no padding) of the UTF-8 bytes of an internal position.
- Followers/following: the internal position is the **peer account ID** of the last item on the previous page (follower ID for `followers`, followee ID for `following`). The next page starts strictly after it.
- Outbox: the internal position is the **note ID** of the last raw note scanned (see §3.1). The next page starts strictly before it, newest first.
- Clients must treat the cursor as opaque and must not construct cursors.
- An invalid cursor returns **400 Bad Request**. Invalid means: not URL-safe base64 without padding, not valid UTF-8 after decoding, or decoding to an empty string. A cursor that decodes to a well-formed but unknown position is not an error: paging simply resumes at that position in key order.

## 5. Errors

| Condition | Response |
|---|---|
| Account not found | 404 (existing behavior) |
| Account deleted | 410 (existing behavior) |
| Invalid `cursor` on `?page=true` | 400 |
| Storage error reading accounts, follows or notes | **500** |
| Storage error while rendering an outbox note | **500** |

No handler maps a storage error to an empty collection. `unwrap_or_default()` is not used on any collection path.

## 6. Per-item skips (logged)

Skips never fail the request. Each one is logged with `eprintln!` naming the collection, the account ID and the offending ID, so the operator can see it.

- **Followers/following:** `find_by_id` returns `Ok(None)` for a peer account. The item is omitted from `orderedItems` and logged. The cursor still advances past it, so the item is not retried on the next page.
- **Outbox:** the note's render returns `NotFound` (for example a missing author or a missing renote target). The item is omitted and logged. The cursor still advances past it.
- Any other render error other than a storage error (`StorageUnavailable`) is also logged and skipped. A storage error is never skipped.
- Notes that fail visibility (`local_only`, not `Public`/`Home`, not `is_visible`) are filtered silently. This is the normal path, not a failure.

## 7. Ordering and set semantics

- Followers and following are ordered by peer account ID, not by the time the follow was created. Misskey orders by follow time. This is a deliberate difference. Recipients (Misskey, Sharkey and other AP servers) treat these collections as sets, and a stable key order is what makes cursor paging exact. Clients must not depend on time order.
- Outbox order is note ID descending, which is the store's time order (newest first).
- The series of pages is exact: no item is duplicated and none is omitted, because each cursor is a key position and the store returns keys in order. Paging test: `slice11_followers_250_page_through_completely` in `src/activitypub/actor.rs`.

## 8. Query parameters not supported

- `since_id` and `until_id` on the outbox (Misskey's own paging parameters) are not supported. They are ignored. Only `page` and `cursor` are read. Clients that need Misskey-style paging should follow `next`.

## 9. Phase 3 gaps

- **Hide followers / hide following.** Misskey lets a user hide these collections (`followingVisibility` / `followersVisibility`, per endpoint-manifest §MeDetailed). `Account` has no such fields (`src/domain/account.rs`), so the handlers always serve the full collection. Adding the fields is a domain change and is out of scope for slice 11. Until then, these collections are public to any AP fetcher. Logged as a Phase 3 gap.
- Outbox `totalItems` (visible-public count) is omitted; a cheap count needs a store-side index.
- `prev` is not emitted (see §3).
- `featured` is not paged; it stays empty.

## 10. Verification

- Unit/handler tests in `src/activitypub/actor.rs` under the `slice11_` prefix cover: 250-follower full traversal (no duplicates, none missing, no `next` on the last page, index `totalItems` equal to the counter); a failing `FollowStore` returning 500; outbox paging over more than `PAGE_SIZE` public notes with non-public notes interleaved; an invalid cursor returning 400; following paging.
- Live interop with Misskey (fetching these collections from a real Misskey instance) is part of the slice 11 live check and is recorded in the Phase 2.6 log, not here.

## Addendum 2026-10-20 (freshness pass)

The contract text above is unchanged. The Status line above stays `Frozen (Phase 2.6 slice 11)`. Verified against `src/activitypub/actor.rs`, `src/domain/account.rs`, `src/application/ports.rs`, and `src/persistence/redb/mod.rs`.

### Status (2026-10-20 freshness pass)

**SUPERSEDED: §9 "Hide followers / hide following" is stale.**
- `Account` now has `followers_visibility` and `following_visibility: FollowVisibility` (`src/domain/account.rs:142,145`; default `Public`, `:456-457`). They are patchable (`src/application/ports.rs:99-100`; redb applies them at `src/persistence/redb/mod.rs:2586-2590`). They are packed as `followersVisibility` / `followingVisibility` (`src/api/misskey/packers.rs:398-403`) and set through `i/update` (`src/api/misskey/endpoints/mod.rs:9827-9828`).
- The AP handlers gate on them. Followers: `src/activitypub/actor.rs:922-924` returns `forbidden_collection()` unless the value is `Public`. Following: `actor.rs:981`. The check applies to every AP fetcher, and this handler has no follower-only exemption. Whether that matches Misskey's followers-only semantics is NOT verified here.
- So "collections are public to any AP fetcher" is no longer true. The remaining gap is the missing followers-only exemption, which needs a test against a real Misskey fetcher.

**OPEN: §9 outbox `totalItems`.** Still omitted by design (`src/activitypub/actor.rs:823`; the test asserts absence at `actor.rs:1728,1733`). There is no store-side count index, so a visible count would still need a scan. Keep the omission unless a count is required.

**LANDED: §2–§7 and the §10 tests.** The `slice11_` tests are in `src/activitypub/actor.rs` (for example `:1533-1575` for paging and counter parity, `:1800` for the index `totalItems`).
