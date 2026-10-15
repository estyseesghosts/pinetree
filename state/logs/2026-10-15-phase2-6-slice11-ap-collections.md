# Phase 2.6 Slice 11 — AP collections (M7)

Date: 2026-10-15. Status: implemented, full `cargo test` green.

## What changed
- Contract first: new `docs/activitypub-collections.md` (Frozen 2026-10-15), indexed in `docs/README.md`; "Superseding note — Phase 2.6 slice 11" appended to `docs/identity-rules.md`.
- `src/activitypub/actor.rs`: followers/following/outbox use `?page=true&cursor=<opaque>` paging. Page size 40; cursor = URL-safe base64 (no padding) of the internal position (followers/following: peer account id; outbox: note position). Page fetch asks for size+1 to decide `next`. Index uses counters (followers/following `totalItems`; outbox omits `totalItems` — a visible-public count is not cheaply available). `prev` omitted.
- Errors: store Err → 500 (no `unwrap_or_default`); `find_by_id` Ok(None) logged + skipped, Err → 500; invalid cursor → 400. Outbox scans further store batches (40 per batch, max 25 batches) when filtering leaves a short page.
- Misskey/Sharkey emit `cursor`/`since_id`/`until_id` with page size 10/20; pinetree uses `cursor` only, ordering by peer id for follow collections (consumers treat as sets; Misskey/Sharkey only check presence of `first`/`orderedItems`).

## Phase 3 gaps
Account carries no hide-followers/following fields (packer hardcodes "public"), so Misskey's 403-on-private behaviour is not implemented. Featured collection still static-empty. `since_id`/`until_id` unsupported.

## Failing-first (reconstructed: follower page truncation without `next`, store errors swallowed)
`slice11_followers_250_page_through_completely` FAILED, `slice11_follow_store_failure_returns_500` FAILED. `grep TEMP-NEUTER src/` empty.

## Verification
Full `cargo test`: lib 191, client_smoke 1 (SMOKE_OK), conformance 51, crash_recovery 2, federation 2, federation_interop 1, minrepro 1 = 249 passed, 0 failed (slice 10: 244).
Tests: `slice11_followers_250_page_through_completely`, `_follow_store_failure_returns_500`, `_outbox_pages_public_notes_skipping_non_public`, `_invalid_cursor_returns_400`, `_following_pages_completely`.

## Not verified
Live Misskey/Sharkey fetch of the collections (no Docker/Node 22 here; `smoke-phase2.sh` not runnable). Outbox test uses the AIDX generator under a fixed clock (pre-existing ~1e-4 counter-wrap flake risk).
