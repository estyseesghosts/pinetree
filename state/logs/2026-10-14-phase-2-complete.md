# 2026-10-14 — Phase 2 (Minimal federation) complete: hang fixed, four-server interop green

Supersedes nothing; continues `2026-10-14-phase-2-d1-d3-app-wiring.md`.

## Slice ledger (A–E)

| Slice | What | Evidence |
|---|---|---|
| A1–A3 | WebFinger / host-meta / NodeInfo, actor keys (RSA, redb), HTTP signatures (sign + verify, Digest) | `cargo test --lib` 112/112; live: Misskey and Sharkey both resolved `pt3@127.0.1.3` via WebFinger and fetched/validated the signed actor |
| B1–B2 | Actor + Note documents, MFM→HTML sanitizer, `source` (`text/x.misskeymarkdown`), no `_misskey_content` | lib tests; Misskey/Sharkey accepted pinetree notes, renotes, reactions |
| C1–C2 | `SafeHttpClient` (SSRF, size, redirect, timeout), remote media cache | `tests/federation.rs::federation_remote_media_cache` ok |
| D1/D3 | Transactional `ActivityDedup`, durable `FederateDeliver` jobs, backoff retry | `tests/conformance.rs` 27/27 (new: `d1_forget_activity_allows_reprocessing`); two-node retry phase |
| D2 | Inbox (shared + personal), signature/Digest auth, authority checks, all core activities | `tests/federation.rs::federation_two_node_end_to_end`; four-server run below |
| E1 | Two-node in-process suite | `tests/federation.rs` |
| E2 | Four-server live interop: Misskey `2026.10.1-alpha.0`, Sharkey `2025.4.7`, 2× pinetree | `scripts/smoke-phase2.sh` → `SMOKE_OK`; `tests/federation_interop.rs` |

(Plan pinned Sharkey `2025.4.4`; the `./sharkey` checkout is `2025.4.7`. Interop was run against the checkout.)

## The blocker: root cause and fix

`federation_two_node_end_to_end` hung because axum handlers (and `JobWorker::spawn`) ran **blocking** `SafeHttpClient`/redb work directly on tokio worker threads. A handler that blocks on an HTTP call to a peer served by the same runtime starves the accept/IO path (reproduced with zero app code in `tests/minrepro.rs`; the 4/16/64-thread results in the handoff were consistent with this: parked workers hold the IO driver).
Fix (correct for production too):
- `api_dispatch` → thin async wrapper over `api_dispatch_blocking` via `tokio::task::spawn_blocking` (`src/api/misskey/endpoints/mod.rs`).
- Inbox handlers run `process_inbox` under `spawn_blocking` (`src/federation/inbox.rs`).
- `JobWorker::spawn` runs `process_one` under `spawn_blocking` (`src/jobs/worker.rs`).
- `tests/minrepro.rs` now uses `spawn_blocking` in the handler and passes (0.21 s); it is kept as the regression vehicle.

## Other real bugs found and fixed

1. `resolve_local_account` (inbox.rs) returned any account whose `uri` matched, including a *remote cached copy* of a local actor, so `Undo(Follow)` silently no-op'd. Now only local accounts qualify.
2. **Misskey reuses ids**: its Follow id is `/follows/{follower}/{followee}` and its Undo id `/follows/{follower}/{followee}/undo` — identical for every follow/unfollow of the same pair. Activity-id dedup therefore swallowed a re-follow (no Accept → Misskey stuck "pending") and a second unfollow. New `ActivityDedup::forget_activity` (default no-op, redb impl) is called after `Undo(Follow)` for both ids. Found only because the interop test was re-run on the same stack.
3. Remote-user resolve failures in `users/show` are now logged to stderr (were swallowed).
4. Warm-up of pinetree's lazily generated RSA key matters when the key is first needed by a *peer's* fetch (dev build ≈ 9 s vs a peer's ~5–10 s timeout; Misskey surfaced it as `AbortError`). The harness warms keys; generating at signup would be a sensible follow-up.

## Interop harness findings (all environmental, documented in `docs/phase-2-interop-howto.md`)

- Misskey/Sharkey `federation` defaults to `none` → must `admin/update-meta {federation:"all"}`.
- Both refuse `http://` actor/url fields when `NODE_ENV=production` (`checkHttps`) → run with `NODE_ENV=development`; `MISSKEY_WEBFINGER_USE_HTTP=true` for http WebFinger.
- Misskey's WebFinger URL builder drops the port from URL queries → each node gets its own loopback IP on port 80 (`127.0.1.1–4`) via `scripts/interop/proxy.mjs`.
- Sharkey needs Node 22 and, in cluster mode, its queue worker never ran here (outbound Follow stayed pending) → `MK_DISABLE_CLUSTERING=1`.
- Sharkey's `following/delete` consults a 30-minute in-memory followings cache that misses remote Accepts; a local follow event (sk1 → local sk2) invalidates it (test does this; not a pinetree defect).
- Misskey does not decrement `renoteCount` when an inbound `Undo(Announce)` deletes the remote renote (the note *is* deleted); the test uses `notes/renotes` instead.
- pinetree rate-limits like Misskey (429 + `retryAfter`); the test client honours it.

## Verification (executed)

`tests/federation_interop.rs` flow: resolve all 12 ordered pairs → 12 follows, all Accepted (every node reports 3 followers) → public note per node visible on the 3 others → ring reactions `👍` (`_misskey_reaction`) visible at owner → ring-2 renotes counted at owner → undo reactions → undo renotes (delete) → delete notes (remote copies 4xx) → unfollow all (every node back to 0 followers).

```
scripts/smoke-phase2.sh (fresh databases + all four servers):
  test federation_interop_four_servers ... ok
  test result: ok. 1 passed; 0 failed; ... finished in 25.09s
  SMOKE_OK
two further runs of the same test on the same live stack (re-follow/re-unfollow path):
  test result: ok. 1 passed ... finished in 9.70s
  test result: ok. 1 passed ... finished in 12.02s
```

Rust suites (`cargo test --offline`, interop test self-skips without `PINETREE_INTEROP=1`):
see the final numbers in the addendum below.

## Open questions / follow-ups

- Generate the actor RSA key at signup (or on a background job) instead of on first fetch.
- Phase 2 is MVP-scoped: no quotes/polls/`_misskey_votes`, no `_misskey_content`, no Mastodon API, no public-internet federation.
- pinetree has no `users/notes` / `notes/renotes` / `users/followers` endpoints; the interop test works around via home timeline and counts (Phase 3 candidates).

## Addendum — flake root cause: a real RSA signing bug

`federation_two_node_end_to_end` failed roughly 1 run in 7–10 (first seen as "A resolves bob → NO_SUCH_USER"; with stderr diagnostics added, `sign activity: "BigUint subtraction underflow"`). Cause: hand-rolled RSA CRT signing in `src/activitypub/signatures.rs` computed `(m1 + p − m2) mod p` with `m2 < q` possibly larger than `m1 + p` whenever the key has `q > p`. That is a production defect, not a test artifact: any actor whose key had q noticeably larger than p failed a percentage of signatures (every delivery/fetch signed with it). Fix: reduce `m2 mod p` first. Regression test `sign_verify_with_q_larger_than_p` (200 signatures with a q>1.125p key) fails without the fix ("sign must not underflow") and passes with it. Existing stored keys stay valid.

After the fix: `tests/federation.rs` 40/40 consecutive passes (was ~1 failure per 7–10).

## Final numbers (executed 2026-10-14)

`cargo test --offline`: lib 113/113, client_smoke 1/1, conformance 27/27, crash_recovery 2/2, federation 2/2, federation_interop 1/1 (skips without `PINETREE_INTEROP=1`), minrepro 1/1.
`scripts/smoke-phase2.sh` with the final binary: `federation_interop_four_servers ... ok`, `SMOKE_OK` (fresh run; earlier fresh run + two same-stack reruns also green after the dedup fix).
