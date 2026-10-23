# 2026-10-23 Waves 5/6 — negative cache, actor single-commit, authority matrix

Three subagents (negative cache; actor upsert; authority matrix). One
combined parent commit: `delivery/resolve.rs` and `inbox/mod.rs` carry both
Wave 5 and Wave 6 hunks and cannot be split. Fan-out cursor (`job_queue.rs`)
and dedup (already committed `62a14ed`) land separately. Parent `config.rs`
work (Wave 5 JSON-config rule) is still open — see below.

## Changed
- `src/federation/mod.rs` (negcache) — `KeyFetchNegativeCache` (bounded
  TTL, same shape/backoff as `StampNegativeCache`: 60s×failures capped 1h,
  `ACTIVITY_INDEX_CAP` bound) + `KeyFetchSingleflight` (per-key
  leader/followers via `Condvar`; leader re-checks gate; `catch_unwind`
  containment). Wired into `clear_ephemeral_caches_for_tests`. 2 unit tests.
- `src/federation/delivery/resolve.rs` (both) — `resolve_remote_public_key`:
  positive-hit → negative gate (`should_fetch`) → singleflight join →
  leader re-check → single `fetch_remote_key_once` → record → finish; actor
  upsert: handshake (`also_known_as`, `moved_to_uri`, one `moved_at_ms`
  read) rides the first `CreateAccountInput`; refresh keeps single update.
  Wave 5 gate + singleflight preserved by actor agent (diff-checked).
- `src/federation/inbox/mod.rs` (negcache) — `resolve_and_verify`
  negative-cache fast path skips `force_refresh=true` retry when initial
  miss already recorded failure. Authority agent preserved it untouched.
- `src/application/ports/account_store.rs` (actor; parent-reviewed shared
  file) — `CreateAccountInput` gains `also_known_as: Option<Vec<String>>`,
  `moved_to_uri: Option<String>`, `moved_at_ms: Option<i64>`
  (`local()` defaults `None`). Additive, no signature breaks.
- `src/persistence/redb/account_store.rs` (actor) — `build_account` copies
  the three new fields. Preserved Wave 4 counter hunk.
- 17 mechanical literal sites (`...: None` ×3): api `tests_admin`,
  `tests_auth`, `tests_batch_n/admin`, `tests_batch_o`, `tests_batch_pq`,
  `tests_common`, `tests_notes/core`, `tests_users`; application
  `commands/{blocks,invites,notifications,notes/federation_tests}`,
  `queries/timeline_tests`; inbox `{delete ×2,like,update,vote}`.
- `tests/wave5_negcache.rs` (new) — counting stub origin (500, 300ms
  stall) + `ManualClock`: 10 sequential dead-key resolves → 1 fetch;
  8 concurrent → still 1; repeats → 0; +61s → exactly 1 refetch. Asserts
  `DEFAULT_MAX_DNS_IN_FLIGHT == 32`.
- `tests/wave6_actor_upsert.rs` (new) — `(creates, updates) == (1, 0)`
  with handshake; stored row carries profile + move columns together.
- `tests/wave6_authority_matrix.rs` (new, test-only, no src change) —
  23 rows driving real `POST /inbox` with forged object-authority claims
  (`actor == signer`): per-arm re-validation asserted (Delete/Move/
  QuoteRequest/`Delete(Person)`/Accept/Reject/Update/Add/Remove/Flag…).
  Choice: arms re-validate; gate unchanged. Parent `rustfmt`ed both new
  Wave 6 test files (import order + assert shape only).

## Verified (subagents, native cargo, shared target dir)
- `--test wave5_negcache` → 2 passed. `--lib federation::` → 77 passed.
- `--test wave6_actor_upsert` → 3 passed.
  `--lib activitypub::actor::remote_actor` → 8 passed.
- `--test wave6_authority_matrix` → 23 passed.
- `rustfmt --edition 2024 --check` on all 8 key files → clean after
  parent fmt of the two Wave 6 test files.

## mtime evidence
- `wave5_negcache-*.exe` 22:18:23 > `mod.rs` 21:45:43.
- `wave6_actor_upsert-*.exe` 22:27:08 > `resolve.rs` 22:23:29.
- `wave6_authority_matrix-*.exe` 22:35:46 > test 22:35:38 (parent fmt
  after green; re-check clean).

## Old-code failure notes
- Negcache: every miss fetched (no gate/coalescing) → 10 sequential = 10
  fetches, 8 concurrent = 8 fetches (each a `spawn_blocking` thread + DNS
  slot).
- Upsert: create (txn 1, profile) + update (txn 2, handshake) → crash
  between left row without move columns; old `(1, 1)` fails `(1, 0)`.
- Authority: forged Delete removed local notes, forged Move set
  `moved_to_uri`, forged QuoteRequest stored rows, third-party
  `Delete(Person)` tombstoned victims — all now 401 + state survives.

## Open (parent)
- Wave 5 `src/config.rs` JSON-config rule (`allow_private_ips`,
  extra-CA, debug-federation, reply-repair-limit + LOW-5/LOW-6) is parent
  work and NOT in this commit.
- Wave 6 `ports/job_queue.rs` trait doc ("No de-duplication") is stale
  after the fan-out cursor fix — update at that commit.
- `job_queue.rs` (fan-out cursor + scans counters) still uncommitted;
  waits on scans agent.
