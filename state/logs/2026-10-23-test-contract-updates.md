# 2026-10-23 test contract updates (parent)

Mechanical shared-test updates necessitated by committed wave behavior
changes. No source behavior changes in this commit (except the
`tests_batch_pq` lib expectation, which pins the Wave 7 collapse already
implemented in `files_http.rs`).

## Changed
- `CreateAccountInput` handshake fields (`also_known_as`, `moved_to_uri`,
  `moved_at_ms: None`) at literal sites missed by the Wave 6 actor agent:
  `tests/batch_q_phase1_app.rs`, `tests/conformance/common.rs` (×2),
  `tests/conformance/moderation_store.rs`, `tests/storage_eviction_heal.rs`,
  `tests/gaps_api_admin.rs` (×2). Tree failed `cargo check --tests` with
  E0063; fixed sites compile.
- `tests/gaps_api_auth.rs` (`totp_key_done_passwordless_unregister_rotate`):
  advance the manual clock past the TOTP step after `done_2fa` — the
  confirmation consumes that step's code (replay protection), so the old
  same-step re-auth failed with `INVALID_TOKEN token already used`.
- `tests/gaps_webhooks.rs`: loopback delivery is now `PermanentFailure`
  (SSRF-blocked) by Wave 3 design. `deliver_success_posts_signed_body`
  and `deliver_transient_then_cooldown_defers` pin the blocked contract
  (closed loopback port, no cooldown); signature round-trip asserts kept
  (pure crypto, no wire).
- `tests/storage_paging_c1.rs`
  (`maintenance_triggers_enqueue_drainable_jobs`): Wave 6 idempotent
  enqueue makes the repeat boot a no-op (pending stays 2, drain 2) instead
  of stacking to 3.
- `tests/files_http_cache_hit.rs` + `src/.../tests_batch_pq.rs`
  (`batch_q_proxy_route`): wrong-type proxy refusal is the collapsed 404
  (Wave 7 oracle collapse), not 400. UA-missing 400 and loop behavior
  unchanged.

## Verified (parent, native cargo, shared target)
- `cargo check --locked --offline --all-targets` → exit 0, no errors.
- `cargo test --locked --offline --test gaps_webhooks --test gaps_api_auth --test storage_paging_c1` → 12 + 6 + 19 passed, exit 0.
- `cargo test --locked --offline --lib -- batch_q_proxy_route` → 1 passed.
- `rustfmt --edition 2024 --check` on all touched files → clean.
