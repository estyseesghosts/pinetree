# Phase 2.6 Slice 9 — Passwords + auth throttling (H3 + H4)

Date: 2026-10-15. Status: implemented, full `cargo test` green.

## Decisions
- Argon2id (`argon2` 0.5.3), PHC string. Params m=19456 KiB, t=4, p=1. Measured 36 ms/hash at t=2 in the test profile (opt-level 1); t=4 targets ~70 ms. Constants `ARGON2_*` in `src/api/misskey/auth.rs`.
- Legacy `salt_hex:sha256_hex` still verifies (constant-time); `needs_rehash` -> rehash through `auth.set_password_hash` on successful signin / MiAuth submit. No port change.
- Hashing and verification run in `spawn_blocking` (`hash_password_async`/`verify_password_async`). The sync dispatcher calls async signin/signup via `Handle::block_on` (only inside its own spawn_blocking).
- Throttling: failure-counted fixed window + progressive delay (1 s doubling, cap 60 s), no lockout. `auth/signin` and `auth/miauth` 10 failures per account / 30 per IP per 60 s; `auth/signup` 20 attempts/IP/hour; `auth/miauth-check` 600 requests/IP/min. Keys `acct:<lowercase user>`, `ip:<ip>`. Returns Misskey JSON 429 `RATE_LIMIT_EXCEEDED`.
- Client IP: `ConnectInfo<SocketAddr>` (server now `into_make_service_with_connect_info`). `trusted_proxies: Vec<IpAddr>` in `Config` (`--trusted-proxies=ip,ip`), `Arc<Vec<IpAddr>>` on `ApiState`. XFF honored only when the peer is trusted: right-to-left, first non-trusted hop. `X-Real-IP` and the `"unknown"` key removed; upload slots and `rate_key_for` use the same function.

## Reviewer fix
Live `client_smoke` failed 429 on the second MiAuth POST: the implementer counted `miauth_check` `ok:false` (normal pending polls) as failures in the shared `auth/miauth` bucket. Check now uses its own plain per-IP `auth/miauth-check` bucket. Smoke script now prints the body on MiAuth failure.

## Failing-first (reconstructed; neutered limiter check, XFF trust check, rehash)
7 of 12 `slice9_*` failed: spoofed_xff_from_untrusted_peer_is_ignored, auth_failures_backoff_then_count_cap_then_window_expiry, many_accounts_from_one_ip_trip_the_ip_bucket, legacy_hash_verifies_then_upgrades_to_argon2id_on_signin, signin_brute_force_returns_rate_limit_exceeded, signin_per_ip_bucket_trips_across_accounts, (+ new_hashes_are_argon2id_phc, a stale `t=2` literal, fixed). `grep TEMP-NEUTER src/` empty.

## Verification
Full `cargo test`: lib 166, client_smoke 1 (SMOKE_OK), conformance 46, crash_recovery 2, federation 2, federation_interop 1, minrepro 1, others 0 = 219 passed, 0 failed (baseline 207, +12). No new warnings introduced by this slice (unused-import warnings are pre-existing).

## Open
- Limiter maps are never pruned (pre-existing pattern). Per-account progressive delay lets an attacker delay a victim up to 60 s (by design: no lockout). Users behind one NAT share the IP bucket.
- `smoke-phase2.sh` not run (needs Node 22 + Docker).
