# 2026-10-23 adversarial-3 final gate (parent)

All of `docs/adversarial-3.md` is implemented, committed in fix order, and
gated on a clean tree. Subagents did all implementation work on disjoint
files (parent merged shared-file snippets, applied the sweep-resume
snippet, fixed 3 agent-test bugs, 2 gate-order swaps, 1 visibility break,
and the Wave 2 cancelled-agent repair handoff). No commits by subagents.

## Commits (this session, fix order)
- `62a14ed` dedup mark on conflict (Wave 6)
- `44c19b8` key-fetch negative cache + single-commit actor upsert +
  authority matrix (Waves 5/6)
- `2d2f7cc` cache debit, backfill, abandon races (Wave 1)
- `6fe43da` storage-error redaction + streaming drop counters (Wave 8)
- `be0de2c` shared-test contract updates (SSRF/idempotency/oracle)
- `7e91eb5` eviction scans, tombstone paging, read-only eviction (Wave 2)
- `86a9e56` backup/fan-out/sweep bounds + maintained counters (Wave 4,
  incl. parent-applied sweep resume cursor)
- `fadc182` idempotent enqueue (Wave 6)
- `045c395` file serving hardening (Wave 7)
- `48ff659` layering, sign-in oracles, 2FA contracts (Wave 8)
- `8eb605a` JSON-config compliance + integrity flag (Wave 5 parent)
- `ed12eaa` verified adversarial-2 baseline snapshot

## Final gate (native PowerShell + native cargo, shared target dir)
- `cargo test --locked --offline --no-fail-fast` → exit 101 with exactly
  2 failing targets: `--test client_profile`, `--test client_smoke`,
  both `SMOKE_FAIL: bun not found` (environmental; `bun` not installed
  on this host — identical to the adversarial-2 gate, recorded as skips
  not passes). Log: `$env:TEMP\finalgate.log`.
- `lib`: 666 passed, 0 failed (incl. config, delivery, streaming,
  fan-out, authority-adjacent unit tests).
- 84 `test result:` lines total; every integration target ok except the
  two `bun` ones above (incl. all `wave*` suites: h1/h2/m3/m4/l1/l3,
  evict-scan, smtp/ssrf, backup-liveness, fanout-split, scans-bound,
  fanout-cursor, dedup-mark, negcache, actor-upsert, authority-matrix,
  files, api-auth, stream-err).
- Freshness: newest source mtime 2:26:29 AM (`wave6_fanout_cursor.rs`
  fmt); lib test exes 2:29–2:30 AM — newer, so the greens are on final
  code (the full gate itself rebuilt everything after the last edit).
- `cargo check --locked --offline --all-targets` (pre-commit) → exit 0,
  no errors. `rustfmt --edition 2024 --check` on every touched Rust file
  → clean at each wave commit (one `.md` was once passed to rustfmt by
  mistake; no source effect).

## Old-code failure coverage
Each wave log records why its regression tests fail on the old behavior
(H1 debit, H2 double-count, M3 single-lock abandon restore, M4 orphan,
L1 slack, L3 saturating release; tombstone head-skip; webhook
`SsrfBlocked` vs dial; SMTP socket tests; backup cap/streaming/skip-log
pins; fanout 100-split; counter-vs-decode pins; negcache 10→1/8→1;
upsert (1,0); authority 401 matrix; TOCTOU/Location/oracle/Cache-Control/
pool pins; redaction sentinel + drop counters; oracle/2FA/layering pins).
Shared-test updates that the behavior changes required
(`gaps_webhooks` loopback, `storage_paging` dedupe, proxy 400→404,
TOTP-step replay) are committed with rationale in `be0de2c` and the
Wave 8 log.

## Explicitly out of scope (untouched, per audit)
IPv6 fail-closed (`url.rs`), Activity id index restart (doc comment),
passwordless dead field (signup/sign-in requirements unchanged).

## Verified-good list (no regressions)
SSRF core, signatures, inbox 64 KiB + `actor == signer`, job caps and
quarantine, `deny_unknown_fields` (all new sections carry it), backup
traversal + streaming verify, `thread_stack_size(8<<20)`, observability
redaction (extended to storage errors + stream skips).
