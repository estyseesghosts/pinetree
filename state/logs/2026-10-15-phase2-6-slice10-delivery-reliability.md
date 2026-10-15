# Phase 2.6 Slice 10 — Delivery reliability (R5 + M7 fan-out cap)

Date: 2026-10-15. Status: implemented, full `cargo test` green.

## What changed
- `SendError{kind,message}`/`SendErrorKind` in `federation/delivery.rs`: permanent = SsrfBlocked, InvalidUrl, DnsNotFound, Redirect, TooLarge; transient = Dns, Timeout, Connect, Io, Tls, Protocol, Internal. `classify_inbox_status`: 408/429/5xx transient, 3xx and other 4xx permanent. `Retry-After` (seconds or HTTP-date, capped 1 h) → `max(backoff, retry-after)`.
- DNS lookup on a helper thread bounded by `dns_timeout` (5 s); plain-HTTP path has `total_timeout` (20 s) via `DeadlineStream`. HTTPS (curl) keeps `--max-time`.
- `JobQueue::{complete_with_followups, enqueue_jobs, defer}` (redb + conformance cells; docs/application-ports.md superseding note). No FDB impl exists yet.
- Fan-out: followers paged by 1000 (no 10k cap); >100 inboxes → per-inbox override jobs in batches ≤500 (last batch atomic with completion); ≤100 delivered inline, transient/cooldown inboxes become per-inbox jobs, permanent ones logged and dropped. `DestinationTracker::cooldown_until` consulted before send; cooldown → `Deferred`/`defer`, attempts not counted. Dead `MAX_DELIVERY_ATTEMPTS` deleted.
- All `worker.drain(64)` removed from API handlers; worker's 50 ms loop delivers. `follows_and_notifications` test drains explicitly. smoke-phase1 and federation_interop already poll.

## Failing-first (reconstructed: cooldown check disabled, failed-inbox follow-ups dropped)
`slice10_one_failing_inbox_retries_only_that_inbox` FAILED, `slice10_cooldown_host_is_deferred_not_attempted` FAILED (3 other conformance/ worker cells passed since they test store primitives). `grep TEMP-NEUTER src/` empty.

## Verification
Full `cargo test`: lib 186, client_smoke 1 (SMOKE_OK), conformance 51, crash_recovery 2, federation 2, federation_interop 1, minrepro 1 (+0-test targets) = 244 passed, 0 failed (slice 9: 219).

## Open / risks
- Worst-case inline fan-out (100 × timeouts) can exceed the 30 s job lease; per-job time budget is a follow-up.
- NXDOMAIN detection matches resolver message text. Batches >500 are enqueue-then-complete (crash may duplicate per-inbox jobs; receivers dedupe).
- Pagination test (e) is unit-level (fake page fn), no 2500-row redb run. `followers_of` cursor semantics (strictly after cursor) are from the redb impl, not documented in the port.
- `smoke-phase2.sh` and live interop not run (Node 22 + Docker unavailable).
