# Developer guide (add a feature without breaking federation)

Status: living page, 2026-10-21. Sources: `AGENTS.md`, `sns-server-proposal.md` §§2-5, `docs/application-ports.md`, `docs/persistence-conformance.md`, `docs/testing.md`.

## 1. Rules

- Dependency: `domain <- application <- adapters`. Domain never imports HTTP/AP/DB. Misskey API + AP call the same `AppServices` ops — no duplicated business logic.
- One txn commits state + jobs; workers fan-out. Jobs idempotent, retryable, crash-safe. Respect FDB limits (5 s, 10 MB): no blobs/unbounded fan-out in txn (followers paged 1000, jobs ≤500/txn, payload ≤64 KiB).
- Every setting via JSON config (`--config`, serde default, `deny_unknown_fields`, CLI overrides, `validate()` once, round-trip test). No env-only settings without exception.
- Style: `rust_style_guide.html`. Subagent rule: disjoint file sets, shared docs/router/schema parent-owned, uniquely-named tests, never commit/stash/reset.

## 2. Add an endpoint (checklist)

1. Port method in `application/ports.rs` (state + jobs in one txn) → redb impl → conformance group in `tests/conformance.rs`.
2. `AppServices` command in `application/commands/` (validate reply/renote targets, visibility narrowing, `get_owner()` hook).
3. Dispatcher arm in `endpoints/mod.rs` (`check_allowed_fields`, `requireCredential/permission`, `rate_limiter.check`, `map_*_error` with stable UUID), packer in `packers.rs`, error ctors in `errors.rs`.
4. Streaming event in `application/events.rs` + `hub.rs` mapping if live; notification gate if notifying.
5. Federation: outbox recipient collection + `execute_federate_deliver` arm + inbox `handle_inbound_*` + dedup record (if federated).
6. Docs: `endpoint-manifest.md` row + profile §3 + manifest addendum if superseding; state log entry (never rewrite past logs).
7. Verify: `cargo test --offline` (536 lib + conformance), `scripts/smoke-all.sh`, `scripts/keystone-sweep.sh`, live Misskey/Sharkey checkout if compat-claimed. Compare binary mtime before trusting gates.

## 3. Add storage / jobs / media

New store ⇒ `ports.rs` trait → redb tables → conformance → FDB inherits path comment. New job ⇒ `JobKind` + typed payload + `dispatch_job` arm (Validation→Quarantine, Storage→Retry) + idempotency key. New media ⇒ metadata in-DB, bytes via `FsBlobStore` (durable vs cache pool, quota + refcount + reconciler), never in txn.

## 4. Federation checklist

Sign outbound (`Digest`, system-actor key for GETs), verify inbound (key refresh-once, `actor==signer`), SSRF via `SafeHttpClient`, 64 KiB caps, dedup + tombstones, per-item visibility on serve, anonymous→404. Test 4-server interop (`smoke-phase2.sh`).

Read more: [Architecture](Architecture.md), [Federation](Federation.md), [Client API](Client-API.md), [How to read the docs](How-To-Read-The-Docs.md).
