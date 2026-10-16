# 2026-10-16 — Phase 2 audit (scope vs. tree)

Owner asked for a Phase 2 completion audit. Method: re-read proposal §7 +
§9 Phase 2, `docs/phase-2-plan.md`, `docs/phase-2-exit-criteria.md`,
`state/logs/2026-10-14-phase-2-complete.md`, then verified each scope item
against current `src/` (routes, handlers, ports) rather than trusting the logs.
No code changes. (`cargo test` not re-run here: the fresh
`KEEP=1 scripts/smoke-phase2.sh` re-run is in flight and holds the cargo lock;
latest executed evidence is 2026-10-15: lib 193 + conformance 51 +
`SMOKE_OK`/`INTEROP_OK`, see `2026-10-15-phase2-6-slice12-rsa-advisory.md`
and `2026-10-15-smoke-green-bench.md`.)

## Verdict: Phase 2 exit stands — every scope item is implemented

| Scope | Status | Evidence in tree |
|---|---|---|
| A1 WebFinger/host-meta/NodeInfo | done | routes `mod.rs:77-88`; JRD+XRD renderers + tests in `activitypub/webfinger.rs` |
| A2 keys | done (+ hardened post-exit) | `commands/keys.rs` RSA-2048 gen/rotation, `system_actor`, key-id→actor binding anti-poisoning (2.6 slice 2) |
| A3 signatures | done (+ hardened) | `signatures.rs` legacy HTTP sigs + `Digest`, clock-skew; signing via `ring` RSA_PKCS1_SHA256, <2048-bit keys rejected (2.6 slice 12) |
| B1 docs/routes | done | all routes present: `users/{id}`, `publickey`, `outbox`, `followers`, `following`, `collections/featured`, `/@{acct}`, `notes/{id}`, `notes/{id}/activity`; pure renotes render as `Announce` (`objects.rs:672-685`) |
| B2 sanitization | done | `sanitize_html_to_mfm` + `sanitize_mfm_text` with adversarial tests (`objects.rs:363-470,1300-1311`) |
| C1 safe client/SSRF | done | size/timeout/redirect limits, SSRF rejection, `PINETREE_ALLOW_PRIVATE_IPS` allowlist (`delivery.rs:139-148`) |
| C2 remote media | done | validation before fetch; metadata in `MediaStore`, bytes in `BlobStore` |
| D1 shared ops + dedup | done | all commands enqueue `FederateDeliver`; `ActivityDedup` + `forget_activity` redb impl (Misskey id-reuse fix) |
| D2 inboxes | done | shared + personal inbox routes; `Follow/Accept/Reject/Undo/Create/Update/Delete/Like/Announce` dispatched (`inbox.rs:215-267`, `handle_*`) |
| D3 durable delivery | done (+ hardened) | atomic `FederateDeliver` jobs; 408/429/5xx transient vs other 4xx permanent; exp backoff; per-destination cooldown; shared-inbox dedup (`retry.rs`, `outbox.rs:516-532`) |
| E1/E2 verification | done | `tests/federation.rs`, `tests/federation_interop.rs`, `scripts/smoke-phase2.sh`; green 2026-10-15, re-running now |

## Gaps / staleness found (none blocks the exit)

1. **Lazy RSA keygen still open** (carried from phase-2-complete §open):
   `ensure_actor_keypair` is called at actor-render/sign time
   (`actor.rs:369,498`, `delivery.rs:1151`), not at signup. Mitigated by
   harness key warm-up, not fixed in product. Low severity, real.
2. **Sharkey version pin stale**: plan + exit-criteria freeze says
   `2025.4.4`; checkout and all runs are `2025.4.7` (complete log notes it,
   frozen files never updated — needs a superseding decision per repo rules).
3. **Conformance count stale in `docs/README.md`** (`26/26`):
   `tests/conformance.rs` now holds 51 `#[test]`s, all green per slice-12 log.

## Explicitly not Phase 2 (no action)

Quotes/polls/`_misskey_votes`, `_misskey_content`, Mastodon API,
public-internet federation — deferred by proposal §7. `users/notes`,
`notes/renotes`, Misskey `users/followers`, streaming route — carried to
Phase 3 per AGENTS.md, confirmed still missing and still Phase 3 scope.
