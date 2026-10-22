# 2026-10-22 — prod-ready-2 Phase 0 baseline + audit verification

Commit: `ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e` (`docs: align single-node ops and release gates`).
Tree: clean except `docs/adversarial.md`, `docs/prod-ready-2.md`, `docs/prod-repair.md` (untracked plan docs) plus
subagent placeholder test files (`tests/media_lifecycle_p0.rs`, `tests/backup_consistency_p1.rs`, `tests/dns_bounded_p1.rs`).
Toolchain (native Windows, no WSL): `rustc 1.99.1`, `cargo 1.99.1`, Windows 11 Education 10.0.28000. Docker: unavailable.

## Audit verdicts (5 parallel read-only audits vs `docs/prod-ready-2.md` §2)

- P0 media CRUD/orphan race: CONFIRMED. `FsBlobStore::put/delete` hold shared gate only around fs op
  (`src/media/storage.rs:232-234,287-290`); logical upload (`src/application/commands/media.rs:66-94`) and delete
  (`:245-256`) split DB txn + fs with no recheck; no per-key lock; sweep `collect_live_blob_keys`
  (`src/media/reconcile/scan.rs:330-353`, paged, no gate) then `sweep_orphan_blobs_checked`
  (`src/media/reconcile/heal.rs:48-117`, shared gate) permits concurrent insert deletion. Remote-cache:
  `evict.rs:53-57` deletes without refcount check; `evicted` flag never set; shared-blob double-charge.
- P0 TOTP/sign-in: CONFIRMED. All password paths mint full sessions with zero TOTP lookup
  (`signup.rs:192-278`, `signin_flow.rs:149-238`, `miauth.rs:219-344`, `meta.rs:231-276`);
  `packers/user.rs:563-565` hardcodes `twoFactorEnabled/usePasswordLessLogin/securityKeys=false`;
  `i/2fa/authenticate` (`twofa.rs:313-356`, `auth_2fa.rs:198-205`) unauthenticated, no challenge, replayable,
  skips suspended/deleted gates; `auth/2fa` throttle buckets referenced but never registered.
- P1 backup snapshot/cap: CONFIRMED. Export (`bundle.rs:417-428`) then separate `list_all_media()` inventory
  (`bundle.rs:560-583`); gaps recorded not failed (`:430-473`, refused only at restore `:187-192`);
  exclusive gate held whole creation but writers hold shared only around fs op; `BackupContext` (`mod.rs:71-76`)
  has no cap field; `main.rs:359-364` never passes `cfg.backup.max_bundle_bytes`; all checks use 64 GiB const.
  No retention; status registry unbounded (`mod.rs:196-263`); no ENOSPC tests; no dir fsync.
  `RESTORE_READY.json` written last (correct); ZIP traversal checks present (correct).
- P1 DNS: CONFIRMED. `resolve_host_with_timeout` (`url.rs:169-205`) spawn-per-lookup + `recv_timeout` + detached
  thread, no bound, not injectable. SSRF validation intact; per-request timeouts/sizes present but constants.
- P2 diagnostics: CONFIRMED (honest nulls in `admin_status.rs:247-269`, enforced by tests).
  P2 panic-on-malformed-input: REFUTED as current code defect (WS/inbox/actor paths return errors; `panic!` hits
  test-only; only production `expect` is `quota.rs:135` not network-reachable) — doc note stale; fuzz gap remains.
  Worker infinite-reclaim: REFUTED (contained via `catch_unwind` + quarantine + retry budget + tests).
  Readiness bounded: CONFIRMED implemented (`middleware.rs:194-241`, no walks).
  P1 role-policy storage-only: CONFIRMED but documented (`domain/admin.rs:38`, `roles.rs:108-123`, Admin wiki,
  endpoint-manifest); assignment surface still live — needs unmistakable non-enforcement or central evaluator.

## Implementation (parallel subagents, disjoint ownership, uncommitted)

- Media lifecycle (Phase 1): `src/media/storage.rs`, `reconcile/*`, `application/commands/media.rs`,
  `remote_cache/*`, `tests/media_lifecycle_p0.rs`.
- TOTP auth (Phase 2): `auth_http/{signin_flow,signup,miauth}.rs`, `dispatch_auth/{twofa,password}.rs`,
  `application/commands/auth_2fa.rs`, `packers/user.rs`, `tests/auth_totp_p0.rs`.
- Backup consistency+cap (Phase 3): `src/backup/{bundle,restore,mod}.rs`, `tests/backup_consistency_p1.rs`
  (shared `config.rs`/`main.rs`/`persistence/redb/backup.rs` via snippets).
- DNS bounds (Phase 4): `federation/delivery/url.rs`, `delivery/client/*`, `federation/retry.rs`,
  `tests/dns_bounded_p1.rs` (shared config/runtime via snippets).
- Diagnostics/roles/panic (Phase 6) + retention (Phase 5): `admin_status.rs`, `dispatch_admin/roles.rs`,
  `middleware.rs`, streaming/inbox/actor parsers, `tests/diagnostics_roles_p2.rs`
  (`backup/mod.rs` retention via snippet only).

Parent owns merge of shared files (`src/config.rs`, `src/main.rs`, `src/runtime.rs`,
`src/persistence/redb/*`, `docs/*`), full test gate, state logs, final commit.
Style: Google Rust Style Guide; native Windows cargo only; `cargo fmt` required.
Release gate (Phase 7) cannot fully qualify here: no Docker, no external Misskey/Sharkey instances —
final verdict must remain UNQUALIFIED until provisioned environment runs strict gate.
