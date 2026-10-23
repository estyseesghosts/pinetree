# 2026-10-22 adversarial-2 repair final gate

Scope: verify all 9 slices of `C:\Users\julie\.opencode\plan\pinetree-adversarial-2-repair.md`,
re-run the plan's regression set, run the native-Windows final gate. No commits,
no stash/reset/checkout. Two gate-revealed test updates were made minimally in
place (see §5); no source behavior was changed by this gate run.

## 1. Slice log inventory (`state/logs/`) — all 9 present, none missing

| Slice | Log file |
|---|---|
| 1 cache reservation | `2026-10-22-cache-reservation.md` |
| 2 evict starvation | `2026-10-22-cache-evict-starvation.md` |
| 3 files serving | `2026-10-22-files-serving.md` |
| 4 reconcile CAS | `2026-10-22-reconcile-dangling-cas.md` |
| 5 class accounting | `2026-10-23-cache-class-accounting.md` (dated 10-23, landed 10-22 8:14 PM) |
| 6 passwordless advertise | `2026-10-22-passwordless-advertise.md` |
| 7 cargo target | `2026-10-22-cargo-target.md` |
| 8 backup status bound | `2026-10-22-backup-status-bound.md` |
| 9 streaming me-updated | `2026-10-22-streaming-me-updated.md` |

`git status --short` at gate start showed all slice source/test/log files
uncommitted, as expected; nothing was committed, stashed, reset, or checked out.

## 2. Plan regression re-runs (native `cargo test --locked --offline`)

Env: `CARGO_TARGET_DIR=C:\Users\julie\.cargo\shared-target`,
`C:\Users\julie\WinLibs\ucrt64\bin` + `C:\Users\julie\.cargo\bin` on PATH,
cargo 1.99.1. All binaries newer than the last source edit at run time
(freshness rule; e.g. lib binary 8:17:11 PM > last source edit 8:13:30 PM).

```text
--test storage_cache_budget:            3 passed
--test storage_cache_evict:             3 passed
--test storage_eviction_heal:           4 passed
--test cache_reservation_accounting:    7 passed
--test cache_evict_starvation:          5 passed
--test cache_class_accounting:          5 passed
--test files_http_cache_hit:            4 passed
--test reconcile_dangling_cas:          6 passed
--test backup_status_bound:             4 passed
--test backup_retention_and_diagnostics: 7 passed
--lib passwordless (tests_auth_totp_p0): 2 passed
--lib totp:                             19 passed
--lib me_updated (dispatch.rs helper):  4 passed
cargo metadata --locked --offline --no-deps: exit 0 (slice 7 intact;
  tracked `.cargo/config.toml` carries only a comment, no `target-dir`)
```

## 3. `cargo fmt --check`

Clean, exit 0 — before and after the §5 test updates.

## 4. Full suite: `cargo test --locked --offline --no-fail-fast`

First full run (exit 101, full output
`C:\Users\julie\AppData\Local\Temp\adv2-gate-full.txt`, 304880 bytes):
lib 656/656 passed; every integration target passed except 4 —
`client_profile`, `client_smoke` (both `SMOKE_FAIL: bun not found`, §6),
`gaps_api_auth::totp_key_done_passwordless_unregister_rotate`, and
`gaps_final_wiring::gaps_final_wiring_arms` (both slice-6 regressions, §5).

Second full run after the §5 fixes (exit 101, full output
`C:\Users\julie\AppData\Local\Temp\adv2-gate-full2.txt`):

```text
lib: 656 passed; 0 failed
every integration target: ok — except:
  --test client_profile: FAILED (SMOKE_FAIL: bun not found)
  --test client_smoke:   FAILED (SMOKE_FAIL: bun not found — tried BUN_BIN, ~/.bun/bin/bun, PATH)
Doc-tests: 0 passed; 0 failed
error: 2 targets failed: `--test client_profile` `--test client_smoke`
```

Tail of the second run is the doc-test block above plus the 2-target error
line. Full per-target output is in the TEMP file, not the repo (disk
mindfulness; TEMP is the approved scratch dir).

## 5. Gate-revealed fixes (minimal, in place, uncommitted)

Both failures encoded the pre-slice-6 advertised behavior; slice 6
deliberately changed it (`set_2fa_passwordless(true)` → `INVALID_PARAM` 400,
packer always `false`). The plan's slice 6 only listed
`tests_auth_totp_p0.rs` for updates and missed these two call sites. Fixed
by updating assertions to the new contract, no behavior change:

- `tests/gaps_api_auth.rs` (`totp_key_done_passwordless_unregister_rotate`):
  `set_2fa_passwordless(&bob, true).unwrap()` → assert it returns
  `INVALID_PARAM`, then `set_2fa_passwordless(&bob, false).unwrap()` as the
  no-op-clear path. Rest of the rotate/unregister flow untouched.
- `tests/gaps_final_wiring/two_factor_apps.rs` (`i/2fa/password-less`
  arm): expect 400 + `INVALID_PARAM` instead of 204. Later arms
  (update-key/done/authenticate/unregister/apps) do not depend on the flag
  and pass unchanged.
- Re-verified: `gaps_api_auth` 6/6, `gaps_final_wiring_arms` ok,
  `fmt --check` clean, rebuilt binaries (8:21:45/8:21:56 PM) newer than the
  test edits (8:21:39 PM).

Note for the parent: these are shared test files (`tests/*`), normally
parent-owned for merging — the gate brief explicitly authorized minimal
in-place fixes for gate failures, so they were edited here and left
uncommitted for the parent to review.

## 6. Environment skips (recorded as skips, not passes)

- `client_profile` / `client_smoke`: require `bun` (misskey-js live-client
  smoke via `scripts/smoke-phase1.sh` / `smoke-phase3.sh`); `bun` is not
  installed on this host and there is no `package.json` in this repo.
  Fails with `SMOKE_FAIL: bun not found` before touching any slice code.
- `scripts/smoke-all.sh` and Docker were not started, per plan.

## 7. Open out-of-scope items (unchanged)

Strict `SMOKE_ALL_REQUIRED=1`, both Docker transcode configs, live
Misskey/Sharkey federation, backup-after-eviction restore. Also: slice 9
left neighboring streaming `.ok()` pack arms alone (noted in its log);
historical `full_bytes` inflation predating the per-key class field is
documented in the slice 5 log, not repaired by guessing.
