# 2026-10-21 storage overhaul S2 gate

## Landed
- S2 merge `53106b8`: quota, on-demand cache, reconciler (deadlock + legacy-count fix), fetch_stream, `tests/storage_*.rs` + parent merge fixes. Tree clean except untracked `docs/database.md`, `docs/gaps.md` (left alone).

## Verified (executed)
- `cargo test --offline --lib`: 534/534 pass, 8.22s.
- `cargo test --offline --no-fail-fast` (full matrix): 534 lib + 10 + 6 + 1 + 119 + 2 + 14 + 6 + 7 + 2 + 1 + 1 + 6 + 5 + 3 + 5 + 4 + 16 pass; only failure is `client_smoke_live_binary_and_misskey_js` (exit 127 / exit 1).
- `tests/storage_*`: 3 + 5 + 4 = 12/12 green in-tree.
- `bash scripts/smoke-all.sh`: step 1 DONE (247s, masked — see gap), step 2 FAIL `SMOKE_FAIL: misskey checkout missing at .../misskey/packages/misskey-js/src/api.ts`. No `./misskey` symlink in this worktree (git-ignored, expected at native-Linux path per `docs/testing.md:48`).

## Known-gate caveats hit
- `smoke-all.sh:53` pipes cargo through `tail -30` without pipefail → step 1 reports DONE despite `client_smoke` failure (`docs/testing.md` gap 1).
- `client_smoke` fails two ways: native Windows (WSL-bash gets `C:\…` path, backslashes eaten) and WSL worktree (checkout absent). Both environmental, unrelated to storage diff.
- `docs/TODOS.md` P1 storage boxes stay open: gate requires green `smoke-all.sh` incl. cap-never-exceeded, local-survives-eviction, cross-owner delete, crash-heal.

## Open
- Re-run gate from a worktree with `./misskey` + `./sharkey` re-linked (or accept SKIP/FAIL as environmental). No code fix attempted for gate script per TODOS P3 (separate change).
