# 2026-10-15 — Smoke-phase2 fresh run green (nvm Node 24/22)

Supersedes `2026-10-15-smoke-bench-node-blocked.md` (which recorded the
Node-major block).

## Environment fix
- User installed nvm; `nvm install 24` → v24.22.0 (default). System node
  stays v22.23.1.
- Run: default PATH Node 24 (Misskey), `SHARKEY_NODE_BIN=/usr/sbin`
  (Node 22, Sharkey). Misskey `re2` (built for NV 137) and Sharkey modules
  (NV 127) both load under their own majors — the earlier
  `ERR_DLOPEN_FAILED` is gone.

## Verification (executed, fresh databases + all four servers)
- `bash scripts/smoke-phase2.sh` → `tests/federation_interop.rs`
  `federation_interop_four_servers ... ok` (11.21 s), `INTEROP_OK`,
  `SMOKE_OK`. Flow covered: 12-pair resolve → 12 follows accepted →
  public notes on all 3 peers → ring reactions → ring-2 renotes →
  undo reactions/renotes → note deletes → unfollow to 0 → locked-account
  pending/accept cycle.
- The trailing `Killed` lines are the script's own `trap stop_all EXIT`
  teardown (non-`KEEP` mode), not failures.
- `cargo audit`: only `RUSTSEC-2023-0071` (`rsa` 0.9.10, no fixed upgrade;
  signing already delegated to `ring` per slice 12).
- Bench (same day, `scripts/bench-local.py`, debug build): notes/create
  870/s p50 1.1 ms; notes/timeline 428/s p50 2.2 ms; users/show 1209/s
  p50 0.8 ms; 0 failures.

## Open
- Stack torn down after the run. For interactive use:
  `KEEP=1 SHARKEY_NODE_BIN=/usr/sbin scripts/smoke-phase2.sh`, then
  Misskey `http://127.0.1.1`, Sharkey `http://127.0.1.2`,
  pinetree A/B `http://127.0.1.3`/`http://127.0.1.4`
  (Windows browsers: `http://localhost:3101`–`3104`).
- Sharkey web UI (vite dev + second proxy, Node 22) offered, not yet built.
