# 2026-10-15 — Smoke rerun blocked on Node majors, local bench, RSA questions

## Smoke-phase2 fresh run: blocked at Misskey migrate
- `bash scripts/smoke-phase2.sh` from scratch: postgres/redis up, then
  Misskey `pnpm migrate` fails: `re2` native module `was compiled against
  a different Node.js version (NODE_MODULE_VERSION 137, needs 127)`.
  Checkout was built under Node 24; sandbox default `node` is now v22.23.1.
- Correct setup (already in `docs/phase-2-interop-howto.md`): default Node 24
  for Misskey, Node 22 via `SHARKEY_NODE_BIN` for Sharkey. User is installing
  nvm and taking over Node; I removed my `/tmp/opencode/node24` fetch and am
  not touching Node further. Rerun + federation test pending on that.
- Last green evidence unchanged: 2026-10-14 four-server run
  (`federation_interop_four_servers ... ok`, `SMOKE_OK`, 6–25 s).

## Bench (executed): single pinetree, debug build, port 3210, fresh data dir
- `scripts/bench-local.py` (new): rotates signup users to stay under the
  `notes/create` 300/h/account bucket, reports throughput + p50/p95/p99.
- Result: `notes/create` 870/s (p50 1.1 ms), `notes/timeline(limit20)` 428/s
  (p50 2.2 ms), `users/show` 1209/s (p50 0.8 ms). 400 creates + 2000 reads,
  0 failures. Debug build = lower bound; no release-bench run.
- Bench server torn down afterwards.

## Docs
- `docs/phase-2-interop-howto.md` gotcha: `re2` is compiled per Node major —
  Misskey under its build-time Node 24, Sharkey under 22, and the
  `ERR_DLOPEN_FAILED` symptom. Two-pinetree + Misskey + Sharkey instructions
  themselves were already accurate (topology, proxy, accounts); unchanged.

## Open
- Rerun `scripts/smoke-phase2.sh` once nvm Node 24/22 are in place
  (`SHARKEY_NODE_BIN=<node22-bin>` with Node 24 default on PATH).
- User questions answered in chat: browser IPs/127.0.1.x rationale, RSA
  Marvin mitigation assessment. No code changes from those.
