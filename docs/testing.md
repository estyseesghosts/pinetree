# Testing and running everything

Single entry point for verification. Source of truth for behavior is
`sns-server-proposal.md`; contract details live in `docs/endpoint-manifest.md`
+ `docs/phase-3-profile.md`.

## The one gate: `scripts/smoke-all.sh`

```bash
bash scripts/smoke-all.sh
```

Runs every automated suite in order against **throwaway** resources (temp
dirs, ephemeral ports, fresh containers) — never the play/browser rig:

| Step | What | Time |
|---|---|---|
| `cargo test --offline` | unit + contract + integration (live count is in the `cargo test` output; older text said 349/351, not re-counted 2026-10-20) | ~15 s |
| `scripts/smoke-phase1.sh` | core flows via real `misskey-js` (+ restart durability) | ~10 s |
| `scripts/smoke-phase3.sh` | profile REST + streaming via real `misskey-js` `Stream` | ~10 s |
| `scripts/keystone-sweep.sh` | Keystone's exact calls (the script's own `KEYSWEEP` PASS/FAIL summary line is authoritative for the count) | ~10 s |
| `scripts/smoke-phase2.sh` | four-server federation, fresh pg/redis (hermetic: tears down after) | ~15–25 min |

Gate result vocabulary (Batch A, 2026-10-21): `SMOKE_ALL_OK` (exit 0) means
every required phase actually ran and passed; `SMOKE_ALL_PARTIAL` (exit 2)
means a dev-mode run skipped a prerequisite-gated phase and is **not** a
release pass; `SMOKE_ALL_FAIL` (exit 1) means a phase ran and failed, or a
strict-mode run skipped a required phase. `SMOKE_ALL_REQUIRED=1` selects
strict mode for CI/release (missing Docker, Node versions, or
Misskey/Sharkey reference checkouts fail the gate). Each step streams live
and writes its full log to `${SMOKE_ALL_LOGDIR:-/tmp/smoke-all-logs}/<slug>.log`
(cargo also keeps `cargo-full.log`; console shows `tail -30` under
`bash -o pipefail`), so a failing `cargo test` cannot be masked.
The runner's own failure paths are pinned by
`scripts/tests/test-smoke-runner.sh` (11 checks) plus
`scripts/tests/test-smoke-gate-a-fix.sh` (22 checks, per-step logs and
stale-binary gate), run under native Git Bash.

Each step streams its own progress live (`[smoke1]` / `[smoke3]` / `[PASS]`
/ `[interop]` lines); the wrapper adds per-step timing and always ends with
a results table plus exactly one of `SMOKE_ALL_OK` / `SMOKE_ALL_PARTIAL` /
`SMOKE_ALL_FAIL` (per-step full logs under `${SMOKE_ALL_LOGDIR}`).
Stale binary gate: with no `PINETREE_BIN`, a source file newer than the
binary fails strict runs (`SMOKE_FAIL` stale) and rebuilds in dev.
`PINETREE_BIN=...` is explicit opt-in (freshness is yours).
CI (`.github/workflows/smoke.yml`): fast `cargo test --offline` plus strict
`bash scripts/smoke-all.sh` with pinned Misskey/Sharkey checkouts built from
source; logs uploaded as `smoke-logs` always.
Do not treat `SMOKE_ALL_PARTIAL` as a release pass.

Environment:

- `SMOKE_ALL_SKIP_PHASE2=1` — skip the slow federation step.
- `SHARKEY_NODE_BIN=<dir with a Node 22 `node`>` — required for the
  federation step (default `node` must be 24.x for Misskey's `re2`).
- `PINETREE_BIN=...` / `BUN_BIN=...` — override auto-detected binaries.
- `PINETREE_ELIXIR_INTEROP=1` — runs `tests/federation_elixir.rs::elixir_quote_federation` (Pinetree↔Pleroma↔Akkoma triangle: baseline posts/replies/renotes/media/follows + quote interop both directions; needs the `local-federation` rig + `pinetree.test` node up, otherwise skips).
- `PINETREE_ELIXIR_STRESS=1` — runs `tests/federation_elixir.rs::elixir_quote_stress` (sustained quote+post fan-out with follow churn, then convergence + no-duplicate assertions; same rig, throwaway users/ports/DBs). The stress matrix now includes a reply burst with exact-match convergence.
- `PINETREE_REPLY_REPAIR_LIMIT=<n>` — bounds the opt-in unresolved-reply-parent repair at startup (`repair_unresolved_reply_parents`, clamped 1..=5000); off by default (unset = no startup repair).

The federation step reports `SKIP` (not failure) when docker, Node 24,
Node 22, or the built `./misskey` + `./sharkey` checkouts are absent:

```bash
# checkouts (git-ignored symlinks to native-Linux paths under WSL):
./misskey -> Misskey 2026.10.1-alpha.0 (built: pnpm build-pre + backend build)
./sharkey -> Sharkey 2025.4.7 (Node 22: pnpm install + build-pre + backend build)
```

`cargo test` notes: needs `./misskey`/`./sharkey` re-linked first
(packer snapshot + `client_profile` tests skip without them);
`target-dir` is redirected to ext4 via `.cargo/config.toml`
(git-ignored, machine-local).

## Browser topologies (need eyes, not automation)

All passwords are `password123`. From Windows, reach the guest at its WSL
IP (currently `172.28.54.228`; changes on WSL restart — mirrored
networking in `.wslconfig` is the durable fix) with one
`netsh interface portproxy` + firewall rule **per port**.

| Combo | Frontend | Backend | Users |
|---|---|---|---|
| pinetree+keystone | Keystone vite `:5173` (default mode → `:3000`) | pinetree pt5 `:3000` (`--public-url=http://127.0.1.5 --data-dir=<pt5>`) | `ks1` |
| pinetree+misskey | Misskey UI via mk-proxy `:8182` (see below) | pinetree `:3103` | `pt3` |
| misskey+keystone | Keystone vite `:5176` (`--mode misskey` → `:3101`) | Misskey `:3101` | `mk1` |
| sharkey+keystone | Keystone vite `:5177` (`--mode sharkey` → `:3102`) | Sharkey `:3102` | `sk1` |
| sharkey-native | Sharkey's own UI at `:3102` (shell + `/vite` → dev `:5174`) | Sharkey `:3102`, restarted with `VITE_PORT=5174 EMBED_VITE_PORT=5175` | `sk1` |
| misskey-native | Misskey's own UI at `:3101` (shell + `/vite` → dev `:5181`) | Misskey `:3101`, restarted with `VITE_PORT=5181 EMBED_VITE_PORT=5182` | `mk1` |

Bring-up (from a fresh `KEEP=1 SHARKEY_NODE_BIN=/usr/sbin bash
scripts/smoke-phase2.sh` → `SMOKE_OK`; work dir `/tmp/pinetree-interop`):

```bash
# pt5 (Keystone's backend): existing data dir keeps ks1; warm its RSA key
PINETREE_ALLOW_PRIVATE_IPS=1 <pinetree-bin> \
  --public-url=http://127.0.1.5 --data-dir=/tmp/pinetree-interop/pt5 \
  --listen=127.0.0.1:3000 &
# keystone vites (Node 24, from /root/keystone/frontend):
node node_modules/.bin/vite --host 0.0.0.0 --port 5173 &
node node_modules/.bin/vite --mode misskey --host 0.0.0.0 --port 5176 --strictPort &
node node_modules/.bin/vite --mode sharkey --host 0.0.0.0 --port 5177 --strictPort &
# pinetree+misskey UI rig (stop Keystone :5173 first — same port):
PROXY_LISTEN=127.0.0.1:8182 PINETREE_UPSTREAM=127.0.0.1:3103 \
  bun /tmp/opencode/mk-proxy.mjs &   # NOT in repo; /tmp/opencode/mk-proxy.mjs was absent at the 2026-10-20 check — recreate or find it before use
# Misskey-native UI: frontend vite :5181 (Node 24, needs packages/i18n build),
# then restart the :3101 backend with VITE_PORT=5181 EMBED_VITE_PORT=5182 (same DB).
# Sharkey-native UI: one-time pnpm --filter frontend-shared build +
# misskey-bubble-game build (Node 22), frontend vite :5174,
# then restart the :3102 backend with VITE_PORT=5174 EMBED_VITE_PORT=5175 (same DB).
```

Post-`SMOKE_OK` checklist before browsing: `admin/update-meta
{"federation":"all"}` is already set by the script; unlock pt3
(`i/update {"isLocked":false}` — the interop test leaves it locked, which
stalls follows); warm each pinetree's RSA key (first signed fetch is slow
in dev builds and peers may time out).

## Troubleshooting (learned bringing this up)

- Misskey/Sharkey federation defaults to `none` — set `all` or nothing
  federates. Both reject `http://` actor/note/url fields unless
  `NODE_ENV=development` (+ `MISSKEY_WEBFINGER_USE_HTTP=true`).
- Node majors are strict: Misskey `re2` is compiled for Node 24,
  Sharkey for Node 22 (`SHARKEY_NODE_BIN`, `MK_DISABLE_CLUSTERING=1`).
- Sharkey caches followers in memory (~30 min): after follow churn,
  restart its backend or delivery stalls (upstream quirk, not pinetree).
- Misskey reuses Follow/Undo ids per pair — dedup forgets them after
  `Undo(Follow)` (handled in `src/federation`, do not "fix").
- Actor RSA keys generate lazily (slow in dev) — warm before interop.
- Media/avatar URLs embed `127.0.1.x` (federation identity): unreachable
  from Windows, so remote thumbnails won't render off-box; text interop
  is unaffected. `127.0.1.x:80` itself is guest-only (loopback proxy).
- `scripts/smoke-phase2.sh stop` kills the interop backends (+ compose);
  vites, pt5, and the `:8182` rig are managed separately (plain PIDs).
- Bench only against a throwaway pinetree (`scripts/bench-local.py`
  on `:3210`), never the play stack.

## Known gaps (open, verified 2026-10-20 by reading the script text)

Items 1–4 were fixed 2026-10-21 (Batch A): pipefail cargo step with full
log, strict `SMOKE_ALL_REQUIRED=1` mode, `SMOKE_ALL_PARTIAL` for dev skips,
guarded steps so the table + sentinel always print, fresh-build binary
resolution, run-metadata header. Historical notes retained below for the
record; the `scripts/tests/test-smoke-runner.sh` harness (11 checks) pins
the new behavior.

1. ~~**Step 1 masks cargo failure.**~~ Fixed: `bash -o pipefail -c` +
   `/tmp/smoke-cargo-full.log`.
2. ~~**SKIP prints `SMOKE_ALL_OK`.**~~ Fixed: dev skip ⇒
   `SMOKE_ALL_PARTIAL` (exit 2); strict skip ⇒ `SMOKE_ALL_FAIL` (exit 1).
3. ~~**Failure exit is silent.**~~ Fixed: all step calls guarded, table +
   sentinel always print.
4. ~~**Keystone count** differs …~~ Fixed: no static count remains; the
   sweep's own `KEYSWEEP` summary is authoritative.
5. **Env-only knobs** `PINETREE_ELIXIR_INTEROP`, `PINETREE_ELIXIR_STRESS`, and
   `PINETREE_REPLY_REPAIR_LIMIT` (above) are not in the JSON config, which
   `AGENTS.md` forbids for new settings (TODOS P3).
6. **Out-of-repo paths.** `/root/keystone`, `/tmp/opencode/mk-proxy.mjs`, and the
   bench rigs under `/root/bench` are not in the repo; `/tmp/opencode/*` did not
   exist at the 2026-10-20 check.
