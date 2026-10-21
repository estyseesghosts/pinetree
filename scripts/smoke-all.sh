#!/usr/bin/env bash
# smoke-all.sh — the one complete gate for all pinetree functionality to date.
#
# Runs every automated suite in dependency order against throwaway resources
# (temp dirs, ephemeral ports, fresh containers); it never touches the
# play/browser rig. Each step streams its own progress live; this wrapper
# adds step headers, per-step timing, and a final verdict table.
#
#   1. cargo test --offline ............ unit + contract + integration (fast)
#   2. scripts/smoke-phase1.sh ........ single-node core flows, real misskey-js
#   3. scripts/smoke-phase3.sh ........ profile REST + streaming, real misskey-js Stream
#   4. scripts/keystone-sweep.sh ...... Keystone's exact calls (38 checks)
#   5. scripts/smoke-phase2.sh ........ four-server federation (slow: fresh
#      pg/redis, migrations, Misskey + Sharkey boots; ~15-25 min)
#
# Prints SMOKE_ALL_OK on success, SMOKE_ALL_FAIL on the first failing step.
# Step 5 needs docker, Node 24 on PATH, a Node 22 (SHARKEY_NODE_BIN), and
# built ./misskey + ./sharkey checkouts; when those are absent it reports
# SKIP instead of failing. SMOKE_ALL_SKIP_PHASE2=1 skips it explicitly.
#
# All serve steps pass --no-transcode: lab/test ffmpeg builds (e.g. Fedora)
# lack software h264/hevc decoders, and the startup gate correctly refuses
# to boot with an incomplete toolchain. Production images carry full ffmpeg;
# transcode itself is covered by the gaps_transcode suite (real ffmpeg).
#
# Browser topologies (pinetree+keystone, pinetree+misskey, misskey+keystone,
# sharkey-native) need eyes, not automation: see docs/testing.md.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
declare -a RESULTS=()
START_ALL=$SECONDS

step() { # name, then command...
  local name="$1"; shift
  local start=$SECONDS
  echo "======================================================================"
  echo "[smoke-all] STEP: $name (live output follows)"
  echo "======================================================================"
  # Stream live AND keep the full log: teed output shows each suite's own
  # progress lines as they happen.
  set +e
  "$@" 2>&1 | tee /tmp/smoke-all-step.log
  local step_status=${PIPESTATUS[0]}
  set -e
  local took=$((SECONDS - start))
  if [[ $step_status -eq 0 ]]; then
    echo "[smoke-all] DONE: $name (${took}s)"
    RESULTS+=("PASS  $name (${took}s)")
  else
    echo "[smoke-all] FAIL: $name (${took}s, exit $step_status) — full log at /tmp/smoke-all-step.log"
    RESULTS+=("FAIL  $name (${took}s)")
    return 1
  fi
}

# --- step 1: everything in-process -------------------------------------------
step "cargo test (unit + contract + integration)" bash -c "cd \"$ROOT\" && cargo test --offline 2>&1 | tail -30"

# --- steps 2-4: live throwaway single-node suites -----------------------------
step "smoke-phase1 (core flows, misskey-js)" bash "$ROOT/scripts/smoke-phase1.sh"
step "smoke-phase3 (profile REST + streaming, misskey-js)" bash "$ROOT/scripts/smoke-phase3.sh"
step "keystone-sweep (Keystone calls, 38 checks)" bash "$ROOT/scripts/keystone-sweep.sh"

# --- step 5: four-server federation --------------------------------------------
phase2_skip_reason=""
if [[ -n "${SMOKE_ALL_SKIP_PHASE2:-}" ]]; then
  phase2_skip_reason="SMOKE_ALL_SKIP_PHASE2=1"
elif ! command -v docker >/dev/null 2>&1; then
  phase2_skip_reason="no docker"
elif [[ "$(node --version 2>/dev/null)" != v24.* ]]; then
  phase2_skip_reason="needs Node 24 on PATH (nvm use 24)"
elif [[ -n "${SHARKEY_NODE_BIN:-}" ]] && [[ "$("$SHARKEY_NODE_BIN/node" --version 2>/dev/null)" != v22.* ]]; then
  phase2_skip_reason="SHARKEY_NODE_BIN is not Node 22"
elif [[ ! -f "$ROOT/misskey/packages/backend/built/entry.js" || ! -f "$ROOT/sharkey/packages/backend/built/boot/entry.js" ]]; then
  phase2_skip_reason="./misskey or ./sharkey backend not built"
fi
if [[ -z "$phase2_skip_reason" && -z "${SHARKEY_NODE_BIN:-}" ]] && [[ "$(node --version 2>/dev/null)" == v24.* ]]; then
  # Node 24 default per the handoff; Sharkey still needs its Node 22.
  phase2_skip_reason="set SHARKEY_NODE_BIN=<dir with a Node 22 node>"
fi
if [[ -n "$phase2_skip_reason" ]]; then
  echo "[smoke-all] SKIP: smoke-phase2 (four-server federation) — $phase2_skip_reason"
  RESULTS+=("SKIP  smoke-phase2 (four-server federation) — $phase2_skip_reason")
else
  # Hermetic: no KEEP, so the script tears its rig down on exit.
  step "smoke-phase2 (four-server federation)" bash "$ROOT/scripts/smoke-phase2.sh"
fi

echo "======================================================================"
echo "[smoke-all] RESULTS:"
for r in "${RESULTS[@]}"; do echo "[smoke-all]   $r"; done
if printf '%s\n' "${RESULTS[@]}" | grep -q '^FAIL'; then
  echo "[smoke-all] SMOKE_ALL_FAIL ($((SECONDS - START_ALL))s total)"
  exit 1
fi
echo "[smoke-all] SMOKE_ALL_OK ($((SECONDS - START_ALL))s total)"
