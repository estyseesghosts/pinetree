#!/usr/bin/env bash
# test-smoke-gate-a-fix.sh — Batch A gate fixes (unique file; does not edit
# test-smoke-runner.sh). Proves with sandboxes + stubs (no docker/live):
#   A1. per-step logs: step() tees to $LOGDIR/<slug>.log, cargo step keeps
#       full log ($LOGDIR/cargo-full.log) while console stays tailed, and
#       the FAIL line points at the per-step log.
#   A2. stale binary: strict (SMOKE_ALL_REQUIRED=1) fails on stale source
#       (SMOKE_FAIL stale) instead of WARNING-only; dev rebuilds.
#   A3. strict skip != OK (redundant with runner (2), pinned here too).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SMOKE_ALL="$ROOT/scripts/smoke-all.sh"

PASS_N=0
FAIL_N=0
pass() { PASS_N=$((PASS_N + 1)); echo "[PASS] $1"; }
fail() { FAIL_N=$((FAIL_N + 1)); echo "[FAIL] $1"; }

T="$(mktemp -d "${TMPDIR:-/tmp}/smoke-gate-a-fix-XXXXXX")"
trap 'rm -rf "$T"' EXIT

echo "== test-smoke-gate-a-fix: ROOT=$ROOT"

# --- A1 static: per-step log plumbing ---------------------------------------
if grep -q 'SMOKE_ALL_LOGDIR' "$SMOKE_ALL" && grep -q 'slug.log' "$SMOKE_ALL"; then
  pass "(A1a) smoke-all.sh uses SMOKE_ALL_LOGDIR with per-step slug.log"
else
  fail "(A1a) per-step LOGDIR/slug.log plumbing missing in smoke-all.sh"
fi
if grep -q 'cargo-full.log' "$SMOKE_ALL" && grep -q 'bash -o pipefail -c' "$SMOKE_ALL"; then
  pass "(A1b) cargo step keeps full log via bash -o pipefail -c + tee cargo-full.log"
else
  fail "(A1b) cargo full-log pipefail pattern missing"
fi
if grep -q 'tee "$LOGDIR/cargo-full.log" | tail -30' "$SMOKE_ALL" \
  || grep -q 'tee \\"$LOGDIR/cargo-full.log\\" | tail -30' "$SMOKE_ALL"; then
  pass "(A1c) cargo console stays tailed (tee full log | tail -30)"
else
  fail "(A1c) cargo tee|tail-30 shape missing"
fi
if grep -q 'full log at \$log' "$SMOKE_ALL"; then
  pass "(A1d) FAIL line points at the per-step log"
else
  fail "(A1d) FAIL line does not point at per-step log"
fi
if grep -q 'step ".*|| true' "$SMOKE_ALL"; then
  pass "(A1e) || true guard preserved on live steps (table+sentinel always print)"
else
  fail "(A1e) || true guard missing"
fi

# --- A1 dynamic: sandbox run leaves per-step logs -----------------------------
SANDBOX_A1="$T/sandbox-a1.sh"
# Number the stubs so each step gets a distinct slug.log.
awk '{ if ($0 ~ /^ *step "/) { n++; sub(/^ *step "[^"]*".*$/, "step \"stub-pass-" n "\" true || true") } print }' \
  "$SMOKE_ALL" >"$SANDBOX_A1"
chmod +x "$SANDBOX_A1"
export SMOKE_ALL_LOGDIR="$T/logs-a1"
export SMOKE_ALL_SKIP_PHASE2=1
set +e
bash "$SANDBOX_A1" >"$T/a1.out" 2>&1
a1_code=$?
set -e
unset SMOKE_ALL_SKIP_PHASE2
n_logs="$(ls "$SMOKE_ALL_LOGDIR"/*.log 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$n_logs" -ge 2 ]]; then
  pass "(A1f) sandbox run preserved $n_logs per-step logs under SMOKE_ALL_LOGDIR (exit $a1_code)"
else
  fail "(A1f) per-step logs not preserved (found $n_logs, exit $a1_code; see $T/a1.out)"
fi
if grep -q 'full log:' "$T/a1.out" || ls "$SMOKE_ALL_LOGDIR" | grep -q 'stub-pass'; then
  pass "(A1g) per-step log path advertised/present (stub-pass slugs)"
else
  fail "(A1g) stub-pass slug logs missing in $SMOKE_ALL_LOGDIR"
fi

# --- A2 static: stale gate in all 4 live scripts ------------------------------
for s in smoke-phase1.sh smoke-phase3.sh keystone-sweep.sh smoke-phase2.sh; do
  f="$ROOT/scripts/$s"
  if grep -q 'SMOKE_ALL_REQUIRED' "$f" && grep -q 'stale binary' "$f" \
    && grep -q 'cargo build --offline' "$f"; then
    pass "(A2-static) $s has strict stale-binary gate (fail in strict, rebuild in dev)"
  else
    fail "(A2-static) $s missing strict stale-binary gate"
  fi
  if grep -q 'WARNING: binary' "$f"; then
    fail "(A2-static) $s still has WARNING-only stale check"
  else
    pass "(A2-static) $s WARNING-only stale check removed"
  fi
done
# PINETREE_BIN bypass stays explicit opt-in (documented, check skipped).
for s in smoke-phase1.sh smoke-phase3.sh keystone-sweep.sh smoke-phase2.sh; do
  f="$ROOT/scripts/$s"
  if grep -q 'PINETREE_BIN bypass is explicit opt-in' "$f"; then
    pass "(A2-doc) $s documents PINETREE_BIN bypass as explicit opt-in"
  else
    fail "(A2-doc) $s missing PINETREE_BIN opt-in doc comment"
  fi
done

# --- A2 dynamic: strict fails, dev rebuilds -----------------------------------
# Replicate the exact gate block with a controlled ROOT/BIN/cargo stub.
run_stale_harness() { # strict_flag(0|1) -> exit code; uses $T/{bin,src}
  local strict="$1"
  mkdir -p "$T/stale/src"
  printf '#!/usr/bin/env bash\necho stub-bin\n' >"$T/stale-bin"
  chmod +x "$T/stale-bin"
  # BIN older than the source file.
  touch -d '2020-01-01 00:00:00' "$T/stale-bin"
  echo '// newer' >"$T/stale/src/new.rs"
  touch -d '2025-01-01 00:00:00' "$T/stale/src/new.rs"
  mkdir -p "$T/stale-stubbin"
  cat >"$T/stale-stubbin/cargo" <<'EOF'
#!/usr/bin/env bash
echo "stub cargo build $*" >>"$0.log"
exit 0
EOF
  chmod +x "$T/stale-stubbin/cargo"
  # Harness mirrors the live-script gate verbatim (BIN missing -> build;
  # stale + strict -> SMOKE_FAIL; stale + dev -> rebuild).
  cat >"$T/stale-harness.sh" <<'HARNESS'
#!/usr/bin/env bash
set -euo pipefail
BIN="$1"
FAKEROOT="$2"
export PATH="$3:$PATH"
if [[ ! -x "$BIN" ]]; then
  (cd "$FAKEROOT" && cargo build --offline) || { echo "SMOKE_FAIL: cargo build failed" >&2; exit 1; }
fi
[[ -x "$BIN" ]] || { echo "SMOKE_FAIL: binary missing at $BIN" >&2; exit 1; }
if [[ -z "${PINETREE_BIN:-}" ]] && newer="$(find "$FAKEROOT/src" -name '*.rs' -newer "$BIN" -print 2>/dev/null | head -1)" && [[ -n "$newer" ]]; then
  if [[ -n "${SMOKE_ALL_REQUIRED:-}" ]]; then
    echo "SMOKE_FAIL: stale binary $BIN predates $newer (strict mode; rebuild to test the current tree)" >&2
    exit 1
  fi
  echo "[smoke] rebuilding stale binary ($newer newer than $BIN)..." >&2
  (cd "$FAKEROOT" && cargo build --offline) || { echo "SMOKE_FAIL: cargo build failed" >&2; exit 1; }
fi
echo HARNESS_OK
HARNESS
  chmod +x "$T/stale-harness.sh"
  rm -f "$T/stale-stubbin/cargo.log"
  set +e
  if [[ "$strict" == "1" ]]; then
    SMOKE_ALL_REQUIRED=1 bash "$T/stale-harness.sh" "$T/stale-bin" "$T/stale" "$T/stale-stubbin" >"$T/stale-$strict.out" 2>&1
  else
    env -u SMOKE_ALL_REQUIRED bash "$T/stale-harness.sh" "$T/stale-bin" "$T/stale" "$T/stale-stubbin" >"$T/stale-$strict.out" 2>&1
  fi
  echo $?
  set -e
}
strict_code="$(run_stale_harness 1)"
if [[ "$strict_code" -eq 1 ]] && grep -q 'SMOKE_FAIL: stale binary' "$T/stale-1.out"; then
  pass "(A2a) stale binary in strict mode fails (exit 1, SMOKE_FAIL stale)"
else
  fail "(A2a) strict stale gate wrong (exit $strict_code; see $T/stale-1.out)"
fi
dev_code="$(run_stale_harness 0)"
if [[ "$dev_code" -eq 0 ]] && grep -q 'HARNESS_OK' "$T/stale-0.out"; then
  pass "(A2b) stale binary in dev mode rebuilds and proceeds (exit 0)"
else
  fail "(A2b) dev rebuild wrong (exit $dev_code; see $T/stale-0.out)"
fi

# --- A3: strict skip != OK ------------------------------------------------------
SANDBOX_PASS="$T/sandbox-pass.sh"
sed -E 's/^ *step "[^"]*".*$/step "stub-pass" true || true/' "$SMOKE_ALL" >"$SANDBOX_PASS"
chmod +x "$SANDBOX_PASS"
set +e
SMOKE_ALL_REQUIRED=1 SMOKE_ALL_SKIP_PHASE2=1 SMOKE_ALL_LOGDIR="$T/logs-a3" bash "$SANDBOX_PASS" >"$T/strict-skip.out" 2>&1
strict_skip_code=$?
set -e
if [[ $strict_skip_code -eq 1 ]] && grep -q '\[smoke-all\] SMOKE_ALL_FAIL' "$T/strict-skip.out" \
  && ! grep -q '\[smoke-all\] SMOKE_ALL_OK' "$T/strict-skip.out"; then
  pass "(A3) strict skip prints SMOKE_ALL_FAIL (exit 1), never SMOKE_ALL_OK"
else
  fail "(A3) strict skip wrong (exit $strict_skip_code)"
fi

echo
echo "GATE_A_FIX_TESTS: $PASS_N passed, $FAIL_N failed"
[[ $FAIL_N -eq 0 ]]
