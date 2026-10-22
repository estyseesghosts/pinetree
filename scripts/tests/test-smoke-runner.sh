#!/usr/bin/env bash
# test-smoke-runner.sh — shell-level regression tests for the smoke gate
# itself (prod-ready.md Batch A §A3). Proves, with a fake `cargo` shim on
# PATH and stubbed sandboxes (no docker/node/live servers needed):
#   1. a fake failing `cargo` + succeeding `tail` still fails the step
#      (pipefail), and the full log is preserved to a file;
#   2. a skipped required phase in strict mode (SMOKE_ALL_REQUIRED=1) cannot
#      print the SMOKE_ALL_OK sentinel;
#   3. a failing phase records FAIL and the results table still prints;
#   4. a developer-mode skip prints SMOKE_ALL_PARTIAL (exit 2), never OK;
#   5. the reported Keystone sweep count matches the actual checks and no
#      stale hardcoded count remains in smoke-all.sh.
#
# Runnable under Git Bash: needs only bash, grep/sed/awk/mktemp, and python3
# (or `python` as fallback — only to mirror the scripts' own interpreter
# lookup, not for sandbox rewriting).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SMOKE_ALL="$ROOT/scripts/smoke-all.sh"
SWEEP="$ROOT/scripts/keystone-sweep.sh"

PASS_N=0
FAIL_N=0
pass() { PASS_N=$((PASS_N + 1)); echo "[PASS] $1"; }
fail() { FAIL_N=$((FAIL_N + 1)); echo "[FAIL] $1"; }

T="$(mktemp -d "${TMPDIR:-/tmp}/smoke-runner-test-XXXXXX")"
trap 'rm -rf "$T"' EXIT

echo "== test-smoke-runner: ROOT=$ROOT"

# --- (1) pipefail: fake failing cargo + succeeding tail must fail ------------
SAVED_PATH="$PATH"
mkdir -p "$T/bin"
cat >"$T/bin/cargo" <<'SHIM_EOF'
#!/usr/bin/env bash
echo "fake-cargo-marker: test result: FAILED. 0 passed; 1 failed" >&2
echo "fake-cargo-marker: test result: FAILED. 0 passed; 1 failed"
exit 1
SHIM_EOF
chmod +x "$T/bin/cargo"
export PATH="$T/bin:$PATH"

set +e
bash -c 'cargo test --offline 2>&1 | tail -5' >"$T/legacy.out" 2>&1
legacy_code=$?
bash -o pipefail -c 'cargo test --offline 2>&1 | tail -5' >"$T/fixed.out" 2>&1
fixed_code=$?
bash -o pipefail -c 'cargo test --offline 2>&1 | tee "$0" | tail -5' "$T/cargo-full.log" >"$T/pattern.out" 2>&1
pattern_code=$?
set -e

if [[ $legacy_code -eq 0 ]]; then
  pass "(1a) legacy 'bash -c ... | tail' masks cargo failure (exit 0 as expected of the bug)"
else
  fail "(1a) legacy pipeline unexpectedly exited $legacy_code (expected 0 — bug shape changed?)"
fi
if [[ $fixed_code -ne 0 ]]; then
  pass "(1b) 'bash -o pipefail -c ... | tail' propagates cargo failure (exit $fixed_code)"
else
  fail "(1b) pipefail pipeline still masks cargo failure (exit 0)"
fi
if grep -q 'bash -o pipefail -c' "$SMOKE_ALL"; then
  pass "(1c) smoke-all.sh cargo step uses 'bash -o pipefail -c'"
else
  fail "(1c) smoke-all.sh cargo step lacks '-o pipefail'"
fi
if [[ $pattern_code -ne 0 ]] && grep -q 'fake-cargo-marker' "$T/cargo-full.log" 2>/dev/null; then
  pass "(1d) full cargo output preserved to log file while console stays tailed (exit $pattern_code)"
else
  fail "(1d) full-log preservation broken (exit $pattern_code, log present: $(test -f "$T/cargo-full.log" && echo yes || echo no))"
fi
export PATH="$SAVED_PATH"

# --- sandboxes: instant stubs for the live steps, real skip/verdict logic ----
make_sandbox() { # variant(pass|fail) outfile
  local variant="$1" out="$2"
  if [[ "$variant" == "fail" ]]; then
    # Keep the `|| true` guard the real script uses: a bare failing `step`
    # under `set -e` would exit before the results table (the bug in §A2).
    awk '{ if ($0 ~ /^ *step "/) { n++; if (n == 1) { sub(/^ *step "[^"]*".*$/, "step \"stub-fail\" false || true") } else { sub(/^ *step "[^"]*".*$/, "step \"stub-pass\" true || true") } } print }' \
      "$SMOKE_ALL" >"$out"
  else
    sed -E 's/^ *step "[^"]*".*$/step "stub-pass" true || true/' "$SMOKE_ALL" >"$out"
  fi
  chmod +x "$out"
}
SANDBOX_PASS="$T/sandbox-pass.sh"
SANDBOX_FAIL="$T/sandbox-fail.sh"
make_sandbox pass "$SANDBOX_PASS"
make_sandbox fail "$SANDBOX_FAIL"

# --- (4) developer skip -> PARTIAL (exit 2), never OK --------------------------
set +e
SMOKE_ALL_SKIP_PHASE2=1 bash "$SANDBOX_PASS" >"$T/dev-skip.out" 2>&1
dev_code=$?
set -e
if [[ $dev_code -eq 2 ]] && grep -q '\[smoke-all\] SMOKE_ALL_PARTIAL' "$T/dev-skip.out" \
  && ! grep -q '\[smoke-all\] SMOKE_ALL_OK' "$T/dev-skip.out"; then
  pass "(4) dev skip prints SMOKE_ALL_PARTIAL (exit 2), never SMOKE_ALL_OK"
else
  fail "(4) dev skip wrong (exit $dev_code; PARTIAL=$(grep -c 'SMOKE_ALL_PARTIAL' "$T/dev-skip.out"), OK=$(grep -c '\[smoke-all\] SMOKE_ALL_OK' "$T/dev-skip.out"))"
fi

# --- (2) strict skip -> FAIL (exit 1), never OK --------------------------------
set +e
SMOKE_ALL_REQUIRED=1 SMOKE_ALL_SKIP_PHASE2=1 bash "$SANDBOX_PASS" >"$T/strict-skip.out" 2>&1
strict_code=$?
set -e
if [[ $strict_code -eq 1 ]] && grep -q '\[smoke-all\] SMOKE_ALL_FAIL' "$T/strict-skip.out" \
  && ! grep -q '\[smoke-all\] SMOKE_ALL_OK' "$T/strict-skip.out"; then
  pass "(2) strict skip prints SMOKE_ALL_FAIL (exit 1), never SMOKE_ALL_OK"
else
  fail "(2) strict skip wrong (exit $strict_code; FAIL=$(grep -c 'SMOKE_ALL_FAIL' "$T/strict-skip.out"), OK=$(grep -c '\[smoke-all\] SMOKE_ALL_OK' "$T/strict-skip.out"))"
fi

# --- (3) failing phase records FAIL; table + sentinel still print -------------
set +e
SMOKE_ALL_SKIP_PHASE2=1 bash "$SANDBOX_FAIL" >"$T/phase-fail.out" 2>&1
fail_code=$?
set -e
if [[ $fail_code -eq 1 ]] && grep -q '^ *\[smoke-all\] *FAIL' "$T/phase-fail.out" \
  && grep -q '\[smoke-all\] RESULTS:' "$T/phase-fail.out" \
  && grep -q '\[smoke-all\] SMOKE_ALL_FAIL' "$T/phase-fail.out"; then
  pass "(3a) failing phase records FAIL; results table + SMOKE_ALL_FAIL still print (exit 1)"
else
  fail "(3a) failing phase mishandled (exit $fail_code; see $T/phase-fail.out)"
fi
# Every live `step` invocation in the real script must be guarded so a failure
# reaches the table instead of exiting early under `set -e`.
unguarded="$(grep -E '^ *step "' "$SMOKE_ALL" | grep -vc '|| true' || true)"
if [[ "$unguarded" -eq 0 ]]; then
  pass "(3b) all step invocations in smoke-all.sh are guarded (results always print)"
else
  fail "(3b) $unguarded unguarded step invocation(s) in smoke-all.sh"
fi

# --- (5) sweep count matches; no stale hardcoded count -------------------------
named_checks="$(grep -c 'check("' "$SWEEP")"
wait_checks="$(grep -c 'wait_for("' "$SWEEP")"
total_checks=$((named_checks + wait_checks))
echo "[info] keystone-sweep.sh: $named_checks check(\"...) + $wait_checks wait_for(\"...) = $total_checks checks"
if [[ $total_checks -gt 0 && $total_checks -ne 38 ]]; then
  pass "(5a) sweep defines $total_checks checks (stale '38' would be wrong)"
else
  fail "(5a) sweep check count unexpected ($total_checks)"
fi
if ! grep -q '38 checks' "$SMOKE_ALL" && ! grep -q '38 checks' "$SWEEP"; then
  pass "(5b) no stale '38 checks' string in smoke-all.sh / keystone-sweep.sh"
else
  fail "(5b) stale '38 checks' string still present"
fi
if grep -q 'KEYSWEEP summary' "$SMOKE_ALL" && grep -q 'KEYSWEEP:' "$SWEEP"; then
  pass "(5c) smoke-all.sh defers to the sweep's own dynamic KEYSWEEP summary"
else
  fail "(5c) dynamic KEYSWEEP summary reference missing"
fi

echo
echo "RUNNER_TESTS: $PASS_N passed, $FAIL_N failed"
[[ $FAIL_N -eq 0 ]]
