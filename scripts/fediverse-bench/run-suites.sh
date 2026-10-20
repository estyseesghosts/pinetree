#!/usr/bin/env bash
# Run the bench suites in plan order, one run directory per invocation.
#
#   run-suites.sh [suite ...]        default: follows features stress concurrent crossfamily interop
#   SUITE_TIMEOUT=3600 run-suites.sh stress
#
# Each suite gets its own log, a hard timeout, and the same BENCH_TRACE file for per-call timing.
# Ctrl-C or SIGTERM kills the whole process group, so no suite outlives the runner. The summary
# lists every suite's exit code and duration; the runner exits non-zero if any suite failed.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
export BENCH_DIR="${BENCH_DIR:?set BENCH_DIR=/root/bench/<run-id>}"
echo $$ > "$BENCH_DIR/run-suites.pid"   # bench-down.sh stops the runner by this PID
RUN="$BENCH_DIR/runs/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN"
export BENCH_TRACE="$RUN/trace.jsonl"
SUITE_TIMEOUT="${SUITE_TIMEOUT:-5400}"
SUITES=("$@"); [ ${#SUITES[@]} -eq 0 ] && SUITES=(follows features stress concurrent crossfamily interop)

cleanup() { trap - INT TERM; echo "runner interrupted: killing process group" >&2; kill -- -$$ 2>/dev/null; exit 130; }
trap cleanup INT TERM
set -m  # each background job below is its own group, so timeout can kill its children

cmd_for() {
  case "$1" in
    interop) echo "bash $HERE/phase6-interop-3node.sh" ;;
    *) echo "python3 -u $HERE/phase-$1.py" ;;
  esac
}

FAILED=0
printf '%-12s %-6s %s\n' suite exit seconds > "$RUN/summary.txt"
for s in "${SUITES[@]}"; do
  t0=$(date +%s)
  echo "[run-suites] start $s $(date -u +%H:%M:%S)"
  timeout --kill-after=30 "$SUITE_TIMEOUT" $(cmd_for "$s") > "$RUN/$s.out" 2>&1
  rc=$?
  [ $rc -eq 124 ] && echo "TIMEOUT after ${SUITE_TIMEOUT}s" >> "$RUN/$s.out"
  [ $rc -ne 0 ] && FAILED=1
  printf '%-12s %-6s %s\n' "$s" "$rc" "$(( $(date +%s) - t0 ))" >> "$RUN/summary.txt"
  echo "[run-suites] done  $s exit=$rc"
done
cat "$RUN/summary.txt"
echo "run dir: $RUN"
exit $FAILED
