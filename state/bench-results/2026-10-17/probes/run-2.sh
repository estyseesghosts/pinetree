#!/usr/bin/env bash
# Rerun 2 (sequential): feature matrix (harness-fixed) -> S4 media (harness-fixed).
set -u
D=/tmp/opencode/bench-rerun
L=$D/logs
cd "$D/scripts" || exit 2
echo "RUN2_START $(date -u +%FT%TZ)" | tee "$L/run2.log"
python3 phase-features.py > "$L/run2-phase-features.log" 2>&1
echo "phase-features exit=$? end=$(date -u +%FT%TZ)" | tee -a "$L/run2.log"
cd "$D" && python3 s4_fixed.py > "$L/run2-s4-fixed.log" 2>&1
echo "s4-fixed exit=$? end=$(date -u +%FT%TZ)" | tee -a "$L/run2.log"
echo "RUN2_END $(date -u +%FT%TZ)" | tee -a "$L/run2.log"
