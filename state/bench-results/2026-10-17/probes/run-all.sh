#!/usr/bin/env bash
# Sequential bench rerun (never concurrent): probe matrix -> feature matrix -> stress.
# Scripts are scratch copies; they read/write /tmp/opencode/bench/ (lib.py, accounts.json, outputs).
set -u
D=/tmp/opencode/bench-rerun
L=$D/logs
mkdir -p "$L"
cd "$D/scripts" || exit 2
echo "RUN_START $(date -u +%FT%TZ)" | tee "$L/run.log"
python3 probe-matrix.py > "$L/probe-matrix.log" 2>&1
echo "probe-matrix exit=$? end=$(date -u +%FT%TZ)" | tee -a "$L/run.log"
python3 phase-features.py > "$L/phase-features.log" 2>&1
echo "phase-features exit=$? end=$(date -u +%FT%TZ)" | tee -a "$L/run.log"
python3 phase-stress.py > "$L/phase-stress.log" 2>&1
echo "phase-stress exit=$? end=$(date -u +%FT%TZ)" | tee -a "$L/run.log"
echo "RUN_END $(date -u +%FT%TZ)" | tee -a "$L/run.log"
