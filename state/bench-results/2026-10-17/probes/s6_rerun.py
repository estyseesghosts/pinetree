#!/usr/bin/env python3
"""Scratch driver (not in repo): re-run only S6 (churn) under debug logging.

Uses a NEW tag so the new churn markers cannot be confused with the residue from the
original S6 (tag S260332). Writes logs/s6-rerun.json; /tmp/opencode/bench/stress.json is untouched.
"""
import sys, os, json, time, importlib.util
sys.path.insert(0, "/tmp/opencode/bench")  # lib.py lives here
spec = importlib.util.spec_from_file_location("phase_stress", "/tmp/opencode/bench-rerun/scripts/phase-stress.py")
P = importlib.util.module_from_spec(spec)
spec.loader.exec_module(P)

P.TAG = "S6R" + str(int(time.time()))[-6:]
print("s6 tag", P.TAG, flush=True)
P.s6()
out = "/tmp/opencode/bench-rerun/logs/s6-rerun.json"
json.dump({"tag": P.TAG, "s6": P.R["phases"].get("S6_churn")}, open(out, "w"), indent=1)
print("S6_RERUN_DONE", out, flush=True)
