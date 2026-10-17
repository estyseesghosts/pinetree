#!/usr/bin/env python3
"""Scratch driver (not in repo): re-run only S2 (reaction storm) from the patched phase-stress.py.

Reuses the S1 markers of tag S260332 (SKIP_S1), so no new posts are created. Writes its own
JSON to logs/s2-rerun.json; /tmp/opencode/bench/stress.json is left untouched.
"""
import sys, os, json, importlib.util
os.environ["SKIP_S1"] = "1"
sys.path.insert(0, "/tmp/opencode/bench")  # lib.py lives here
spec = importlib.util.spec_from_file_location("phase_stress", "/tmp/opencode/bench-rerun/scripts/phase-stress.py")
P = importlib.util.module_from_spec(spec)
spec.loader.exec_module(P)

P.s2()
out = "/tmp/opencode/bench-rerun/logs/s2-rerun.json"
json.dump(P.R["phases"].get("S2_reaction_storm"), open(out, "w"), indent=1)
print("S2_RERUN_DONE", out, flush=True)
