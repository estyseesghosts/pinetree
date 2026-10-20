#!/usr/bin/env python3
"""Resolve-only connectivity matrix: can every node look up a user (default u9) on every other node?

Read-only apart from the remote-actor records each server creates. Run it before a suite run so that
environment gaps (DNS, TLS trust, routing) are found before they show up as federation failures.
Writes $BENCH_DIR/probe-resolve.json; exits 1 if any pair fails.
"""
import json, os, sys, time
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import load_nodes, HOSTS, BENCH_DIR

USER = os.environ.get("PROBE_USER", "u9")
nodes = load_nodes()
pairs = [(s, d) for s in nodes for d in nodes if s != d]

def one(p):
    s, d = p
    t0 = time.time()
    try:
        nodes[s].resolve(f"{USER}@{HOSTS[d]}", nodes[s].users[0])
        return s, d, "ok", round(time.time() - t0, 2)
    except Exception as e:
        return s, d, str(e)[:200], round(time.time() - t0, 2)

with ThreadPoolExecutor(max_workers=10) as ex:
    res = list(ex.map(one, pairs))
bad = [r for r in res if r[2] != "ok"]
order = list(nodes)
print("src\\dst " + " ".join(f"{d:>8}" for d in order))
for s in order:
    row = {d: ("ok" if r == "ok" else "FAIL") for (ss, d, r, _) in res if ss == s}
    print(f"{s:>7} " + " ".join(f"{row.get(d, '-'):>8}" for d in order))
for s, d, r, t in bad: print(f"FAIL {s} -> {d}: {r}")
json.dump([{"src": s, "dst": d, "result": r, "seconds": t} for s, d, r, t in res], open(os.path.join(BENCH_DIR, "probe-resolve.json"), "w"), indent=1)
print(f"{len(res) - len(bad)}/{len(res)} pairs resolve")
sys.exit(1 if bad else 0)
