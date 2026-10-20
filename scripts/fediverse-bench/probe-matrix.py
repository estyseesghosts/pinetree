#!/usr/bin/env python3
"""Reachability + per-post federation proof.

R1: one probe post per node (user 0 of each). R2: every ordered instance pair (A,B):
resolve A's probes on B via AP deref (note_by_uri forces a federated fetch).
R3: home-timeline census on every node: markers per source instance.
"""
import json, sys, time
from concurrent.futures import ThreadPoolExecutor
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import load_nodes, BENCH_DIR

nodes = load_nodes()
# Filter to nodes that have accounts (reference impls may not have tokens)
nodes = {k: v for k, v in nodes.items() if len(v.users) > 0}
TAG = "PR" + str(int(time.time()))[-5:]

# R1: probes
probes = {}   # (src, user) -> (note_id, uri)
def post_probe(src, u, i):
    marker = f"PROBE|{src}|{u.name}|{i}|{TAG}"
    nid = nodes[src].post(u, text=marker)
    return (src, u.name, nid, nodes[src].uri_of(nid, u), marker)
jobs = [(n, u, i) for n, N in nodes.items() for u in N.users for i in range(1)]
t0 = time.time()
with ThreadPoolExecutor(max_workers=8) as ex:
    for out in ex.map(lambda j: post_probe(*j), jobs):
        probes[(out[0], out[1])] = (out[2], out[3], out[4])
print(f"R1: {len(probes)} probes posted in {time.time()-t0:.0f}s")

# R2: resolve each probe on every other node
def check(pair_key):
    (src, uname), (dst,) = pair_key
    nid, uri, marker = probes[(src, uname)]
    du = nodes[dst].users[0]
    deadline = time.time() + 60
    while time.time() < deadline:
        v = nodes[dst].note_by_uri(uri, du)
        if v:
            txt = v.get("text") if nodes[dst].family else (v.get("content") or "")
            return (src, dst, "ok" if marker in (txt or "") else f"text-mismatch:{(txt or '')[:60]}")
        time.sleep(4)
    return (src, dst, "UNRESOLVED")
pairs = [((s, u), (d,)) for (s, u) in probes for d in nodes if d != s]
t0 = time.time()
with ThreadPoolExecutor(max_workers=6) as ex:
    results = list(ex.map(check, pairs))
fails = [r for r in results if r[2] != "ok"]
print(f"R2: {len(results)} pair-checks in {time.time()-t0:.0f}s, {len(fails)} failures")
for f in fails[:30]: print("  FAIL", f)
# matrix
print("\n=== PROBE MATRIX (rows=src, cols=dst, ok/FAIL) ===")
print(f"{'src':9}", " ".join(f"{d[:6]:^7}" for d in nodes))
for s in nodes:
    row = []
    for d in nodes:
        if d == s: row.append("  -  ")
        else:
            m = [r for r in results if r[0] == s and r[1] == d]
            row.append("  ok  " if m and m[0][2] == "ok" else " FAIL ")
    print(f"{s:9}", " ".join(f"{x:^7}" for x in row))

# R3: home census (user#1 of each node, deep pages, count markers per src)
print("\n=== HOME FEED CENSUS (markers per source, user#1, 32 pages) ===")
def census(dst):
    node = nodes[dst]; u = node.users[0]
    texts = []
    if node.family:
        until = None
        for _ in range(32):
            body = {"limit": 30}
            if until: body["untilId"] = until
            s, v = node.api("notes/timeline", body, u)
            if s != 200 or not v: break
            texts += [x.get("text") or "" for x in v]; until = v[-1]["id"]
    else:
        mx = None
        for _ in range(32):
            q = "/timelines/home?limit=30" + (f"&max_id={mx}" if mx else "")
            s, v = node.mapi("GET", q, user=u)
            if s != 200 or not v: break
            texts += [x.get("content") or "" for x in v]; mx = v[-1]["id"]
    per = {}
    for t in texts:
        for s2 in nodes:
            if f"|{s2}|" in t or t.count(f"PROBE|{s2}|") or f"PERF|{s2}|" in t or f"FTR|.*|{s2}|" in t:
                per[s2] = per.get(s2, 0) + 1
    return dst, len(texts), per
with ThreadPoolExecutor(max_workers=5) as ex:
    for dst, total, per in ex.map(census, nodes):
        print(f"{dst:9} items={total:4}  " + "  ".join(f"{k}:{v}" for k, v in sorted(per.items())))

json.dump({"probe_fails": fails, "pair_results": results},
          open(os.path.join(BENCH_DIR, "probes.json"), "w"), indent=1)
print("\nPROBES_DONE" if not fails else f"\nPROBES_DONE_WITH_FAILURES={len(fails)}")
