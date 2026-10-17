#!/usr/bin/env python3
"""Phase A: full-mesh follows across all 30 accounts (30x29=870 edges) + verify.

Warms pinetree RSA keys first (lazy generation is slow in dev builds and can
time out a peer's first signed fetch). Verifies each user's follower set.
"""
import json, sys, time, urllib.request
sys.path.insert(0, "/tmp/opencode/bench")
from lib import load_nodes, HOSTS, fetch_bytes

nodes = load_nodes()
assert len(nodes) == 5, nodes.keys()

# 1. warm RSA keys on both pinetree nodes (AP fetch of every local actor)
for n in ("pt-ks", "pt-mk"):
    node = nodes[n]
    for u in node.users:
        uid = node.local_id(u)
        st, body = fetch_bytes(f"http://{HOSTS[n]}/users/{uid}", timeout=60)
        print(f"warm {n}/{u.name}: {st}", flush=True)

# 2. full mesh follows
t0 = time.time()
total = errs = 0
for src, node in nodes.items():
    for u in node.users:
        for dst, peer in nodes.items():
            if dst == src: continue
            for pu in peer.users:
                s, v = node.follow(u, pu.acct)
                total += 1
                if s not in (200, 409):  # 409-ish = already following
                    errs += 1
                    print(f"FOLLOW_FAIL {src}/{u.name} -> {pu.acct}: {s} {v}", flush=True)
    print(f"{src}: follows done ({time.time()-t0:.0f}s elapsed, {errs} errors)", flush=True)

# 3. verify convergence: every user's followers == everyone else (29)
deadline = time.time() + 600
missing = {}
while True:
    missing = {}
    for src, node in nodes.items():
        for u in node.users:
            uid = node.local_id(u)
            if node.family:
                s, v = node.api("users/followers", {"userId": uid, "limit": 100}, node.users[0])
                followers = set()
                for f in (v or []):
                    if s != 200: break
                    fu = f.get("follower") if isinstance(f.get("follower"), dict) else f
                    followers.add(f"{fu.get('username')}@{fu.get('host') or HOSTS[src]}")
            else:
                s, v = node.mapi("GET", f"/accounts/{uid}/followers?limit=80", user=node.users[0])
                followers = {f["acct"] for f in v} if s == 200 else set()
            want = {pu.acct for dn, peer in nodes.items() if dn != src for pu in peer.users}
            lack = want - followers
            if lack: missing[f"{src}/{u.name}"] = sorted(lack)
    if not missing or time.time() > deadline:
        break
    print(f"...{sum(len(v) for v in missing.values())} missing edges; waiting 20s", flush=True)
    time.sleep(20)

json.dump({"missing": missing, "total_follows": total, "errors": errs},
          open("/tmp/opencode/bench/follows.json", "w"), indent=1)
if missing:
    print("MISSING EDGES:")
    for k, v in list(missing.items())[:20]: print(" ", k, "->", v)
    sys.exit(1)
print(f"FOLLOWS_OK total={total} errors={errs} elapsed={time.time()-t0:.0f}s")
