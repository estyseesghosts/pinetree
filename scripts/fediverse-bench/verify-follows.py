#!/usr/bin/env python3
"""Re-check follow convergence without re-following: for every local user, are all other nodes' users
among its followers? Same check as the end of phase-follows.py. Prints, per node pair, how many
`follower -> followee` edges are missing, so the direction is unambiguous.
Writes $BENCH_DIR/verify-follows.json. Exit 1 if any edge is missing.
"""
import json, os, sys, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import load_nodes, HOSTS, BENCH_DIR

nodes = load_nodes()
missing = collections.Counter()   # (follower node, followee node) -> missing edges
total = 0
for src, node in nodes.items():       # src = the node whose users are being followed
    for u in node.users:
        uid = node.local_id(u)
        followers = node.followers_accts(uid, node.users[0])
        for dn, peer in nodes.items():
            if dn == src: continue
            for pu in peer.users:
                total += 1
                if pu.acct not in followers: missing[(dn, src)] += 1
order = list(nodes)
print("missing edges, follower node (rows) -> followee node (cols); each cell is out of 100 (alice on mastodon adds 10)")
print(f"{'':9}" + " ".join(f"{d:>8}" for d in order))
for f in order:
    print(f"{f:9}" + " ".join(f"{('-' if f == d else missing.get((f, d), 0)):>8}" for d in order))
print(f"{sum(missing.values())} missing of {total} edges")
json.dump({f"{a}->{b}": n for (a, b), n in sorted(missing.items())}, open(os.path.join(BENCH_DIR, "verify-follows.json"), "w"), indent=1)
sys.exit(1 if missing else 0)
