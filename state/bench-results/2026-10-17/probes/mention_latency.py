"""Measure time-to-visibility of a remote mention notification on pinetree (sequential, one at a time).

Posts "@pk1@pt-ks.test EXP|mention|<src>|<dst>|<n>" from a source node, then polls the
destination's i/notifications every 3 s until the marker appears (or 240 s). Read-only on the
destination except for the mention itself. Prints time-to-visible per case.
"""
import sys, json, time
sys.path.insert(0, "/tmp/opencode/bench")
from lib import load_nodes, HOSTS

nodes = load_nodes()
CASES = [("pt-mk", "pt-ks", "pk1"), ("misskey", "pt-ks", "pk1"), ("catodon", "pt-ks", "pk1")]
TIMEOUT = 240
run = str(int(time.time()))[-6:]
for src, dst, uname in CASES:
    S, D = nodes[src], nodes[dst]
    su = S.users[0]
    du = [u for u in D.users if u.name == uname][0]
    marker = f"EXP|mention|{src}|{dst}|{run}"
    t0 = time.time()
    nid = S.post(su, text=f"@{du.name}@{HOSTS[dst]} {marker}")
    seen = None
    while time.time() - t0 < TIMEOUT:
        items = D.notifications(du, limit=40)
        if marker in json.dumps(items):
            seen = time.time() - t0
            break
        time.sleep(3)
    print(f"{src}->{dst} ({uname}) post={nid} visible_after={'%.1fs' % seen if seen is not None else 'NOT within %ds' % TIMEOUT}", flush=True)
