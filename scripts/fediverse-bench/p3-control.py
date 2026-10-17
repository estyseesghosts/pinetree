import sys, time, json
sys.path.insert(0, "/tmp/opencode/bench")
from lib import load_nodes
nodes = load_nodes()
def total(rs): return sum(len(a) if isinstance(a, list) else int(a or 0) for a in rs.values())
def wait(fn, t):
    t0 = time.time()
    while time.time() - t0 < t:
        if fn(): return round(time.time() - t0, 1)
        time.sleep(1)
    return None
pairs = [a for a in sys.argv[1:]]
for pair in pairs:
    src, dst = pair.split(">")
    S, D = nodes[src], nodes[dst]
    for i in range(3):
        su, du = S.users[i], D.users[i + 2]
        nid = S.post(su, text=f"p3 control {src}>{dst} #{i} {int(time.time())}")
        nid = nid if isinstance(nid, str) else nid.get("id", nid)
        uri = S.note_uri(nid, su)
        local = None
        for _ in range(30):
            v = D.note_by_uri(uri, du)
            if v and v.get("id"): local = v["id"]; break
            time.sleep(2)
        if not local: print(pair, i, "unresolved"); continue
        D.react(du, local)
        r = wait(lambda: total(S.reactions(nid, su)) >= 1, 60)
        D.unreac(du, local)
        u = wait(lambda: total(S.reactions(nid, su)) == 0, 90)
        print(pair, i, "react_s=", r, "undo_s=", u, "remaining=", S.reactions(nid, su), flush=True)
