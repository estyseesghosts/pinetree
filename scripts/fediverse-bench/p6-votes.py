import sys, time
sys.path.insert(0, "/tmp/opencode/bench")
from lib import load_nodes
from concurrent.futures import ThreadPoolExecutor
nodes = load_nodes()
mode = sys.argv[1]; pairs = [p.split(">") for p in sys.argv[2:]]
def run(i, src, dst):
    S, D = nodes[src], nodes[dst]
    su, du = S.users[i % 6], D.users[(i + 2) % 6]
    P = S.post(su, text=f"p6 {src}>{dst} {i} {int(time.time())}", poll={"choices": ["alpha", "beta", "gamma"], "expires_in": 3600})
    uri = S.uri_of(P, su)
    local = None
    for _ in range(40):
        v = D.note_by_uri(uri, du)
        if v and v.get("id"): local = v["id"]; break
        time.sleep(2)
    if not local: return (src, dst, i, "unresolved", None)
    s, v = D.poll_vote(du, local, 1)
    t0 = time.time()
    while time.time() - t0 < 150:
        p = S.poll(P, su)
        if p and p["choices"][1]["votes"] >= 1: return (src, dst, i, "ok", round(time.time() - t0, 1))
        time.sleep(1)
    return (src, dst, i, f"NO VOTE (vote status {s})", None)
jobs = [(i, a, b) for i in range(int(mode)) for a, b in pairs]
with ThreadPoolExecutor(max_workers=8) as ex:
    for r in ex.map(lambda j: run(*j), jobs): print(r, flush=True)
