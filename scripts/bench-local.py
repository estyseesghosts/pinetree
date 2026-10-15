#!/usr/bin/env python3
"""Single-node pinetree bench: notes/create, timeline reads, users/show.
Stays under per-account rate limits (notes/create 300/h) by rotating users.
Usage: python3 scripts/bench-local.py [--base http://127.0.0.1:3210] [--creates 400] [--reads 1000]
"""
import json, sys, time, statistics, urllib.request

def post(base, ep, body, token=None):
    req = urllib.request.Request(f"{base}/api/{ep}",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json", **({"authorization": f"Bearer {token}"} if token else {})},
        method="POST")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            out = json.loads(r.read() or b"null")
            return r.status, out, (time.perf_counter() - t0) * 1000
    except urllib.error.HTTPError as e:
        try: out = json.loads(e.read() or b"null")
        except Exception: out = {}
        return e.code, out, (time.perf_counter() - t0) * 1000

def pct(lat, p):
    s = sorted(lat)
    return s[min(len(s) - 1, int(p / 100 * len(s)))]

def main():
    base = sys.argv[sys.argv.index("--base") + 1] if "--base" in sys.argv else "http://127.0.0.1:3210"
    n_create = int(sys.argv[sys.argv.index("--creates") + 1]) if "--creates" in sys.argv else 400
    n_read = int(sys.argv[sys.argv.index("--reads") + 1]) if "--reads" in sys.argv else 1000
    # setup users (rotate to stay under 300/h/user)
    n_users = max(1, (n_create + 250) // 250)
    toks = []
    for i in range(n_users):
        u = f"bench{i}"
        s, v, _ = post(base, "signup", {"username": u, "password": "password123"})
        if s != 200:
            s, v, _ = post(base, "signin", {"username": u, "password": "password123"})
            assert s == 200, f"signin {u}: {s} {v}"
            toks.append(v["token"])
        else:
            toks.append(v["token"])
    tok0 = toks[0]
    # warmup
    post(base, "notes/create", {"text": "warmup", "visibility": "public"}, tok0)
    # creates
    lat = []
    t0 = time.perf_counter()
    fails = 0
    for i in range(n_create):
        tok = toks[i % len(toks)]
        s, v, ms = post(base, "notes/create", {"text": f"bench note {i} " + "x" * 40, "visibility": "public"}, tok)
        if s != 200:
            fails += 1
        lat.append(ms)
    wall = time.perf_counter() - t0
    print(f"notes/create: n={n_create} users={n_users} fails={fails} wall={wall:.2f}s "
          f"throughput={n_create/wall:.1f}/s p50={statistics.median(lat):.1f}ms p95={pct(lat,95):.1f}ms p99={pct(lat,99):.1f}ms")
    # timeline reads (no rate bucket)
    lat = []
    t0 = time.perf_counter()
    fails = 0
    for i in range(n_read):
        s, v, ms = post(base, "notes/timeline", {"limit": 20}, tok0)
        if s != 200: fails += 1
        lat.append(ms)
    wall = time.perf_counter() - t0
    print(f"notes/timeline(limit20): n={n_read} fails={fails} wall={wall:.2f}s "
          f"throughput={n_read/wall:.1f}/s p50={statistics.median(lat):.1f}ms p95={pct(lat,95):.1f}ms p99={pct(lat,99):.1f}ms")
    # users/show self (no rate bucket)
    s, v, _ = post(base, "signin", {"username": "bench0", "password": "password123"})
    uid = v["id"]
    lat = []
    t0 = time.perf_counter()
    fails = 0
    for i in range(n_read):
        s, vv, ms = post(base, "users/show", {"userId": uid}, tok0)
        if s != 200: fails += 1
        lat.append(ms)
    wall = time.perf_counter() - t0
    print(f"users/show: n={n_read} fails={fails} wall={wall:.2f}s "
          f"throughput={n_read/wall:.1f}/s p50={statistics.median(lat):.1f}ms p95={pct(lat,95):.1f}ms p99={pct(lat,99):.1f}ms")

if __name__ == "__main__":
    import urllib.error
    main()
