#!/usr/bin/env python3
"""Concurrent stress test: 50 parallel posters across all nodes.

Measures throughput, dupe rate, p50/p95/p99 latency for:
- C1: 50 parallel post creates across nodes
- C2: 50 parallel reactions across nodes
- C3: 50 parallel media uploads + posts
- C4: 50 parallel replies across nodes
Results -> $BENCH_DIR/concurrent.json
"""
import json, sys, time, threading, statistics, os
from concurrent.futures import ThreadPoolExecutor
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import load_nodes, HOSTS, fetch_bytes, sha, BENCH_DIR

nodes = load_nodes()
nodes = {k: v for k, v in nodes.items() if len(v.users) > 0}
__import__('lib').run_header('concurrent', nodes)
R = {"phases": {}}
LOCK = threading.Lock()

def note_text(node_key, n):
    return (n.get("text") or "") if nodes[node_key].family else (n.get("content") or "")

def pct(lat, p):
    s = sorted(lat)
    return s[min(len(s) - 1, int(p / 100 * len(s)))] if s else None

def deep_home(node_key, user_idx, pages=4, size=30):
    node = nodes[node_key]
    u = node.users[user_idx % len(node.users)]
    if node.family:
        out, until = [], None
        for _ in range(pages):
            body = {"limit": size}
            if until: body["untilId"] = until
            s, v = node.api("notes/timeline", body, u)
            if s != 200 or not v: break
            out.extend(v); until = v[-1]["id"]
        return out
    out, mx = [], None
    for _ in range(pages):
        q = f"/timelines/home?limit={size}" + (f"&max_id={mx}" if mx else "")
        s, v = node.mapi("GET", q, user=u)
        if s != 200 or not v: break
        out.extend(v); mx = v[-1]["id"]
    return out

def wait_until(fn, timeout=120, interval=2):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
            if last: return last
        except Exception as e:
            last = f"exc:{e}"
        time.sleep(interval)
    return None

def png_bytes(seed: int):
    import zlib, struct
    def chunk(t, d):
        c = struct.pack(">I", len(d)) + t + d
        return c + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
    ihdr = struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0)
    raw = zlib.compress(b"\x00" + bytes([seed % 256, (seed * 7) % 256, (seed * 13) % 256]))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", raw) + chunk(b"IEND", b""))

TAG = "C" + str(int(time.time()))[-6:]

# ---------------- C1: 50 parallel posts ----------------
def c1():
    lat = []
    lat_lock = threading.Lock()
    results = []

    def one_post(src, u, i):
        marker = f"CONC|post|{src}|{u.name}|{i}|{TAG}"
        t0 = time.perf_counter()
        try:
            nid = nodes[src].post(u, text=marker)
            ms = (time.perf_counter() - t0) * 1000
            with lat_lock:
                lat.append(ms)
                results.append({"src": src, "user": u.name, "i": i, "id": nid, "ok": True, "ms": round(ms, 1)})
        except Exception as e:
            ms = (time.perf_counter() - t0) * 1000
            with lat_lock:
                lat.append(ms)
                results.append({"src": src, "user": u.name, "i": i, "ok": False, "err": str(e)[:200], "ms": round(ms, 1)})

    # 50 posts: 5 per node x 10 accounts spread across nodes
    jobs = []
    node_list = list(nodes.keys())
    for i in range(50):
        src = node_list[i % len(node_list)]
        u = nodes[src].users[i % len(nodes[src].users)]
        jobs.append((src, u, i))

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=50) as ex:
        list(ex.map(lambda j: one_post(*j), jobs))
    wall = time.time() - t0

    ok_count = sum(1 for r in results if r["ok"])
    p = {"total": len(jobs), "ok": ok_count, "fail": len(jobs) - ok_count,
         "wall_s": round(wall, 1), "throughput_posts_per_sec": round(ok_count / wall, 1) if wall > 0 else 0,
         "latency_ms": {"p50": round(statistics.median(lat), 1) if lat else None,
                         "p95": round(pct(lat, 95), 1) if lat else None,
                         "p99": round(pct(lat, 99), 1) if lat else None,
                         "min": round(min(lat), 1) if lat else None,
                         "max": round(max(lat), 1) if lat else None},
         "failures": [r for r in results if not r["ok"]][:10]}
    R["phases"]["C1_parallel_posts"] = p
    print("C1:", json.dumps(p), flush=True)
    return results

# ---------------- C2: 50 parallel reactions ----------------
def c2(post_results):
    # Wait for posts to propagate
    time.sleep(15)
    # Resolve posts on remote nodes and react
    lat = []
    lat_lock = threading.Lock()
    results = []

    def one_react(job):
        src, u, i, post_id = job
        # Find a remote node that has this post
        dst_list = [n for n in nodes if n != src]
        dst = dst_list[i % len(dst_list)]
        du = nodes[dst].users[i % len(nodes[dst].users)]

        # Resolve the post on the remote node
        uri = nodes[src].uri_of(post_id, u)
        if not uri:
            return
        remote = wait_until(lambda: nodes[dst].note_by_uri(uri, du), timeout=60, interval=2)
        if not remote:
            return
        remote_id = remote.get("id") if isinstance(remote, dict) else remote

        t0 = time.perf_counter()
        try:
            s, v = nodes[dst].react(du, remote_id, "🎉" if nodes[dst].family else "❤")
            ms = (time.perf_counter() - t0) * 1000
            with lat_lock:
                lat.append(ms)
                results.append({"src": src, "dst": dst, "i": i, "status": s, "ok": s in (200, 204), "ms": round(ms, 1)})
        except Exception as e:
            ms = (time.perf_counter() - t0) * 1000
            with lat_lock:
                lat.append(ms)
                results.append({"src": src, "dst": dst, "i": i, "ok": False, "err": str(e)[:200], "ms": round(ms, 1)})

    # Pick 50 successful posts
    successful = [r for r in post_results if r.get("ok")][:50]
    jobs = [(r["src"], nodes[r["src"]].users[int(r["i"]) % len(nodes[r["src"]].users)], int(r["i"]), r["id"]) for r in successful]

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=50) as ex:
        list(ex.map(one_react, jobs))
    wall = time.time() - t0

    ok_count = sum(1 for r in results if r["ok"])
    p = {"total": len(jobs), "ok": ok_count, "fail": len(jobs) - ok_count,
         "wall_s": round(wall, 1),
         "latency_ms": {"p50": round(statistics.median(lat), 1) if lat else None,
                         "p95": round(pct(lat, 95), 1) if lat else None,
                         "p99": round(pct(lat, 99), 1) if lat else None},
         "failures": [r for r in results if not r["ok"]][:10]}
    R["phases"]["C2_parallel_reactions"] = p
    print("C2:", json.dumps(p), flush=True)

# ---------------- C3: 50 parallel media uploads ----------------
def c3():
    lat = []
    lat_lock = threading.Lock()
    results = []

    def one_media(src, u, i):
        data = png_bytes(i * 17 + hash(src) % 100)
        marker = f"CONC|media|{src}|{u.name}|{i}|{TAG}"
        t0 = time.perf_counter()
        try:
            fid, furl = nodes[src].upload(u, data, name=f"c{src}{i}.png")
            nid = nodes[src].post(u, text=marker, media=[fid])
            ms = (time.perf_counter() - t0) * 1000
            with lat_lock:
                lat.append(ms)
                results.append({"src": src, "user": u.name, "i": i, "id": nid, "ok": True, "ms": round(ms, 1)})
        except Exception as e:
            ms = (time.perf_counter() - t0) * 1000
            with lat_lock:
                lat.append(ms)
                results.append({"src": src, "user": u.name, "i": i, "ok": False, "err": str(e)[:200], "ms": round(ms, 1)})

    node_list = list(nodes.keys())
    jobs = []
    for i in range(50):
        src = node_list[i % len(node_list)]
        u = nodes[src].users[i % len(nodes[src].users)]
        jobs.append((src, u, i))

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=50) as ex:
        list(ex.map(lambda j: one_media(*j), jobs))
    wall = time.time() - t0

    ok_count = sum(1 for r in results if r["ok"])
    p = {"total": len(jobs), "ok": ok_count, "fail": len(jobs) - ok_count,
         "wall_s": round(wall, 1), "throughput_uploads_per_sec": round(ok_count / wall, 1) if wall > 0 else 0,
         "latency_ms": {"p50": round(statistics.median(lat), 1) if lat else None,
                         "p95": round(pct(lat, 95), 1) if lat else None,
                         "p99": round(pct(lat, 99), 1) if lat else None},
         "failures": [r for r in results if not r["ok"]][:10]}
    R["phases"]["C3_parallel_media"] = p
    print("C3:", json.dumps(p), flush=True)
    return results

# ---------------- C4: 50 parallel replies ----------------
def c4():
    time.sleep(10)
    lat = []
    lat_lock = threading.Lock()
    results = []

    def one_reply(src, u, i):
        # Find a post to reply to from a different node
        dst_list = [n for n in nodes if n != src]
        dst = dst_list[i % len(dst_list)]
        du = nodes[dst].users[i % len(nodes[dst].users)]
        items = deep_home(src, i % len(nodes[src].users), pages=2)
        cands = [n for n in items if note_text(src, n) and f"|{u.name}|" not in note_text(src, n)]
        if not cands:
            return
        tgt = cands[i % len(cands)]
        local_id = tgt.get("id")
        marker = f"CONC|reply|{src}|{u.name}|{i}|{TAG}"
        t0 = time.perf_counter()
        try:
            nid = nodes[src].post(u, text=marker, reply_to=local_id)
            ms = (time.perf_counter() - t0) * 1000
            with lat_lock:
                lat.append(ms)
                results.append({"src": src, "user": u.name, "i": i, "id": nid, "ok": True, "ms": round(ms, 1)})
        except Exception as e:
            ms = (time.perf_counter() - t0) * 1000
            with lat_lock:
                lat.append(ms)
                results.append({"src": src, "user": u.name, "i": i, "ok": False, "err": str(e)[:200], "ms": round(ms, 1)})

    node_list = list(nodes.keys())
    jobs = []
    for i in range(50):
        src = node_list[i % len(node_list)]
        u = nodes[src].users[i % len(nodes[src].users)]
        jobs.append((src, u, i))

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=50) as ex:
        list(ex.map(lambda j: one_reply(*j), jobs))
    wall = time.time() - t0

    ok_count = sum(1 for r in results if r["ok"])
    p = {"total": len(jobs), "ok": ok_count, "fail": len(jobs) - ok_count,
         "wall_s": round(wall, 1), "throughput_replies_per_sec": round(ok_count / wall, 1) if wall > 0 else 0,
         "latency_ms": {"p50": round(statistics.median(lat), 1) if lat else None,
                         "p95": round(pct(lat, 95), 1) if lat else None,
                         "p99": round(pct(lat, 99), 1) if lat else None},
         "failures": [r for r in results if not r["ok"]][:10]}
    R["phases"]["C4_parallel_replies"] = p
    print("C4:", json.dumps(p), flush=True)

def main():
    print(f"concurrent tag={TAG}", flush=True)
    t0 = time.time()
    post_results = c1()
    c2(post_results)
    c3()
    c4()
    R["wall_total_s"] = round(time.time() - t0, 1)
    json.dump(R, open(os.path.join(BENCH_DIR, "concurrent.json"), "w"), indent=1)
    print("CONCURRENT_DONE", json.dumps({k: v.get("ok") for k, v in R["phases"].items()}))
    bad = {k: v["fail"] for k, v in R["phases"].items() if isinstance(v, dict) and v.get("fail")}
    if bad:
        print("CONCURRENT_FAILURES", json.dumps(bad), flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()