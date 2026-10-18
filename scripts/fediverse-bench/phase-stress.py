#!/usr/bin/env python3
"""Phase C: stress bursts across the 5-node matrix.

S1 post storm      30 users x 30 posts = 900, latency sampling + convergence sweeps
S2 reaction storm  300 reactions across nodes + undo + baseline restore
S3 poll storm      30 polls + ~150 remote votes
S4 media burst     60 uploads+posts, hash-verified remote copies
S5 reply storm     300 cross-node replies
S6 churn           60 post+delete cycles (zero residue) + reaction RTT timing
S7 big text        2900-char posts from every node
Results -> /tmp/bench/stress.json
"""
import json, sys, time, threading, statistics
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "/tmp/bench")
from lib import load_nodes, HOSTS, fetch_bytes, sha

nodes = load_nodes()
nodes = {k: v for k, v in nodes.items() if len(v.users) > 0}
import os
if os.environ.get("SKIP_S1") and os.path.exists("/tmp/bench/stress.tag"):
    TAG = open("/tmp/bench/stress.tag").read().strip()
else:
    TAG = "S" + str(int(time.time()))[-6:]
    open("/tmp/bench/stress.tag", "w").write(TAG)
R = {"tag": TAG, "phases": {}}
LOCK = threading.Lock()

def note_text(node, n):
    return (n.get("text") or "") if nodes[node].family else (n.get("content") or "")


def count_reactions(rs):
    return sum(len(a) if isinstance(a, list) else int(a or 0) for a in rs.values())

def pct(lat, p):
    s = sorted(lat)
    return s[min(len(s) - 1, int(p / 100 * len(s)))] if s else None

def deep_home(node_key, user_idx, pages, size=30):
    node = nodes[node_key]
    u = node.users[user_idx]
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

# ---------------- S1 post storm ----------------
def s1():
    lat, lock = [], threading.Lock()
    stop = threading.Event()
    uris = {}          # (src,user) -> marker
    pollers = []
    def one_post(src, u, i):
        marker = f"PERF|{src}|{u.name}|{i}|{TAG}"
        t0 = time.perf_counter()
        nid = nodes[src].post(u, text=marker)
        with lock: lat.append((time.perf_counter() - t0) * 1000)
        if i % 10 == 0:  # latency sample: poll a remote node until seen
            dst = [k for k in nodes if k != src][i % 4]
            def poll():
                t1 = time.perf_counter()
                du = nodes[dst].users[(i // 10) % 6]
                while not stop.is_set() and time.perf_counter() - t1 < 300:
                    if any(marker in note_text(dst, n) for n in deep_home_node(dst, du)):
                        SAMPLES.append((src, dst, (time.perf_counter() - t1)))
                        return
                    time.sleep(1)
            th = threading.Thread(target=poll, daemon=True); th.start(); pollers.append(th)
        return nid
    global SAMPLES
    SAMPLES = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=9) as ex:
        futs = [ex.submit(one_post, src, u, i)
                for src, _N in nodes.items()
                for u in _N.users for i in range(30)]
        fails = sum(1 for f in futs if f.exception())
    wall = time.time() - t0
    create_ms = [x for x in lat if x is not None]
    p = {"posts": len(futs), "create_failures": fails, "wall_s": round(wall, 1),
         "create_p50_ms": round(statistics.median(create_ms), 1) if create_ms else None,
         "create_p95_ms": round(pct(create_ms, 95), 1) if create_ms else None,
         "create_p99_ms": round(pct(create_ms, 99), 1) if create_ms else None}
    print("S1 created:", json.dumps(p), flush=True)
    # convergence sweeps on user#1 of each node (home should carry 29x30=870 remote markers)
    sweeps = {}
    for wait_s in (15, 60, 180):
        time.sleep(wait_s if not sweeps else wait_s - max(sweeps))
        snap = {}
        for dst in nodes:
            items = deep_home(dst, 0, pages=32)
            texts = [note_text(dst, n) for n in items]
            per_src = {src: sum(1 for t in texts if f"PERF|{src}|" in (t or ""))
                       for src in nodes if src != dst}
            dupes = len(texts) - len({(n.get("uri") or n.get("id")) for n in items})
            snap[dst] = {"per_src": per_src, "total_markers": sum(per_src.values()),
                         "dupes": dupes, "items": len(items)}
        sweeps[wait_s] = snap
        print(f"S1 sweep +{wait_s}s:", json.dumps(snap), flush=True)
    for th in pollers: th.join(timeout=0)
    lat_samples = [{"src": s, "dst": d, "secs": round(t, 1)} for s, d, t in SAMPLES]
    p["sweeps"] = sweeps
    p["federation_latency_samples"] = lat_samples
    allt = [t for _, _, t in SAMPLES]
    if allt:
        p["fed_latency_p50_s"] = round(statistics.median(allt), 2)
        p["fed_latency_p95_s"] = round(pct(allt, 95), 2)
        p["fed_latency_max_s"] = round(max(allt), 2)
    R["phases"]["S1_post_storm"] = p
    return uris

def deep_home_node(dst, du):
    node = nodes[dst]
    if node.family:
        s, v = node.api("notes/timeline", {"limit": 30}, du)
        return v if s == 200 else []
    s, v = node.mapi("GET", "/timelines/home?limit=30", user=du)
    return v if s == 200 else []

# ---------------- S2 reaction storm ----------------
def s2():
    # targets: post seq-0 of each user in S1 (30 targets across all nodes)
    targets = []
    for src, _N in nodes.items():
        for u in _N.users:
            marker = f"PERF|{src}|{u.name}|0|{TAG}"
            v = None
            # own posts do NOT appear in one's own home timeline (misskey-family);
            # scan the local public timeline first, then home as fallback.
            if _N.family:
                s, tl = _N.api("users/notes", {"userId": _N.local_id(u), "limit": 40}, u)
                cands = tl if s == 200 and isinstance(tl, list) else []
            else:
                s, accts = _N.mapi("GET", "/accounts/verify_credentials", user=u)
                s, tl = _N.mapi("GET", f"/accounts/{accts['id']}/statuses?limit=40", user=u)
                cands = tl if s == 200 and isinstance(tl, list) else []
            for n in cands:
                if marker in note_text(src, n):
                    v = n; break
            if not v:
                for n in deep_home(src, _N.users.index(u), pages=2):
                    if marker in note_text(src, n):
                        v = n; break
            if v: targets.append((src, u, v.get("id"), v.get("uri") or f"{nodes[src].base}/notes/{v.get('id')}"))
    print(f"S2: {len(targets)} targets", flush=True)
    if not targets:
        R["phases"]["S2_reaction_storm"] = {"error": "no targets"}
        return
    base = {}
    for src, u, nid, uri in targets:
        rs = nodes[src].reactions(nid, u)
        base[nid] = count_reactions(rs)
    def one(job):
        (rn, ru), (tn, tu, tid, turi) = job
        node = nodes[rn]
        local = node.note_by_uri(turi, ru)
        if not local: return ("unresolved", rn, tn)
        s, v = node.react(ru, local.get("id") if isinstance(local, dict) else local, "❤" if tn == "akkoma" else "🎉")
        if s != 200: return ("react_fail", s, v)
        return (None, rn, tn, ru.name, local.get("id") if isinstance(local, dict) else local, tid)
    jobs = []
    for ri, (rn, ru) in enumerate([(n, u) for n, _N in nodes.items() for u in _N.users]):
        for tgt in targets[(ri * 3) % len(targets):(ri * 3) % len(targets) + 10]:
            if tgt[0] != rn: jobs.append(((rn, ru), tgt))
    t0 = time.time()
    resolved = []
    def safe_one(j):
        try: return one(j)
        except Exception as e: return ("exc", str(e))
    with ThreadPoolExecutor(max_workers=8) as ex:
        for out in ex.map(safe_one, jobs):
            if out[0] is None:
                resolved.append(out)
            elif out[0][0] in ("unresolved", "exc"):
                pass  # note not arrived or transient error; skip
            else:
                print("S2 react fail:", out, flush=True)
    wait = 90
    time.sleep(wait)
    counts = {}
    for src, u, nid, uri in targets:
        rs = nodes[src].reactions(nid, u)
        counts[nid] = count_reactions(rs) - base.get(nid, 0)
    expect = {}
    for (_, rn, tn, uname, local, tid) in resolved:
        expect[tid] = expect.get(tid, 0) + 1
    short = {tid: (counts.get(tid, 0), exp) for tid, exp in expect.items() if counts.get(tid, 0) < exp}
    # undo with the SAME acting user
    undon = ufail = 0
    for (_, rn, tn, uname, local, tid) in resolved:
        ru = [u for u in nodes[rn].users if u.name == uname][0]
        s, v = nodes[rn].unreac(ru, local)
        if s == 200: undon += 1
        else: ufail += 1
    time.sleep(60)
    residue = {}
    for src, u, nid, uri in targets:
        rs = nodes[src].reactions(nid, u)
        left = count_reactions(rs) - base.get(nid, 0)
        if left > 0: residue[nid] = left
    R["phases"]["S2_reaction_storm"] = {
        "targets": len(targets), "reactions_attempted": len(jobs),
        "reactions_resolved": len(resolved), "verify_wait_s": wait,
        "shortfall_n": len(short), "shortfall": {k: v for k, v in list(short.items())[:20]},
        "undo_ok": undon, "undo_fail": ufail,
        "residue_after_undo": dict(list(residue.items())[:10]),
        "wall_s": round(time.time() - t0, 1)}
    print("S2:", json.dumps(R["phases"]["S2_reaction_storm"]), flush=True)

# ---------------- S3 poll storm ----------------
def s3():
    polls = []   # (src, note_id, uri)
    for src, _N in nodes.items():
        for k in range(5):
            u = _N.users[1]
            marker = f"PERF|poll|{src}|{k}|{TAG}"
            nid = nodes[src].post(u, text=marker,
                                  poll={"choices": ["one", "two", "three"], "expires_in": 7200})
            polls.append((src, u, nid, nodes[src].uri_of(nid, u)))
    print(f"S3: {len(polls)} polls", flush=True)
    if not polls:
        R["phases"]["S3_poll_storm"] = {"error": "no polls"}
        return
    votes = []
    def vote_one(job):
        (rn, ru, pi) = job
        src, su, nid, uri = polls[pi]
        node = nodes[rn]
        try:
            local = node.note_by_uri(uri, ru)
            if not local: return (src, nid, "unresolved")
            s, v = node.poll_vote(ru, local.get("id") if isinstance(local, dict) else local, pi % 3)
            return (src, nid, s)
        except Exception as e:
            return (src, nid, f"exc:{e}")
    jobs = []
    for ri, (rn, ru) in enumerate([(n, u) for n, _N in nodes.items() for u in _N.users]):
        for j in range(5):
            pi = (ri * 7 + j * 3) % len(polls)
            if polls[pi][0] != rn:
                jobs.append((rn, ru, pi))
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=8) as ex:
        outs = list(ex.map(vote_one, jobs))
    ok = sum(1 for o in outs if o and o[2] == 200)
    time.sleep(120)
    tallied = {}
    for src, su, nid, uri in polls:
        p = nodes[src].poll(nid, su)
        tallied[nid] = sum(c["votes"] for c in p["choices"]) if p else None
    expect = {}
    for o in outs:
        if o and o[2] == 200:
            expect[o[1]] = expect.get(o[1], 0) + 1
    short = {k: (tallied.get(k), v) for k, v in expect.items() if (tallied.get(k) or 0) < v}
    R["phases"]["S3_poll_storm"] = {
        "polls": len(polls), "votes_attempted": len(jobs), "votes_ok": ok,
        "verify_wait_s": 120, "shortfall_n": len(short),
        "shortfall": dict(list(short.items())[:10]),
        "wall_s": round(time.time() - t0, 1)}
    print("S3:", json.dumps(R["phases"]["S3_poll_storm"]), flush=True)

# ---------------- S4 media burst ----------------
def png(seed):
    import zlib, struct
    def chunk(t, d):
        c = struct.pack(">I", len(d)) + t + d
        return c + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
    ihdr = struct.pack(">IIBBBBB", 2, 2, 8, 2, 0, 0, 0)
    raw = zlib.compress(b"\x00" + bytes([seed % 251, (seed * 7) % 251, (seed * 13) % 251]) * 6)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", raw) + chunk(b"IEND", b""))

def s4():
    uploads = []   # (src, user, note_id, uri, sha)
    def up_one(job):
        src, u, k = job
        data = png(hash((src, u.name, k)) % 100000)
        fid, url = nodes[src].upload(u, data, name=f"bench{src}{u.name}{k}.png")
        marker = f"PERF|media|{src}|{u.name}|{k}|{TAG}"
        nid = nodes[src].post(u, text=marker, media=[fid])
        return (src, u, nid, nodes[src].uri_of(nid, u), sha(data))
    jobs = [(src, u, k) for src, _N in nodes.items() for u in _N.users for k in range(2)]
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=6) as ex:
        outs = [o for o in ex.map(up_one, jobs) if o]
    # verify on one sampled remote node per upload
    def verify(job):
        src, u, nid, uri, want = job
        dst = [k for k in nodes if k != src][len(nid) % 4]
        du = nodes[dst].users[0]
        local = nodes[dst].note_by_uri(uri, du)
        if not local: return (src, dst, "unresolved")
        v = nodes[dst].note(local, du)
        files = (v or {}).get("files") or (v or {}).get("attachments") or []
        if not files: return (src, dst, "no-attachment")
        url = files[0].get("url") or files[0].get("remote_url") or files[0].get("textUrl")
        st, body = fetch_bytes(url, timeout=60)
        return (src, dst, "ok" if (st == 200 and sha(body) == want) else f"mismatch:{st}")
    with ThreadPoolExecutor(max_workers=6) as ex:
        checks = list(ex.map(verify, outs))
    bad = [c for c in checks if c[2] != "ok"]
    R["phases"]["S4_media_burst"] = {
        "uploads": len(outs), "verified_ok": len(checks) - len(bad),
        "bad": bad[:10], "wall_s": round(time.time() - t0, 1)}
    print("S4:", json.dumps(R["phases"]["S4_media_burst"]), flush=True)

# ---------------- S5 reply storm ----------------
def s5():
    def rep_one(job):
        rn, ru, k = job
        # reply to the k-th foreign target note found in own home timeline
        items = deep_home(rn, ru_idx(ru), pages=2)
        cands = [n for n in items if (note_text(rn, n) or "").startswith("PERF|")
                 and f"|{ru.name}|" not in (note_text(rn, n) or "")]
        if not cands: return None
        tgt = cands[k % len(cands)]
        local_id = tgt.get("id")
        marker = f"PERF|reply|{rn}|{ru.name}|{k}|{TAG}"
        nid = nodes[rn].post(ru, text=marker, reply_to=local_id)
        return (rn, tgt.get("uri") or tgt.get("url"), marker, nid)
    def ru_idx(u):
        return nodes[[n for n, N in nodes.items() if u in N.users][0]].users.index(u)
    jobs = [(n, u, k) for n, _N in nodes.items() for u in _N.users for k in range(10)]
    t0 = time.time()
    def safe_rep(j):
        try: return rep_one(j)
        except Exception: return None
    with ThreadPoolExecutor(max_workers=8) as ex:
        outs = [o for o in ex.map(safe_rep, jobs) if o]
    time.sleep(90)
    # verify parents saw the replies (renoteCount on parent won't move; check reply resolution at src)
    def check(o):
        rn, turi, marker, nid = o
        node = nodes[rn]
        # find a foreign node that can resolve the reply and check its replyId
        dst = [k for k in nodes if k != rn][0]
        v = nodes[dst].note_by_uri(node.uri_of(nid, node.users[0]), nodes[dst].users[0])
        if not v: return "unresolved"
        rid = v.get("replyId") or (v.get("reply") or {}).get("id")
        return "ok" if rid else f"no-parent:{json.dumps(v)[:120]}"
    with ThreadPoolExecutor(max_workers=8) as ex:
        checks = list(ex.map(check, outs[:60]))   # sample 60
    bad = [c for c in checks if c != "ok"]
    R["phases"]["S5_reply_storm"] = {
        "replies": len(outs), "sampled": len(checks), "ok": len(checks) - len(bad),
        "bad": bad[:6], "wall_s": round(time.time() - t0, 1)}
    print("S5:", json.dumps(R["phases"]["S5_reply_storm"]), flush=True)

# ---------------- S6 churn ----------------
def s6():
    res = {"deleted": 0}
    for src, _N in nodes.items():
        for u in _N.users[:2]:
            marker = f"PERF|churn|{src}|{u.name}|{TAG}"
            nid = nodes[src].post(u, text=marker)
            nodes[src].delete(u, nid)
            res["deleted"] += 1
    time.sleep(120)
    residue = {}
    for dst in nodes:
        items = deep_home(dst, 0, pages=32)
        hits = [note_text(dst, n) for n in items if "PERF|churn|" in (note_text(dst, n) or "")]
        if hits: residue[dst] = len(hits)
    # reaction RTT timing on pt-ks <-> pt-mk
    rtt = []
    a, b = nodes["pt-ks"], nodes["pt-mk"]
    au, bu = a.users[4], b.users[4]
    nid = a.post(au, text=f"PERF|rtt|{TAG}")
    uri = a.uri_of(nid, au)
    local = wait_local(b, bu, uri)
    if local:
        for i in range(20):
            t0 = time.perf_counter()
            b.react(bu, local)
            def seen():
                rs = a.reactions(nid, au)
                return count_reactions(rs) >= 1
            if wait(seen, 30): rtt.append((time.perf_counter() - t0))
            t0 = time.perf_counter()
            b.unreac(bu, local)
            def gone():
                rs = a.reactions(nid, au)
                return count_reactions(rs) == 0
            wait(gone, 30); rtt.append((time.perf_counter() - t0))
    res["reaction_rtt_s"] = {"n": len(rtt),
                             "p50": round(statistics.median(rtt), 2) if rtt else None,
                             "max": round(max(rtt), 2) if rtt else None}
    res["residue"] = residue
    R["phases"]["S6_churn"] = res
    print("S6:", json.dumps(res), flush=True)

def wait_local(node, u, uri, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = node.note_by_uri(uri, u)
        if v and v.get("id"): return v["id"]
        time.sleep(3)
    return None

def wait(fn, timeout=30, interval=1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn(): return True
        except Exception: pass
        time.sleep(interval)
    return False

# ---------------- S7 big text ----------------
def s7():
    blob = "x" * 2900
    out = {}
    for src, _N in nodes.items():
        u = _N.users[5]
        marker = f"PERF|big|{src}|{TAG}|"
        try:
            nid = nodes[src].post(u, text=marker + blob)
            uri = nodes[src].uri_of(nid, u)
            dst = [k for k in nodes if k != src][0]
            local = wait_local(nodes[dst], nodes[dst].users[0], uri)
            v = nodes[dst].note(local, nodes[dst].users[0]) if local else None
            txt = (v or {}).get("text") if nodes[dst].family else (v or {}).get("content")
            out[src] = "ok" if (txt or "").startswith(marker) and len(txt) > 2900 - 50 else \
                       f"bad:len={len(txt or '')}"
        except Exception as e:
            out[src] = f"err:{e}"
    R["phases"]["S7_big_text"] = out
    print("S7:", json.dumps(out), flush=True)

def main():
    print(f"stress tag={TAG}", flush=True)
    t0 = time.time()
    if os.environ.get("SKIP_S1"):
        print("skipping S1 (reuse batch in timelines)")
        R["phases"]["S1_post_storm"] = {"skipped": True, "tag_reused": TAG}
    else:
        s1()
    s2(); s3(); s4(); s5(); s6(); s7()
    R["wall_total_s"] = round(time.time() - t0, 1)
    json.dump(R, open("/tmp/bench/stress.json", "w"), indent=1)
    print("STRESS_DONE", json.dumps({k: v.get("wall_s") for k, v in R["phases"].items()}))

if __name__ == "__main__":
    main()
