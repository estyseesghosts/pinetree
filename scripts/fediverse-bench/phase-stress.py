#!/usr/bin/env python3
"""Phase C: stress bursts across the 5-node matrix.

S1 post storm      30 users x 30 posts = 900, latency sampling + convergence sweeps
S2 reaction storm  300 reactions across nodes + undo + baseline restore
S3 poll storm      30 polls + ~150 remote votes
S4 media burst     60 uploads+posts, hash-verified remote copies
S5 reply storm     300 cross-node replies
S6 churn           60 post+delete cycles (zero residue) + reaction RTT timing
S7 big text        450-char posts from every node + over-limit 422 probe
Results -> $BENCH_DIR/stress.json
"""
import json, sys, time, threading, statistics
from concurrent.futures import ThreadPoolExecutor
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import load_nodes, HOSTS, fetch_bytes, sha, _req, BENCH_DIR
import urllib.parse

nodes = load_nodes()
nodes = {k: v for k, v in nodes.items() if len(v.users) > 0}
__import__('lib').run_header('stress', nodes)
import os
if os.environ.get("SKIP_S1") and os.path.exists(os.path.join(BENCH_DIR, "stress.tag")):
    TAG = open(os.path.join(BENCH_DIR, "stress.tag")).read().strip()
else:
    TAG = "S" + str(int(time.time()))[-6:]
    open(os.path.join(BENCH_DIR, "stress.tag"), "w").write(TAG)
R = {"tag": TAG, "phases": {}}
LOCK = threading.Lock()

def U(node, i):
    """User i of a node; wraps for nodes with fewer accounts (mastodon has one)."""
    return node.users[i % len(node.users)]


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
                du = nodes[dst].users[(i // 10) % len(nodes[dst].users)]
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

def build_index(node_key, pages=32):
    """uri/url -> note for every note in the node's local users' home timelines.

    Scans all local users in parallel. pages=32 x 30 covers the whole S1 batch (960 items per
    home timeline); the old 10-page window missed the oldest S1 targets.
    """
    idx = {}
    def scan(i):
        return deep_home(node_key, i, pages=pages)
    with ThreadPoolExecutor(max_workers=8) as ex:
        for items in ex.map(scan, range(len(nodes[node_key].users))):
            for n in items:
                for k in (n.get("uri"), n.get("url")):
                    if k: idx.setdefault(k, n)
    return idx

def edge_from_author(tn, tu, rn):
    """True when at least one local user on node rn follows target author tu (on node tn)."""
    N = nodes[tn]
    uid = N.local_id(tu)
    accts = set()
    if N.family:
        s, v = N.api("users/followers", {"userId": uid, "limit": 100}, N.users[0])
        for f in ((v or []) if s == 200 else []):
            fu = f.get("follower") if isinstance(f.get("follower"), dict) else f
            accts.add(f"{fu.get('username')}@{fu.get('host') or HOSTS[tn]}")
    else:
        s, v = N.mapi("GET", f"/accounts/{uid}/followers?limit=80", user=N.users[0])
        accts = {f["acct"] for f in v} if s == 200 else set()
    return any(ru.acct in accts for ru in nodes[rn].users)

# ---------------- S2 reaction storm ----------------
S2_SETTLE_S = int(os.environ.get("S2_SETTLE_S", 120))

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
        base[nid] = count_reactions(nodes[src].reactions(nid, u))
    # Remote notes are found on a reacting node by (a) ap/show for misskey-family nodes, or
    # (b) its home timelines for mastodon-family nodes. Mastodon-family search?resolve=true
    # returns statuses: [] for a remote note URI (see capability below), so timelines are the
    # only route there. Timelines are indexed once, over the full S1 window.
    IDX = {rn: build_index(rn) for rn in nodes if not nodes[rn].family}
    def locate(rn, turi, ru):
        node = nodes[rn]
        local = node.note_by_uri(turi, ru) if node.family else IDX[rn].get(turi)
        if not local: return None
        return local.get("id") if isinstance(local, dict) else local
    def react_job(job):
        (rn, ru), (tn, tu, tid, turi) = job
        lid = locate(rn, turi, ru)
        if lid is None: return ("unresolved", job)
        s, v = nodes[rn].react(ru, lid, "❤" if tn == "akkoma" else "🎉")
        if s not in (200, 204): return ("react_fail", job, s, str(v)[:200])
        return ("ok", job, lid)
    def safe_react(job):
        try: return react_job(job)
        except Exception as e: return ("exc", job, str(e)[:200])
    jobs = []
    for ri, (rn, ru) in enumerate([(n, u) for n, _N in nodes.items() for u in _N.users]):
        for tgt in targets[(ri * 3) % len(targets):(ri * 3) % len(targets) + 10]:
            if tgt[0] != rn: jobs.append(((rn, ru), tgt))
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=8) as ex:
        outs = list(ex.map(safe_react, jobs))
    resolved = [o for o in outs if o[0] == "ok"]
    unresolved = [o[1] for o in outs if o[0] == "unresolved"]
    react_fails = [o for o in outs if o[0] == "react_fail"]
    excs = [o for o in outs if o[0] == "exc"]
    first_pass_resolved = len(resolved)
    # Bounded settle: give deliveries time to land, rebuild the timeline indexes, retry the misses once.
    print(f"S2: first pass resolved={first_pass_resolved} unresolved={len(unresolved)}; settling {S2_SETTLE_S}s", flush=True)
    time.sleep(S2_SETTLE_S)
    for rn in IDX: IDX[rn] = build_index(rn)
    still = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for job, out in zip(unresolved, ex.map(safe_react, unresolved)):
            if out[0] == "ok": resolved.append(out)
            else: still.append(job)
    recovered = len(resolved) - first_pass_resolved
    # Classify what is still unresolved. Every category here is a failure, not a pass.
    #   no_follow_edge: no local user on the reacting node follows the author (follow-graph gap)
    #   edge_present_not_visible: a follow edge exists, but the note is not in the reacting
    #     node's timelines after the settle (undelivered, or not returned by the timeline API)
    breakdown = {"no_follow_edge": 0, "edge_present_not_visible": 0}
    examples = {k: [] for k in breakdown}
    pairs = {}
    edge = {}
    for (rn, ru), (tn, tu, tid, turi) in still:
        key = (rn, tn, tu.name)
        if key not in edge:
            edge[key] = edge_from_author(tn, tu, rn)
        cat = "edge_present_not_visible" if edge[key] else "no_follow_edge"
        breakdown[cat] += 1
        pk = f"{rn}->{tn}"; pairs[pk] = pairs.get(pk, 0) + 1
        if len(examples[cat]) < 5: examples[cat].append(f"{rn}/{ru.name} <- {tn}/{tu.name} {turi}")
    # Capability: can each mastodon-family reacting node resolve a remote status URI by search?
    capability = {}
    for (rn, ru), (tn, tu, tid, turi) in still:
        if nodes[rn].family or rn in capability: continue
        N = nodes[rn]
        s, v = _req("GET", f"{N.base}/api/v2/search?" + urllib.parse.urlencode({"q": turi, "resolve": "true"}),
                    token=ru.token, host_header=N.host_header)
        capability[rn] = {"uri": turi, "http": s,
                          "statuses_returned": len((v or {}).get("statuses") or []) if isinstance(v, dict) else None}
    print(f"S2 unresolved final={len(still)} breakdown={breakdown} recovered_after_settle={recovered}", flush=True)
    for rn, c in capability.items(): print(f"S2 capability {rn} search-resolve {c}", flush=True)
    if react_fails: print(f"S2 react_fail n={len(react_fails)} e.g. {react_fails[0][2:]}", flush=True)
    expect = {}
    for _, ((rn, ru), (tn, tu, tid, turi)), lid in resolved:
        expect[tid] = expect.get(tid, 0) + 1
    # reactions are counted on the origin note after the verify wait
    time.sleep(90)
    counts = {}
    for src, u, nid, uri in targets:
        counts[nid] = count_reactions(nodes[src].reactions(nid, u)) - base.get(nid, 0)
    short = {tid: (counts.get(tid, 0), exp) for tid, exp in expect.items() if counts.get(tid, 0) < exp}
    # undo with the SAME acting user
    undon = ufail = 0
    for _, ((rn, ru), _t), lid in resolved:
        s, v = nodes[rn].unreac(ru, lid)
        if s in (200, 204): undon += 1  # reactions/delete answers 204
        else: ufail += 1
    time.sleep(60)
    residue = {}
    for src, u, nid, uri in targets:
        left = count_reactions(nodes[src].reactions(nid, u)) - base.get(nid, 0)
        if left > 0: residue[nid] = left
    R["phases"]["S2_reaction_storm"] = {
        "targets": len(targets), "reactions_attempted": len(jobs),
        "resolved_first_pass": first_pass_resolved, "unresolved_first_pass": len(unresolved),
        "resolved_after_settle": recovered, "settle_s": S2_SETTLE_S,
        "reactions_resolved": len(resolved), "react_fail": len(react_fails), "exceptions": len(excs),
        "unresolved_final": len(still), "unresolved_breakdown": breakdown,
        "unresolved_pairs": pairs, "unresolved_examples": examples,
        "by_uri_status_lookup": capability,
        "verify_wait_s": 90, "shortfall_n": len(short),
        "shortfall": {k: v for k, v in list(short.items())[:20]},
        "undo_ok": undon, "undo_fail": ufail,
        "residue_after_undo": dict(list(residue.items())[:10]),
        "wall_s": round(time.time() - t0, 1)}
    print("S2:", json.dumps(R["phases"]["S2_reaction_storm"]), flush=True)

# ---------------- S3 poll storm ----------------
# Polls federate on every node since birchtree reversed D29 (2026-10-19):
# every vote here counts as attempted, never as "not implemented".
POLLS_NOT_FEDERATED: set = set()

def s3():
    polls = []   # (src, note_id, uri)
    for src, _N in nodes.items():
        for k in range(5):
            u = U(_N, 1)
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
    # outs[i] belongs to jobs[i]: (voting node, voter, poll index)
    ni = [(j, o) for j, o in zip(jobs, outs) if j[0] in POLLS_NOT_FEDERATED or polls[j[2]][0] in POLLS_NOT_FEDERATED]
    fed = [(j, o) for j, o in zip(jobs, outs) if (j, o) not in ni]
    ok = sum(1 for _, o in fed if o and o[2] in (200, 204))  # pinetree answers a vote with 204
    time.sleep(120)
    tallied = {}
    for src, su, nid, uri in polls:
        p = nodes[src].poll(nid, su)
        tallied[nid] = sum(c["votes"] for c in p["choices"]) if p else None
    expect = {}
    for _, o in fed:
        if o and o[2] in (200, 204):
            expect[o[1]] = expect.get(o[1], 0) + 1
    short = {k: (tallied.get(k), v) for k, v in expect.items() if (tallied.get(k) or 0) < v}
    R["phases"]["S3_poll_storm"] = {
        "polls": len(polls), "votes_attempted": len(fed), "votes_ok": ok,
        "votes_not_implemented": len(ni),
        "polls_not_implemented": sum(1 for p in polls if p[0] in POLLS_NOT_FEDERATED),
        "verify_wait_s": 120, "shortfall_n": len(short),
        "shortfall": dict(list(short.items())[:10]),
        "wall_s": round(time.time() - t0, 1)}
    print("S3:", json.dumps(R["phases"]["S3_poll_storm"]), flush=True)

# ---------------- S4 media burst ----------------
S4_SETTLE_S = int(os.environ.get("S4_SETTLE_S", 30))
S4_DEADLINE_S = int(os.environ.get("S4_DEADLINE_S", 240))

def png(seed):
    import zlib, struct
    def chunk(t, d):
        c = struct.pack(">I", len(d)) + t + d
        return c + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
    # 1x1 RGB image: one scanline = filter byte + 3 pixel bytes. (The 2x2 header with
    # 6 pixels of data was malformed; birchtree rejects it with 422 "could not be verified".)
    ihdr = struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0)
    raw = zlib.compress(b"\x00" + bytes([seed % 251, (seed * 7) % 251, (seed * 13) % 251]))
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
    # Remote media is fetched by an async job (pinetree: RemoteMediaFetch; observed ~30s after
    # the note). Verifying immediately measured the fetch queue, not federation, so settle first.
    print(f"S4: {len(outs)} uploaded; settling {S4_SETTLE_S}s for async media fetch", flush=True)
    time.sleep(S4_SETTLE_S)
    # verify on one remote node per upload (chosen by URI hash so every destination gets samples)
    def verify_once(job):
        src, u, nid, uri, want = job
        others = [k for k in nodes if k != src]
        dst = others[sum(map(ord, uri)) % len(others)]
        du = nodes[dst].users[0]
        local = nodes[dst].note_by_uri(uri, du)
        if not local: return (src, dst, "unresolved")
        lid = local.get("id") if isinstance(local, dict) else local
        v = nodes[dst].note(lid, du)
        # misskey-family: files; mastodon-family: media_attachments
        files = (v or {}).get("files") or (v or {}).get("media_attachments") or (v or {}).get("attachments") or []
        if not files: return (src, dst, "no-attachment")
        url = files[0].get("url") or files[0].get("remote_url") or files[0].get("textUrl")
        if not url: return (src, dst, "no-url")
        st, body = fetch_bytes(url, timeout=60)
        return (src, dst, "ok" if (st == 200 and sha(body) == want) else f"mismatch:{st}")
    # Remote media arrives by an async fetch job whose queue grows with the burst, so poll to a
    # deadline instead of sleeping a fixed time: an upload that shows up late is lag, not loss.
    verify_deadline = time.time() + S4_DEADLINE_S
    arrival = []
    def verify(job):
        t1 = time.time()
        while True:
            r = verify_once(job)
            if r[2] == "ok":
                arrival.append(time.time() - t1); return r
            if time.time() > verify_deadline: return r
            # A note that never reached the node (an unreachable pair) must not hold a worker for the
            # whole deadline; give it one minute for plain delivery lag.
            if r[2] == "unresolved" and time.time() - t1 > 60: return r
            time.sleep(5)
    with ThreadPoolExecutor(max_workers=6) as ex:
        checks = list(ex.map(verify, outs))
    bad = [c for c in checks if c[2] != "ok"]
    by_cat = {}
    for c in bad: by_cat[c[2]] = by_cat.get(c[2], 0) + 1
    # Per node pair, so an unreachable pair (e.g. pinetree -> mastodon) is not mixed with real media loss.
    by_pair = {}
    for src, dst, cat in checks:
        d = by_pair.setdefault(f"{src}->{dst}", {"ok": 0})
        k = "ok" if cat == "ok" else cat
        d[k] = d.get(k, 0) + 1
    R["phases"]["S4_media_burst"] = {
        "uploads": len(outs), "verified_ok": len(checks) - len(bad),
        "settle_s": S4_SETTLE_S, "deadline_s": S4_DEADLINE_S, "failure_categories": by_cat, "by_pair": by_pair,
        "arrival_after_settle_p50_s": round(statistics.median(arrival), 1) if arrival else None,
        "arrival_after_settle_max_s": round(max(arrival), 1) if arrival else None,
        "bad": [c for c in bad if c[2] != "unresolved"][:10], "wall_s": round(time.time() - t0, 1)}
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
    # reaction RTT timing on pt-a <-> pt-b (first run used pt-ks <-> pt-mk)
    rtt = []
    a, b = nodes["pt-a"], nodes["pt-b"]
    au, bu = U(a, 4), U(b, 4)
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
# Two halves. (a) Federation: a 450-char post every node accepts (under
# Mastodon's 500 default); the remote copy must arrive intact — judged.
# (b) Over-limit: a 2900-char post. Servers enforcing Mastodon's 500 default
# (bt-*, real mastodon) must answer 422; 3000/5000-limit servers (pt-*,
# misskey, catodon, akkoma) accept. Informational only, never judged: it
# documents limit enforcement, not federation.
S7_FED_CHARS = int(os.environ.get("S7_FED_CHARS", 450))
S7_OVER_CHARS = int(os.environ.get("S7_OVER_CHARS", 2900))
S7_LIMIT_500 = {"bt-a", "bt-b", "bt-c", "bt-d", "bt-e", "bt-solo", "mastodon", "mast-a", "mast-b"}
def s7():
    fed_blob = "x" * S7_FED_CHARS
    out = {}
    for src, _N in nodes.items():
        u = U(_N, 5)
        marker = f"PERF|big|{src}|{TAG}|"
        try:
            nid = nodes[src].post(u, text=marker + fed_blob)
            uri = nodes[src].uri_of(nid, u)
            dst = [k for k in nodes if k != src][0]
            local = wait_local(nodes[dst], nodes[dst].users[0], uri)
            v = nodes[dst].note(local, nodes[dst].users[0]) if local else None
            txt = (v or {}).get("text") if nodes[dst].family else (v or {}).get("content")
            out[src] = "ok" if (txt or "").startswith(marker) and len(txt) > S7_FED_CHARS - 50 else \
                       f"bad:len={len(txt or '')}"
        except Exception as e:
            out[src] = f"err:{e}"
    R["phases"]["S7_big_text"] = out
    print("S7:", json.dumps(out), flush=True)
    over_blob = "x" * S7_OVER_CHARS
    over = {}
    for src, _N in nodes.items():
        u = U(_N, 5)
        marker = f"PERF|huge|{src}|{TAG}|"
        try:
            nodes[src].post(u, text=marker + over_blob)
            got = "accepted"
        except Exception as e:
            got = f"rejected:{e}"
        if src in S7_LIMIT_500:
            over[src] = "ok" if "422" in got else f"bad:{got[:60]}"
        else:
            over[src] = "ok" if got == "accepted" else f"info:{got}"
    R["phases"]["S7_over_limit"] = {"outcomes": over, "chars": S7_OVER_CHARS,
                                    "note": "informational: limit enforcement, not federation"}
    print("S7-over-limit:", json.dumps(over), flush=True)

def failures(phases):
    """Every count that means something did not federate. Known-unreachable pairs count too."""
    f = []
    g = lambda ph, k: (phases.get(ph) or {}).get(k) or 0
    for ph, k in [("S1_post_storm", "create_failures"), ("S2_reaction_storm", "unresolved_final"),
                  ("S2_reaction_storm", "react_fail"), ("S2_reaction_storm", "exceptions"),
                  ("S2_reaction_storm", "shortfall_n"), ("S2_reaction_storm", "undo_fail"),
                  ("S2_reaction_storm", "residue_after_undo"), ("S3_poll_storm", "shortfall_n"),
                  ("S6_churn", "residue")]:
        if g(ph, k): f.append(f"{ph}.{k}={g(ph, k)}")
    s3 = phases.get("S3_poll_storm") or {}
    if s3.get("votes_attempted", 0) > s3.get("votes_ok", 0): f.append(f"S3.votes_failed={s3['votes_attempted'] - s3['votes_ok']}")
    s4 = phases.get("S4_media_burst") or {}
    if s4.get("uploads", 0) > s4.get("verified_ok", 0): f.append(f"S4.unverified={s4['uploads'] - s4['verified_ok']}")
    s5 = phases.get("S5_reply_storm") or {}
    if s5.get("sampled", 0) > s5.get("ok", 0): f.append(f"S5.bad={s5['sampled'] - s5['ok']}")
    f += [f"S7.{k}={v}" for k, v in (phases.get("S7_big_text") or {}).items() if v != "ok"]
    # The over-limit probe is informational except when a limit-enforcing
    # server wrongly accepts (a real product bug); "info:" never fails.
    f += [f"S7-over-limit.{k}={v}" for k, v in
          ((phases.get("S7_over_limit") or {}).get("outcomes") or {}).items()
          if v.startswith("bad:")]
    return f

def main():
    print(f"stress tag={TAG}", flush=True)
    t0 = time.time()
    # ONLY=s2,s4 runs selected phases and merges them into the existing stress.json (same tag)
    only = set(os.environ["ONLY"].split(",")) if os.environ.get("ONLY") else None
    phases = [("s1", s1), ("s2", s2), ("s3", s3), ("s4", s4), ("s5", s5), ("s6", s6), ("s7", s7)]
    out = os.path.join(BENCH_DIR, "stress.json")
    if only and os.path.exists(out):
        prev = json.load(open(out))
        if prev.get("tag") == TAG:
            R["phases"] = {**prev.get("phases", {}), **R["phases"]}
    for name, fn in phases:
        if only and name not in only: continue
        if name == "s1" and os.environ.get("SKIP_S1"):
            print("skipping S1 (reuse batch in timelines)")
            R["phases"]["S1_post_storm"] = {"skipped": True, "tag_reused": TAG}
            continue
        fn()
    R["wall_total_s"] = round(time.time() - t0, 1)
    json.dump(R, open(out, "w"), indent=1)
    print("STRESS_DONE", json.dumps({k: v.get("wall_s") for k, v in R["phases"].items() if isinstance(v, dict)}))
    # Only judge the phases this invocation ran; a reused S1 or an earlier phase is not re-judged.
    ran = {k: v for k, v in R["phases"].items() if not only or any(k.lower().startswith(n) for n in only)}
    bad = failures(ran)
    if bad:
        print("STRESS_FAILURES", json.dumps(bad), flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
