#!/usr/bin/env python3
"""Stress fix-up round: S2 (bigger window), S3 (patient vote resolution),
S4b (patient media verification), S6-res (deleted-note residue inspection)."""
import json, sys, time
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "/tmp/opencode/bench")
from lib import load_nodes, fetch_bytes, sha

nodes = load_nodes()
TAG = open("/tmp/opencode/bench/stress.tag").read().strip()
R = json.load(open("/tmp/opencode/bench/stress.json")) if __import__("os").path.exists("/tmp/opencode/bench/stress.json") else {}
out = {}

def note_text(node, n):
    return (n.get("text") or "") if nodes[node].family else (n.get("content") or "")

# ---- S6 residue inspection: what are the leftover deleted notes? ----
residue_detail = {}
for dst in nodes:
    N = nodes[dst]; u = N.users[0]
    until = None; items = []
    for _ in range(16):
        body = {"limit": 30}
        if until: body["untilId"] = until
        if N.family:
            s, v = N.api("notes/timeline", body, u)
        else:
            q = "/timelines/home?limit=30" + (f"&max_id={until}" if until else "")
            s, v = N.mapi("GET", q, user=u)
        if s != 200 or not v: break
        items += v; until = v[-1]["id"] if N.family else v[-1]["id"]
    hits = [x for x in items if "PERF|churn|" in note_text(dst, x)]
    det = []
    for h in hits[:5]:
        hid = h.get("id")
        # does a direct note fetch still return it?
        if N.family:
            s2, v2 = N.api("notes/show", {"noteId": hid}, u)
        else:
            s2, v2 = N.mapi("GET", f"/statuses/{hid}", user=u)
        det.append({"id": hid, "uri": h.get("uri"), "author": (h.get("user") or {}).get("username"),
                    "host": (h.get("user") or {}).get("host"),
                    "direct_show": s2, "deleted": (s2 != 200) or (isinstance(v2, dict) and (v2.get("deleted") or v2.get("text") is None))})
    if hits: residue_detail[dst] = det
out["s6_residue_detail"] = residue_detail
print("S6-RES:", json.dumps(residue_detail, indent=1)[:1200], flush=True)

# ---- S2 rerun: full 30 targets, users/notes window 80 ----
def find_target(src, u):
    N = nodes[src]
    marker = f"PERF|{src}|{u.name}|0|{TAG}"
    if N.family:
        s, tl = N.api("users/notes", {"userId": N.local_id(u), "limit": 80}, u)
    else:
        s, ac = N.mapi("GET", "/accounts/verify_credentials", user=u)
        s, tl = N.mapi("GET", f"/accounts/{ac['id']}/statuses?limit=80", user=u)
    if s != 200 or not isinstance(tl, list): return None
    for n in tl:
        if marker in note_text(src, n):
            return (src, u, n.get("id"), n.get("uri") or f"{N.base}/notes/{n.get('id')}")
    return None

targets = []
with ThreadPoolExecutor(max_workers=6) as ex:
    for t in ex.map(lambda ju: find_target(*ju),
                    [(n, u) for n, N in nodes.items() for u in N.users]):
        if t: targets.append(t)
print(f"S2r: {len(targets)}/30 targets found", flush=True)

base = {}
for src, u, nid, uri in targets:
    rs = nodes[src].reactions(nid, u)
    base[nid] = sum(len(a) if isinstance(a, list) else int(a or 0) for a in rs.values())

def react_one(job):
    (rn, ru), (tn, tu, tid, turi) = job
    node = nodes[rn]
    try:
        local = node.note_by_uri(turi, ru)
        if not local: return ("unresolved", rn, tn)
        lid = local.get("id") if isinstance(local, dict) else local
        s, v = node.react(ru, lid, "❤" if tn == "akkoma" else "🎉")
        if s != 200: return ("react_fail", rn, tn, s, str(v)[:100])
        return (None, rn, tn, ru.name, lid, tid)
    except Exception as e:
        return ("exc", rn, tn, str(e)[:80])

jobs = []
for ri, (rn, ru) in enumerate([(n, u) for n, N in nodes.items() for u in N.users]):
    if not targets: break
    for tgt in targets[(ri * 3) % len(targets):(ri * 3) % len(targets) + 10]:
        if tgt[0] != rn: jobs.append(((rn, ru), tgt))
t0 = time.time()
resolved = []
fails = 0
with ThreadPoolExecutor(max_workers=6) as ex:
    for o in ex.map(react_one, jobs):
        if o[0] is None: resolved.append(o)
        else: fails += 1
print(f"S2r: reactions attempted={len(jobs)} resolved={len(resolved)} fails={fails}", flush=True)
time.sleep(90)
counts, expect = {}, {}
for src, u, nid, uri in targets:
    rs = nodes[src].reactions(nid, u)
    counts[nid] = sum(len(a) if isinstance(a, list) else int(a or 0) for a in rs.values()) - base.get(nid, 0)
for (_, rn, tn, uname, lid, tid) in resolved:
    expect[tid] = expect.get(tid, 0) + 1
short = {k: (counts.get(k, 0), v) for k, v in expect.items() if counts.get(k, 0) < v}
# undo
undon = ufail = 0
for (_, rn, tn, uname, lid, tid) in resolved:
    ru = [u for u in nodes[rn].users if u.name == uname][0]
    s, v = nodes[rn].unreac(ru, lid)
    if s == 200: undon += 1
    else: ufail += 1
time.sleep(60)
residue = {}
for src, u, nid, uri in targets:
    rs = nodes[src].reactions(nid, u)
    left = sum(len(a) if isinstance(a, list) else int(a or 0) for a in rs.values()) - base.get(nid, 0)
    if left > 0: residue[nid] = left
out["S2_reaction_storm"] = {"targets": len(targets), "attempted": len(jobs), "note": "skipped" if not targets else None,
    "resolved": len(resolved), "fails": fails, "shortfall_n": len(short),
    "shortfall": dict(list(short.items())[:10]), "undo_ok": undon, "undo_fail": ufail,
    "residue_after_undo": dict(list(residue.items())[:10]), "wall_s": round(time.time() - t0, 1)}
print("S2r:", json.dumps(out["S2_reaction_storm"]), flush=True)

# ---- S3 rerun: 15 fresh polls + patient votes ----
polls = []
for src, N in nodes.items():
    for k in range(3):
        u = N.users[1]
        marker = f"PERF|poll2|{src}|{k}|{TAG}"
        nid = N.post(u, text=marker, poll={"choices": ["one", "two", "three"], "expires_in": 7200})
        polls.append((src, u, nid, N.uri_of(nid, u)))
print(f"S3r: {len(polls)} polls", flush=True)

def vote_one(job):
    (rn, ru, pi) = job
    src, su, nid, uri = polls[pi]
    node = nodes[rn]
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            local = node.note_by_uri(uri, ru)
            if local:
                lid = local.get("id") if isinstance(local, dict) else local
                s, v = node.poll_vote(ru, lid, pi % 3)
                if s == 200: return (src, nid, "ok")
                if s not in (400, 404, 429): return (src, nid, s)
        except Exception: pass
        time.sleep(10)
    return (src, nid, "unresolved")

jobs = []
for ri, (rn, ru) in enumerate([(n, u) for n, N in nodes.items() for u in N.users]):
    for j in range(3):
        pi = (ri * 5 + j * 4) % len(polls)
        if polls[pi][0] != rn: jobs.append((rn, ru, pi))
t0 = time.time()
with ThreadPoolExecutor(max_workers=6) as ex:
    outs = list(ex.map(vote_one, jobs))
ok = sum(1 for o in outs if o[2] == "ok")
print(f"S3r: votes attempted={len(jobs)} ok={ok}", flush=True)
time.sleep(120)
tallied, expectv = {}, {}
for src, su, nid, uri in polls:
    p = nodes[src].poll(nid, su)
    tallied[nid] = sum(c["votes"] for c in p["choices"]) if p else None
for o in outs:
    if o[2] == "ok": expectv[o[1]] = expectv.get(o[1], 0) + 1
shortv = {k: (tallied.get(k), v) for k, v in expectv.items() if (tallied.get(k) or 0) < v}
out["S3_poll_storm"] = {"polls": len(polls), "attempted": len(jobs), "votes_ok": ok,
    "shortfall_n": len(shortv), "shortfall": dict(list(shortv.items())[:10]),
    "wall_s": round(time.time() - t0, 1)}
print("S3r:", json.dumps(out["S3_poll_storm"]), flush=True)

# ---- S4b: patient re-verification of the 60 S4 uploads is impossible without
# stored ids; instead: 30 fresh uploads, patient polling for attachments ----
import zlib, struct
def png(seed):
    def chunk(t, d):
        c = struct.pack(">I", len(d)) + t + d
        return c + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
    ihdr = struct.pack(">IIBBBBB", 2, 2, 8, 2, 0, 0, 0)
    raw = zlib.compress(b"\x00" + bytes([seed % 251, (seed * 7) % 251, (seed * 13) % 251]) * 6)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", raw) + chunk(b"IEND", b""))

ups = []
def up(job):
    src, u, k = job
    N = nodes[src]
    data = png(hash((src, u.name, k, "b")) % 100000)
    fid, url = N.upload(u, data, name=f"b{src}{u.name}{k}.png")
    nid = N.post(u, text=f"PERF|media2|{src}|{u.name}|{k}|{TAG}", media=[fid])
    return (src, u, nid, N.uri_of(nid, u), sha(data))
jobs = [(src, u, k) for src, N in nodes.items() for u in N.users[:1] for k in range(5)]
with ThreadPoolExecutor(max_workers=6) as ex:
    ups = [o for o in ex.map(up, jobs) if o]
def verify(job):
    src, u, nid, uri, want = job
    dst = [k for k in nodes if k != src][len(nid) % 4]
    du = nodes[dst].users[0]
    deadline = time.time() + 300
    last = "unresolved"
    while time.time() < deadline:
        try:
            local = nodes[dst].note_by_uri(uri, du)
            if local:
                lid = local.get("id") if isinstance(local, dict) else local
                v = nodes[dst].note(lid, du)
                files = (v or {}).get("files") or (v or {}).get("attachments") or []
                if files:
                    url = files[0].get("url") or files[0].get("remote_url") or files[0].get("textUrl")
                    st, body = fetch_bytes(url, timeout=60)
                    if st == 200 and sha(body) == want: return (src, dst, "ok", round(deadline - time.time()))
                    last = f"fetch:{st}"
                else:
                    last = "no-attachment-yet"
        except Exception as e:
            last = f"exc:{e}"
        time.sleep(10)
    return (src, dst, last, 0)
t0 = time.time()
with ThreadPoolExecutor(max_workers=6) as ex:
    checks = list(ex.map(verify, ups))
okc = [c for c in checks if c[2] == "ok"]
lat = [c[3] for c in okc]
out["S4b_media"] = {"uploads": len(ups), "ok": len(okc),
    "bad": [c[:3] for c in checks if c[2] != "ok"][:10],
    "p50_link_s": sorted(lat)[len(lat)//2] if lat else None,
    "max_link_s": max(lat) if lat else None, "wall_s": round(time.time() - t0, 1)}
print("S4b:", json.dumps(out["S4b_media"]), flush=True)

json.dump(out, open("/tmp/opencode/bench/stress-fixup.json", "w"), indent=1)
print("FIXUP_DONE")
