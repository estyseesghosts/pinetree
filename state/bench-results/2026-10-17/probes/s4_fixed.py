"""S4 media burst, harness-fixed (2026-10-17 rerun 2). Same workload as phase-stress.py S4 (60 uploads),
with three harness fixes:
  * note_by_uri returns a dict on misskey-family nodes; its id is passed to note(), not the dict.
  * destination rotates over the 4 other nodes (the original len(nid) % 4 was constant).
  * attachment check polls up to 60 s after resolve (linking is async, P5).
Akkoma destinations are reported separately and excluded from the verdict (rig has no search backend).
Writes logs/s4-fixed.json next to the runner.
"""
import sys, json, time, zlib, struct, os
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "/tmp/opencode/bench")
from lib import load_nodes, fetch_bytes, sha

nodes = load_nodes()
TAG = "M" + str(int(time.time()))[-6:]


def png(seed):
    def chunk(t, d):
        c = struct.pack(">I", len(d)) + t + d
        return c + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
    ihdr = struct.pack(">IIBBBBB", 2, 2, 8, 2, 0, 0, 0)
    raw = zlib.compress(b"\x00" + bytes([seed % 251, (seed * 7) % 251, (seed * 13) % 251]) * 6)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", raw) + chunk(b"IEND", b"")


def local_id(x):
    return x.get("id") if isinstance(x, dict) else x


def up_one(job):
    src, u, k, seq = job
    data = png(seq * 7919 % 100000 + 1)
    fid, _url = nodes[src].upload(u, data, name=f"s4{TAG}{src}{u.name}{k}.png")
    marker = f"PERF|media|{src}|{u.name}|{k}|{TAG}"
    nid = nodes[src].post(u, text=marker, media=[fid])
    return (src, u, nid, nodes[src].uri_of(nid, u), sha(data), seq)


jobs, seq = [], 0
for src, N in nodes.items():
    for u in N.users:
        for k in range(2):
            seq += 1
            jobs.append((src, u, k, seq))
t0 = time.time()
with ThreadPoolExecutor(max_workers=6) as ex:
    outs = [o for o in ex.map(up_one, jobs) if o]
print(f"S4 tag={TAG} uploaded={len(outs)} in {time.time() - t0:.1f}s", flush=True)


def others(src):
    return [k for k in nodes if k != src]


def verify(job):
    src, u, nid, uri, want, seq = job
    dst = others(src)[seq % 4]
    du = nodes[dst].users[seq % 6]
    t_res = time.time()
    local = nodes[dst].note_by_uri(uri, du)
    if not local:
        return (src, dst, "unresolved", None)
    lid = local_id(local)
    files = []
    while True:
        v = nodes[dst].note(lid, du)
        files = (v or {}).get("files") or (v or {}).get("attachments") or []
        if files or time.time() - t_res > 60:
            break
        time.sleep(1)
    if not files:
        return (src, dst, "no-attachment-60s", None)
    wait = round(time.time() - t_res, 2)
    url = files[0].get("url") or files[0].get("remote_url") or files[0].get("textUrl")
    st, body = fetch_bytes(url, timeout=60)
    status = "ok" if (st == 200 and sha(body) == want) else f"mismatch:{st}"
    return (src, dst, status, wait)


with ThreadPoolExecutor(max_workers=6) as ex:
    checks = list(ex.map(verify, outs))

pairs = {}
for src, dst, status, wait in checks:
    key = f"{src}->{dst}"
    pairs.setdefault(key, {"ok": 0, "bad": 0, "waits": []})
    if status == "ok":
        pairs[key]["ok"] += 1
        pairs[key]["waits"].append(wait)
    else:
        pairs[key]["bad"] += 1
        pairs[key].setdefault("statuses", []).append(status)

akk = {k: v for k, v in pairs.items() if k.endswith("->akkoma")}
core = {k: v for k, v in pairs.items() if not k.endswith("->akkoma")}
verdict_ok = sum(v["ok"] for v in core.values())
verdict_n = sum(v["ok"] + v["bad"] for v in core.values())
for k in sorted(pairs):
    v = pairs[k]
    w = sorted(v["waits"])
    wtxt = f"attach_wait p50={w[len(w)//2]}s max={w[-1]}s" if w else ""
    print(f"  {k:18s} ok={v['ok']} bad={v['bad']} {wtxt} {v.get('statuses', '')}")
print(f"VERDICT (excl. akkoma dst): {verdict_ok}/{verdict_n} verified")
print(f"akkoma-dst (reported, excluded): ok={sum(v['ok'] for v in akk.values())} bad={sum(v['bad'] for v in akk.values())}")
out = {"tag": TAG, "uploads": len(outs), "pairs": pairs, "verdict_ok": verdict_ok, "verdict_n": verdict_n}
json.dump(out, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "s4-fixed.json"), "w"), indent=1)
