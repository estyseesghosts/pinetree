#!/usr/bin/env python3
"""Scratch probe (not in repo): S6-style churn on Misskey — create and delete with NO pause,
then check whether pt-ks / pt-mk still hold the note. Repeats N times (default 5).

Residue check: pinetree ap/show on the note uri. 200 + text present = residue (note still live);
anything else = gone. A 40 s settle wait precedes the check.
"""
import json, sys, time, urllib.request, urllib.error

ACC = json.load(open("/tmp/opencode/bench/accounts.json"))
MK = {u["user"]: u["token"] for u in ACC["misskey"]}["mk1"]
PT = {n: {u["user"]: u["token"] for u in ACC[n]} for n in ("pt-ks", "pt-mk")}
N = int(sys.argv[1]) if len(sys.argv) > 1 else 5

def call(base, ep, body, tok):
    h = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": "Bearer " + tok}
    r = urllib.request.Request(base + ep, data=json.dumps(body).encode(), headers=h, method="POST")
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:200].decode(errors="replace")

uris = []
for i in range(N):
    s, v = call("http://misskey.test", "/api/notes/create", {"text": f"FASTDEL {i} {int(time.time())}"}, MK)
    nid = v["createdNote"]["id"]
    s2, _ = call("http://misskey.test", "/api/notes/delete", {"noteId": nid}, MK)
    uris.append(f"http://misskey.test/notes/{nid}")
    print(f"created+deleted {nid} (delete status {s2})", flush=True)
time.sleep(40)
for uri in uris:
    row = []
    for node in ("pt-ks", "pt-mk"):
        s, v = call(f"http://{node}.test", "/api/ap/show", {"uri": uri}, PT[node]["pk1"] if node == "pt-ks" else PT[node]["pm1"])
        obj = v.get("object") if isinstance(v, dict) else None
        txt = (obj or {}).get("text") if obj else None
        row.append(f"{node}:{'RESIDUE' if txt else 'gone'}({s})")
    print(uri.rsplit('/', 1)[1], " ".join(row), flush=True)
