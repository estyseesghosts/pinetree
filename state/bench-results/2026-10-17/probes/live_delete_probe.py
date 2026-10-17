#!/usr/bin/env python3
"""Scratch probe (not in repo): does a Misskey Delete reach and remove a note on pinetree, live?

1. mk1 posts on Misskey; 2. pt-ks and pt-mk ap/show the uri (so the note is stored locally);
3. mk1 deletes the note on Misskey; 4. after 40 s, pinetree must hide it (notes/show -> 404 /
text absent). Prints each step; nothing is asserted silently.
"""
import json, sys, time, urllib.request, urllib.error

ACC = json.load(open("/tmp/opencode/bench/accounts.json"))
MK = {u["user"]: u["token"] for u in ACC["misskey"]}["mk1"]
PT = {n: {u["user"]: u["token"] for u in ACC[n]} for n in ("pt-ks", "pt-mk")}

def call(base, ep, body, tok):
    h = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": "Bearer " + tok}
    r = urllib.request.Request(base + ep, data=json.dumps(body).encode(), headers=h, method="POST")
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:200].decode(errors="replace")

s, v = call("http://misskey.test", "/api/notes/create", {"text": f"DELPROBE {int(time.time())}"}, MK)
nid = v["createdNote"]["id"]
uri = f"http://misskey.test/notes/{nid}"
print("misskey note", nid, uri, flush=True)
local = {}
for node in ("pt-ks", "pt-mk"):
    end = time.time() + 60
    while time.time() < end:
        s, v = call(f"http://{node}.test", "/api/ap/show", {"uri": uri}, PT[node]["pm1"] if node == "pt-mk" else PT[node]["pk1"])
        if s == 200 and isinstance(v, dict) and v.get("object"):
            local[node] = v["object"]["id"]
            break
        time.sleep(2)
    print(f"{node} stored: {local.get(node)}", flush=True)

s, v = call("http://misskey.test", "/api/notes/delete", {"noteId": nid}, MK)
print("misskey notes/delete status", s, flush=True)
time.sleep(40)
for node, lid in local.items():
    tok = PT[node]["pm1"] if node == "pt-mk" else PT[node]["pk1"]
    s, v = call(f"http://{node}.test", "/api/notes/show", {"noteId": lid}, tok)
    text = v.get("text") if isinstance(v, dict) else None
    print(f"{node} notes/show {lid}: status={s} text={text!r}", flush=True)
