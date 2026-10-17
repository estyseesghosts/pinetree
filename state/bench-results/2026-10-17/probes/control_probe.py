#!/usr/bin/env python3
"""Scratch control probe (not in repo): does a Misskey reactor's Undo(Like) remove the favourite on Akkoma?

Same shape as heart_probe.py, but the reactor is a Misskey account (misskey node) and the
emoji is 🎉. Usage: control_probe.py <akkoma_status_id> [misskey_user]
"""
import json, sys, time, urllib.request, urllib.error

ACC = json.load(open("/tmp/opencode/bench/accounts.json"))
AK_TOK = ACC["akkoma"][0]["token"]
user = sys.argv[2] if len(sys.argv) > 2 else "mk1"
MK_TOK = {u["user"]: u["token"] for u in ACC["misskey"]}[user]
TAG = f"{user}@misskey.test"

def call(base, ep, body, tok, method="POST"):
    h = {"Content-Type": "application/json", "Authorization": "Bearer " + tok, "Accept": "application/json"}
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(base + ep, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:200].decode(errors="replace")

def ak_favs(status_id):
    s, v = call("http://akkoma.test", f"/api/v1/statuses/{status_id}/favourited_by", None, AK_TOK, "GET")
    return [a.get("acct") for a in v] if isinstance(v, list) else v

def wait_for(pred, secs=40):
    end = time.time() + secs
    while time.time() < end:
        if pred(): return True
        time.sleep(2)
    return False

ak_id = sys.argv[1]
s, st = call("http://akkoma.test", f"/api/v1/statuses/{ak_id}", None, AK_TOK, "GET")
s2, v = call("http://misskey.test", "/api/ap/show", {"uri": st["uri"]}, MK_TOK)
lid = (v.get("object") or v)["id"]
print(f"akkoma={ak_id} misskey local={lid} user={user}", flush=True)
print("akkoma favs before:", ak_favs(ak_id), flush=True)
s, _ = call("http://misskey.test", "/api/notes/reactions/create", {"noteId": lid, "reaction": "🎉"}, MK_TOK)
print(f"reactions/create status={s}", flush=True)
seen = wait_for(lambda: TAG in (ak_favs(ak_id) or []))
print(f"akkoma shows {TAG}: {seen} favs={ak_favs(ak_id)}", flush=True)
s, _ = call("http://misskey.test", "/api/notes/reactions/delete", {"noteId": lid}, MK_TOK)
print(f"reactions/delete status={s}", flush=True)
gone = wait_for(lambda: TAG not in (ak_favs(ak_id) or []), secs=40)
print(f"akkoma {TAG} gone after undo: {gone} favs={ak_favs(ak_id)}", flush=True)
