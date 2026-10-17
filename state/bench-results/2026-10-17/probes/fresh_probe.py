#!/usr/bin/env python3
"""Scratch probe (not in repo): pt-mk reactor vs a FRESH Akkoma note, after the Undo(Like) fix.

1. ak1 creates a new public status on Akkoma (no prior reactions).
2. pt-mk resolves it (ap/show), pm1 reacts ❤ -> Akkoma must list pm1 in favourited_by;
   pm1 undoes -> pm1 must disappear from favourited_by.
3. pm1 reacts 🎉 -> Akkoma's emoji_reactions 🎉 count must rise; undo -> must fall back.
Prints every step; nothing is asserted silently.
"""
import json, sys, time, urllib.request, urllib.error

ACC = json.load(open("/tmp/opencode/bench/accounts.json"))
AK_TOK = {u["user"]: u["token"] for u in ACC["akkoma"]}["ak1"]
PM1 = {u["user"]: u["token"] for u in ACC["pt-mk"]}["pm1"]

def call(base, ep, body, tok, method="POST"):
    h = {"Content-Type": "application/json", "Accept": "application/json"}
    if tok: h["Authorization"] = "Bearer " + tok
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(base + ep, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:200].decode(errors="replace")

def favs(sid):
    s, v = call("http://akkoma.test", f"/api/v1/statuses/{sid}/favourited_by", None, AK_TOK, "GET")
    return [a.get("acct") for a in v] if isinstance(v, list) else v

def emoji_count(sid, name="🎉"):
    s, v = call("http://akkoma.test", f"/api/v1/statuses/{sid}", None, AK_TOK, "GET")
    for e in (v.get("pleroma") or {}).get("emoji_reactions", []) if isinstance(v, dict) else []:
        if e.get("name") == name:
            return e.get("count", 0)
    return 0

def wait_for(pred, secs=45):
    end = time.time() + secs
    while time.time() < end:
        if pred(): return True
        time.sleep(2)
    return False

s, st = call("http://akkoma.test", "/api/v1/statuses",
             {"status": f"FIX-PROBE {int(time.time())}", "visibility": "public"}, AK_TOK)
sid, uri = st["id"], st["uri"]
print(f"akkoma fresh status={sid} uri={uri}", flush=True)
s, v = call("http://pt-mk.test", "/api/ap/show", {"uri": uri}, PM1)
lid = (v.get("object") or v)["id"]
print(f"pt-mk local id={lid} (ap/show status {s})", flush=True)

s, _ = call("http://pt-mk.test", "/api/notes/reactions/create", {"noteId": lid, "reaction": "❤"}, PM1)
print(f"[heart] react status={s}", flush=True)
landed = wait_for(lambda: "pm1@pt-mk.test" in (favs(sid) or []))
print(f"[heart] akkoma lists pm1 as favourite: {landed} favs={favs(sid)}", flush=True)
s, _ = call("http://pt-mk.test", "/api/notes/reactions/delete", {"noteId": lid}, PM1)
print(f"[heart] undo status={s}", flush=True)
gone = wait_for(lambda: "pm1@pt-mk.test" not in (favs(sid) or []))
print(f"[heart] akkoma pm1 gone after undo: {gone} favs={favs(sid)}", flush=True)
time.sleep(3)

base = emoji_count(sid)
s, _ = call("http://pt-mk.test", "/api/notes/reactions/create", {"noteId": lid, "reaction": "🎉"}, PM1)
print(f"[party] react status={s} (🎉 count before={base})", flush=True)
up = wait_for(lambda: emoji_count(sid) > base)
print(f"[party] akkoma 🎉 count rose: {up} now={emoji_count(sid)}", flush=True)
s, _ = call("http://pt-mk.test", "/api/notes/reactions/delete", {"noteId": lid}, PM1)
print(f"[party] undo status={s}", flush=True)
down = wait_for(lambda: emoji_count(sid) <= base)
print(f"[party] akkoma 🎉 count back to {base}: {down} now={emoji_count(sid)}", flush=True)
