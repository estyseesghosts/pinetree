#!/usr/bin/env python3
"""Scratch probe (not in repo): does pt-mk's "❤" reaction + undo reach Akkoma, vs "🎉".

Reactor: pt-mk pm1. Target: an Akkoma-authored note already known to pt-mk (local copy).
For each emoji: react -> wait until Akkoma lists pm1 -> unreact -> wait -> check Akkoma again.
Records every HTTP status and the Akkoma favourited_by list for pm1 at each step.
Usage: heart_probe.py <akkoma_status_id>
"""
import json, sys, time, urllib.request, urllib.error

ACC = json.load(open("/tmp/opencode/bench/accounts.json"))
PM1 = {u["user"]: u["token"] for u in ACC["pt-mk"]}["pm1"]
AK_TOK = ACC["akkoma"][0]["token"]

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

def local_id(status_id):
    s, st = call("http://akkoma.test", f"/api/v1/statuses/{status_id}", None, AK_TOK, "GET")
    s2, v = call("http://pt-mk.test", "/api/ap/show", {"uri": st["uri"]}, PM1)
    obj = v.get("object", v) if isinstance(v, dict) else v
    return obj["id"]

def wait_for(pred, secs=40):
    end = time.time() + secs
    while time.time() < end:
        if pred(): return True
        time.sleep(2)
    return False

def main():
    ak_id = sys.argv[1]
    lid = local_id(ak_id)
    print(f"akkoma={ak_id} pt-mk local={lid}", flush=True)
    print("akkoma favs before:", ak_favs(ak_id), flush=True)
    for emoji in ["❤", "🎉"]:
        s, _ = call("http://pt-mk.test", "/api/notes/reactions/create", {"noteId": lid, "reaction": emoji}, PM1)
        print(f"[{emoji}] reactions/create status={s}", flush=True)
        seen = wait_for(lambda: any("pm1@" in a for a in (ak_favs(ak_id) or [])))
        print(f"[{emoji}] akkoma shows pm1 after react: {seen} favs={ak_favs(ak_id)}", flush=True)
        s, _ = call("http://pt-mk.test", "/api/notes/reactions/delete", {"noteId": lid}, PM1)
        print(f"[{emoji}] reactions/delete status={s}", flush=True)
        gone = wait_for(lambda: not any("pm1@" in a for a in (ak_favs(ak_id) or [])), secs=40)
        print(f"[{emoji}] akkoma pm1 gone after undo: {gone} favs={ak_favs(ak_id)}", flush=True)
        time.sleep(3)

if __name__ == "__main__":
    main()
