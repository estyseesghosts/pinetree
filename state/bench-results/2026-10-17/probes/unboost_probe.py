#!/usr/bin/env python3
"""Scratch probe (not in repo): akkoma unboost — DELETE /statuses/{wrapper} vs POST /statuses/{orig}/unreblog.

Target: pt-ks-authored note A (from feature pair pt-ks -> akkoma). Reads the akkoma reblog
wrapper, records the HTTP status of each undo method, and reads pinetree's renoteCount and
notes/renotes for A after each step. Sequential; mutates only the bench rig.
Usage: unboost_probe.py <A_pt_ks_id> <akkoma_wrapper_id> <akkoma_wrapper_user_acct>
"""
import json, sys, time, urllib.request, urllib.error

ACC = json.load(open("/tmp/opencode/bench/accounts.json"))
PT_KS = "http://pt-ks.test"
AK = "http://akkoma.test"

def req(method, base, path, token=None, body=None, timeout=30):
    h = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    if token:
        h["Authorization"] = "Bearer " + token
    r = urllib.request.Request(base + path, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            ct = resp.headers.get("Content-Type", "")
            return resp.status, (json.loads(raw) if raw and "json" in ct else raw[:300].decode(errors="replace"))
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw[:300].decode(errors="replace")

def token_for(node, acct_name):
    for u in ACC[node]:
        if u["user"] == acct_name:
            return u["token"]
    raise SystemExit(f"no token for {node}:{acct_name}")

def pt_state(note_id, label):
    s, v = req("POST", PT_KS, "/api/notes/show", body={"noteId": note_id}, token=ACC["pt-ks"][0]["token"])
    rc = v.get("renoteCount") if isinstance(v, dict) else v
    s2, rn = req("POST", PT_KS, "/api/notes/renotes", body={"noteId": note_id, "limit": 20}, token=ACC["pt-ks"][0]["token"])
    users = [((x.get("user") or {}).get("username"), (x.get("user") or {}).get("host")) for x in rn] if isinstance(rn, list) else rn
    print(f"[{label}] pt-ks renoteCount={rc} notes/renotes(status={s2})={users}", flush=True)
    return rc, users

def main():
    a_id, wrapper_id, wrapper_acct = sys.argv[1], sys.argv[2], sys.argv[3]
    wrap_user = wrapper_acct  # local akkoma username
    tok = token_for("akkoma", wrap_user)

    pt_state(a_id, "before-undo")
    s, w = req("GET", AK, f"/api/v1/statuses/{wrapper_id}", token=tok)
    print(f"[wrapper GET] status={s} id={w.get('id')} reblog.id={(w.get('reblog') or {}).get('id')} "
          f"account={(w.get('account') or {}).get('acct')} uri={w.get('uri')}", flush=True)
    orig_local = (w.get("reblog") or {}).get("id")

    # 1. the harness's undo: DELETE on the wrapper id
    s, body = req("DELETE", AK, f"/api/v1/statuses/{wrapper_id}", token=tok)
    print(f"[DELETE wrapper] status={s} body={str(body)[:200]}", flush=True)
    time.sleep(30)
    pt_state(a_id, "after-DELETE-wrapper+30s")

    # 2. fresh boost by a second akkoma user, then POST /statuses/{orig}/unreblog
    others = [u["user"] for u in ACC["akkoma"] if u["user"] != wrap_user]
    other = others[0]
    otok = token_for("akkoma", other)
    s, b2 = req("POST", AK, f"/api/v1/statuses/{orig_local}/reblog", token=otok, body={})
    print(f"[reblog by {other}] status={s} wrapper2={b2.get('id') if isinstance(b2, dict) else b2}", flush=True)
    # wait until pt-ks lists the NEW booster (not just the old one) in notes/renotes
    seen = False
    for _ in range(45):
        s3, rn = req("POST", PT_KS, "/api/notes/renotes", body={"noteId": a_id, "limit": 20}, token=ACC["pt-ks"][0]["token"])
        if isinstance(rn, list) and any((x.get("user") or {}).get("username") == other for x in rn):
            seen = True
            break
        time.sleep(2)
    print(f"[wait new booster {other} on pt-ks] seen={seen}", flush=True)
    pt_state(a_id, "after-reblog")
    s, body = req("POST", AK, f"/api/v1/statuses/{orig_local}/unreblog", token=otok, body={})
    print(f"[POST unreblog] status={s} body={str(body)[:200]}", flush=True)
    time.sleep(30)
    pt_state(a_id, "after-unreblog+30s")

if __name__ == "__main__":
    main()
