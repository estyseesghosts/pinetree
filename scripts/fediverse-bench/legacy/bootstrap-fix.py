#!/usr/bin/env python3
"""Finish mk2..mk6 + cd2..cd6 after captcha/registration fixes. Merges into accounts.json."""
import os
BD = os.environ["BENCH_DIR"]
import json, sys, time, urllib.request, urllib.error

PASS = "password123"
ACC = os.path.join(BD, "accounts.json")
out = json.load(open(ACC))

def req(base, ep, body, token=None):
    h = {"content-type": "application/json"}
    if token: h["authorization"] = f"Bearer {token}"
    r = urllib.request.Request(f"{base}/api/{ep}", data=json.dumps(body).encode(), headers=h, method="POST")
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try: err = json.loads(e.read())
        except Exception: err = None
        return e.code, err

errors = []
for node, (base, prefix) in {"misskey": ("http://misskey.test", "mk"),
                             "catodon": ("http://catodon.test", "cd")}.items():
    users = out.get(node, [])
    admin = users[0] if users else None
    if not admin:
        errors.append(f"{node}: no admin account in accounts.json"); continue
    # open registration (misskey/catodon default to closed in 2026)
    s, v = req(base, "admin/update-meta", {"disableRegistration": False}, token=admin["token"])
    if s not in (200, 204):
        errors.append(f"{node} open registration: {s} {v}")
    have = {u["user"] for u in users}
    for i in range(1, 7):
        u = f"{prefix}{i}"
        if u in have: continue
        s, v = req(base, "signup", {"username": u, "password": PASS})
        if s != 200:
            errors.append(f"{node} signup {u}: {s} {v}"); continue
        s2, v2 = req(base, "signin-flow", {"username": u, "password": PASS})
        tok = v2.get("i") if s2 == 200 else None
        if not tok:
            errors.append(f"{node} signin-flow {u}: {s2} {v2}"); continue
        users.append({"user": u, "token": tok})
        time.sleep(0.2)
    # close registration again to match production defaults
    req(base, "admin/update-meta", {"disableRegistration": True}, token=admin["token"])
    # confirm federation=all really applies
    s, v = req(base, "meta", {})
    fed = (v or {}).get("federation")
    print(f"{node}: {len(users)}/6 accounts, federation={fed!r}", flush=True)
    if fed != "all": errors.append(f"{node}: federation is {fed!r}")
    out[node] = users

json.dump(out, open(ACC, "w"), indent=1)
if errors:
    print("ERRORS:"); [print(" ", e) for e in errors]; sys.exit(1)
print("FIXUP_OK")
