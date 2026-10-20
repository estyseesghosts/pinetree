#!/usr/bin/env python3
"""Bootstrap the 5-node bench matrix: 6 accounts per instance, all password123.

Nodes: pt-ks (pinetree/keystone), pt-mk (pinetree/misskey), misskey (misskey/keystone),
catodon (catodon), akkoma (akkoma/pleroma-fe).
Writes $BENCH_DIR/accounts.json with {node: [{user, token}]}.
"""
import os
BD = os.environ["BENCH_DIR"]
import json, subprocess, sys, time, urllib.request, urllib.parse, urllib.error

PASS = "password123"
N_USERS = 6

MISSKEY_FAMILY = {  # node -> (base url, user prefix)
    "pt-ks":  ("http://pt-ks.test",  "pk"),
    "pt-mk":  ("http://pt-mk.test",  "pm"),
    "misskey":("http://misskey.test","mk"),
    "catodon":("http://catodon.test","cd"),
}

def req(base, ep, body=None, token=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    h = {"content-type": "application/json"}
    if token: h["authorization"] = f"Bearer {token}"
    r = urllib.request.Request(f"{base}/api/{ep}", data=data, headers=h, method=method or ("POST" if data else "GET"))
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try: out = json.loads(e.read())
        except Exception: out = None
        return e.code, out

out = {}
errors = []

for node, (base, prefix) in MISSKEY_FAMILY.items():
    users = []
    for i in range(1, N_USERS + 1):
        u = f"{prefix}{i}"
        if node in ("misskey", "catodon") and i == 1:
            # first account: no-auth bootstrap, becomes admin
            s, v = req(base, "admin/accounts/create", {"username": u, "password": PASS})
            if s != 200:
                errors.append(f"{node} admin/accounts/create {u}: {s} {v}")
                continue
            s, v = req(base, "signin-flow", {"username": u, "password": PASS})
            tok = v.get("i") if s == 200 else None
        else:
            s, v = req(base, "signup", {"username": u, "password": PASS})
            if s != 200:
                errors.append(f"{node} signup {u}: {s} {v}")
                continue
            if node in ("misskey", "catodon"):
                s2, v2 = req(base, "signin-flow", {"username": u, "password": PASS})
                tok = v2.get("i") if s2 == 200 else None
                if not tok: errors.append(f"{node} signin-flow {u}: {s2} {v2}")
            else:
                tok = v.get("token")
        if tok:
            users.append({"user": u, "token": tok})
        time.sleep(0.2)
    # enable federation (misskey family default is none) via the admin account
    if users and node in ("misskey", "catodon"):
        s, v = req(base, "admin/update-meta", {"federation": "all"}, token=users[0]["token"])
        if s != 200:
            errors.append(f"{node} update-meta federation=all: {s} {v}")
    out[node] = users
    print(f"{node}: {len(users)}/{N_USERS} accounts", flush=True)

# akkoma: mix-created users + OAuth password grants
AKK = "http://akkoma.test"
for i in range(1, N_USERS + 1):
    u = f"ak{i}"
    r = subprocess.run(["docker", "exec", "local-federation-akkoma-1", "mix", "pleroma.user",
                        "new", u, f"{u}@akkoma.test", "--password", PASS, "-y"],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0 and "already exists" not in (r.stdout + r.stderr):
        errors.append(f"akkoma user {u}: {r.stdout[-200:]} {r.stderr[-200:]}")
    subprocess.run(["docker", "exec", "local-federation-akkoma-1", "mix", "pleroma.user",
                    "set", u, "--confirmed", "--approved"],
                   capture_output=True, text=True, timeout=60)
users = []
s, app = None, None
try:
    with urllib.request.urlopen(urllib.request.Request(AKK + "/api/v1/apps",
        data=json.dumps({"client_name": "bench", "redirect_uris": "urn:ietf:wg:oauth:2.0:oob",
                         "scopes": "read write follow"}).encode(),
        headers={"content-type": "application/json"}, method="POST"), timeout=20) as r:
        app = json.loads(r.read())
except Exception as e:
    errors.append(f"akkoma apps: {e}")
if app:
    for i in range(1, N_USERS + 1):
        u = f"ak{i}"
        try:
            with urllib.request.urlopen(urllib.request.Request(AKK + "/oauth/token",
                data=urllib.parse.urlencode({"grant_type": "password", "client_id": app["client_id"],
                    "client_secret": app["client_secret"], "username": f"{u}@akkoma.test",
                    "password": PASS, "scope": "read write follow"}).encode(),
                headers={"content-type": "application/x-www-form-urlencoded"}, method="POST"), timeout=20) as r:
                tok = json.loads(r.read())["access_token"]
                users.append({"user": u, "token": tok})
        except Exception as e:
            errors.append(f"akkoma token {u}: {e}")
out["akkoma"] = users
print(f"akkoma: {len(users)}/{N_USERS} accounts", flush=True)

with open(os.path.join(BD, "accounts.json"), "w") as f:
    json.dump(out, f, indent=1)

if errors:
    print("ERRORS:")
    for e in errors: print(" ", e)
    sys.exit(1)
print("BOOTSTRAP_OK")
