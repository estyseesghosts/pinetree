#!/usr/bin/env python3
"""Collect tokens for the seeded u0..u9 accounts on pt-a/b/c and bt-a/b/c (see start-nodes.sh).

Merges into $BENCH_DIR/accounts.json ({node: [{user, token}]}), keeping other nodes' entries.
Pinetree: POST /api/signin. Birchtree: the lab's OAuth PKCE helper (LAB/birchtree_token.py).
"""
import json, os, subprocess, sys
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import BENCH_DIR, PASS, HOSTS, NODE_OVERRIDE, _req

LAB = os.environ.get("LAB", "/root/parity/lab")
N_USERS = int(os.environ.get("N_USERS", "10"))

def pt_token(node, user):
    addr, host = NODE_OVERRIDE[node]
    s, v = _req("POST", f"http://{addr}/api/signin", body={"username": user, "password": PASS}, host_header=host)
    if s != 200 or not v or "i" not in v: raise RuntimeError(f"{node}/{user} signin: {s} {v}")
    return v["i"]

def bt_token(node, user):
    out = subprocess.run([sys.executable, "-I", f"{LAB}/birchtree_token.py", f"http://{HOSTS[node]}", user, PASS,
                          f"{LAB}/certs/lab-ca.crt"], capture_output=True, text=True, timeout=60)
    if out.returncode != 0: raise RuntimeError(f"{node}/{user} oauth: {out.stderr[-200:]}")
    return json.loads(out.stdout)["access_token"]

def one(job):
    node, user = job
    return node, user, (pt_token if node.startswith("pt-") else bt_token)(node, user)

path = os.path.join(BENCH_DIR, "accounts.json")
acc = json.load(open(path)) if os.path.exists(path) else {}
nodes = ["pt-a", "pt-b", "pt-c", "bt-a", "bt-b", "bt-c", "bt-d", "bt-e"]
jobs = [(n, f"u{i}") for n in nodes for i in range(N_USERS)]
res = {n: [] for n in nodes}
with ThreadPoolExecutor(max_workers=6) as ex:
    for node, user, tok in ex.map(one, jobs):
        res[node].append({"user": user, "token": tok})
acc.update(res)
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f: json.dump(acc, f, indent=1)
print({n: len(v) for n, v in acc.items()})
