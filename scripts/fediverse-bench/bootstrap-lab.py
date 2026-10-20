#!/usr/bin/env python3
"""Create u0..u9 on Misskey, Catodon, Akkoma (mix + password grant) and Mastodon (rails runner + Doorkeeper token),
plus `alice` on Mastodon (used by phase6-interop-3node.sh). Merges into $BENCH_DIR/accounts.json.
Docker containers: local-federation-akkoma-1, bt-mastodon.
"""
import json, os, subprocess, sys, urllib.parse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import BENCH_DIR, PASS, _req

N_USERS = int(os.environ.get("N_USERS", "10"))
path = os.path.join(BENCH_DIR, "accounts.json")
acc = json.load(open(path)) if os.path.exists(path) else {}

def akkoma():
    users = []
    s, app = _req("POST", "http://127.0.0.1:4000/api/v1/apps", host_header="akkoma.test",
                  form={"client_name": "bench", "redirect_uris": "urn:ietf:wg:oauth:2.0:oob", "scopes": "read write follow"})
    assert s == 200, (s, app)
    for i in range(N_USERS):
        u = f"u{i}"
        r = subprocess.run(["docker", "exec", "local-federation-akkoma-1", "mix", "pleroma.user", "new", u, f"{u}@akkoma.test",
                            "--password", PASS, "-y"], capture_output=True, text=True, timeout=120)
        created = f"User {u} created" in r.stdout  # else it may already exist; the token grant below decides
        s, v = _req("POST", "http://127.0.0.1:4000/oauth/token", host_header="akkoma.test",
                    form={"grant_type": "password", "client_id": app["client_id"], "client_secret": app["client_secret"],
                          "username": u, "password": PASS, "scope": "read write follow"})
        assert s == 200 and "access_token" in v, (u, "created" if created else "create failed", s, v)
        users.append({"user": u, "token": v["access_token"]})
    return users

def misskey_family(base):
    """u0 becomes admin via the no-auth bootstrap; the admin creates the rest. Federation is switched to `all`."""
    users = []
    admin_tok = None
    for i in range(N_USERS):
        u = f"u{i}"
        # Registration closes once the first (admin) account exists, so the rest are made by the admin.
        s, v = _req("POST", f"{base}/api/admin/accounts/create", body={"username": u, "password": PASS}, token=admin_tok)
        created = s == 200
        tok = v.get("token") if created and v else None  # Catodon's signin-flow demands a captcha
        if not tok:
            s, v = _req("POST", f"{base}/api/signin-flow", body={"username": u, "password": PASS})
            tok = v.get("i") if s == 200 and v else None
        assert tok, (base, u, "created" if created else "create failed", "signin-flow", s, v)
        if i == 0: admin_tok = tok
        users.append({"user": u, "token": tok})
    s, v = _req("POST", f"{base}/api/admin/update-meta", body={"federation": "all"}, token=users[0]["token"])
    assert s in (200, 204), (base, "update-meta", s, v)
    return users

RUBY = """
app = Doorkeeper::Application.find_or_create_by!(name: 'bench') { |a| a.redirect_uri = 'urn:ietf:wg:oauth:2.0:oob'; a.scopes = 'read write follow' }
out = []
(%s).each do |name|
  acct = Account.find_by(username: name, domain: nil)
  user = acct&.user
  unless user
    user = User.new(email: "#{name}@bench.invalid", password: '%s', agreement: true, bypass_registration_checks: true,
                    account_attributes: { username: name })
    user.save!(validate: false)
    user.confirm
    user.approve!
  end
  t = Doorkeeper::AccessToken.create!(application: app, resource_owner_id: user.id, scopes: 'read write follow')
  out << { user: name, token: t.token }
end
puts 'BENCH_JSON=' + out.to_json
"""

def mastodon():
    names = ", ".join(f"'u{i}'" for i in range(N_USERS)) + ", 'alice'"
    r = subprocess.run(["docker", "exec", "bt-mastodon", "bin/rails", "runner", RUBY % (f"[{names}]", PASS)],
                       capture_output=True, text=True, timeout=600)
    line = [l for l in r.stdout.splitlines() if l.startswith("BENCH_JSON=")]
    if not line: raise RuntimeError(f"mastodon: {r.stdout[-400:]} {r.stderr[-400:]}")
    return json.loads(line[0][len("BENCH_JSON="):])

FNS = {"akkoma": akkoma, "mastodon": mastodon,
       "misskey": lambda: misskey_family("http://misskey.test"), "catodon": lambda: misskey_family("http://catodon.test")}
which = sys.argv[1:] or list(FNS)
for n in which:
    acc[n] = FNS[n]()
    print(n, len(acc[n]), flush=True)
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f: json.dump(acc, f, indent=1)
