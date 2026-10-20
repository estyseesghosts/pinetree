"""Normalized adapters for the 10-node bench matrix.

Nodes:
  pt-a    pinetree    Misskey API subset   http://pt-a.test
  pt-b    pinetree    Misskey API subset   http://pt-b.test
  pt-c    pinetree    Misskey API subset   http://pt-c.test
  bt-a    birchtree   Mastodon API v1      http://bt-a.lab
  bt-b    birchtree   Mastodon API v1      http://bt-b.lab
  bt-c    birchtree   Mastodon API v1      http://bt-c.lab
  misskey Misskey     Misskey API          http://misskey.test
  catodon Catodon     Misskey API          http://catodon.test
  akkoma  Akkoma      Mastodon API v1      http://akkoma.test
  mastodon Mastodon   Mastodon API v1      https://mastodon.lab

Accounts: $BENCH_DIR/accounts.json (BENCH_DIR is required; use /root/bench/<run-id>)  {node: [{user, token}]}.
Every method is normalized; raw fields kept under .raw where useful.
"""
import io, json, os, sys, time, urllib.request, urllib.parse, urllib.error, hashlib

BENCH_DIR = os.environ.get("BENCH_DIR") or sys.exit("BENCH_DIR must be set (e.g. /root/bench/<run-id>); the harness never defaults to /tmp")
PASS = "password123"
HOSTS = {
    "pt-a": "pt-a.test",
    "pt-b": "pt-b.test",
    "pt-c": "pt-c.test",
    "bt-a": "bt-a.lab",
    "bt-b": "bt-b.lab",
    "bt-c": "bt-c.lab",
    "misskey": "misskey.test",
    "catodon": "catodon.test",
    "akkoma": "akkoma.test",
    "mastodon": "mastodon.lab",
}
# Pinetree checks the Host header; use 127.0.0.1 + explicit Host header
NODE_OVERRIDE = {
    "pt-a": ("127.0.0.1:3000", "pt-a.test"),
    "pt-b": ("127.0.0.1:3103", "pt-b.test"),
    "pt-c": ("127.0.0.1:3105", "pt-c.test"),
}
MISSKEY_FAMILY = {"pt-a", "pt-b", "pt-c", "misskey", "catodon"}
MASTODON_FAMILY = {"bt-a", "bt-b", "bt-c", "akkoma", "mastodon"}
NODES = list(HOSTS)

def _req(method, url, body=None, token=None, form=None, headers=None, timeout=30, host_header=None):
    h = {"Accept": "application/json"}
    if host_header:
        h["Host"] = host_header
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        h["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
            h["Content-Type"] = "application/json"
        else:
            data = body  # raw bytes (multipart caller sets header)
    if token:
        h["Authorization"] = f"Bearer {token}"
    if headers:
        h.update(headers)
    last = None
    for attempt in range(4):
        r = urllib.request.Request(url, data=data, headers=h, method=method)
        t_start = time.time()
        try:
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                raw = resp.read()
                ct = resp.headers.get("Content-Type", "")
                _trace(method, url, resp.status, t_start, attempt)
                if "json" in ct and raw:
                    return resp.status, json.loads(raw)
                return resp.status, raw.decode(errors="replace") if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            _trace(method, url, e.code, t_start, attempt)
            try: out = json.loads(raw) if raw else None
            except Exception: out = raw.decode(errors="replace")[:400]
            if e.code == 429:  # rate limited: back off and retry
                time.sleep(2 * (attempt + 1)); last = (e.code, out); continue
            return e.code, out
        except Exception as e:
            _trace(method, url, "exc", t_start, attempt)
            last = ("exc", str(e)); time.sleep(1)
    return last

# Per-call timing: set BENCH_TRACE=<file> to append one JSON line per HTTP attempt
# {ts, method, url (no query), status, ms, attempt}. Off by default.
TRACE_PATH = os.environ.get("BENCH_TRACE")
_trace_lock = __import__("threading").Lock()

def _trace(method, url, status, t_start, attempt):
    if not TRACE_PATH: return
    rec = {"ts": round(t_start, 3), "method": method, "url": url.split("?")[0], "status": status,
           "ms": round((time.time() - t_start) * 1000, 1), "attempt": attempt}
    with _trace_lock, open(TRACE_PATH, "a") as f:
        f.write(json.dumps(rec) + "\n")

def _multipart(fields, files):
    """files: {name: (filename, bytes, content_type)}"""
    b = "----bench" + str(int(time.time()))
    out = io.BytesIO()
    for k, v in fields.items():
        out.write(f"--{b}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode())
    for k, (fn, data, ct) in files.items():
        out.write(f"--{b}\r\nContent-Disposition: form-data; name=\"{k}\"; filename=\"{fn}\"\r\n"
                  f"Content-Type: {ct}\r\n\r\n".encode())
        out.write(data); out.write(b"\r\n")
    out.write(f"--{b}--\r\n".encode())
    return out.getvalue(), {"Content-Type": f"multipart/form-data; boundary={b}"}

class User:
    def __init__(self, node, name, token):
        self.node, self.name, self.token = node, name, token
    @property
    def acct(self):
        return f"{self.name}@{HOSTS[self.node]}"

class Node:
    def __init__(self, node, accounts):
        self.node = node
        if node in NODE_OVERRIDE:
            addr, self.host_header = NODE_OVERRIDE[node]
            self.base = f"http://{addr}"
        else:
            self.base = f"http://{HOSTS[node]}"
            self.host_header = None
        self.family = node in MISSKEY_FAMILY
        self.users = [User(node, a["user"], a["token"]) for a in accounts]
        self._ids = {}  # "user@host" -> local user id
        import threading
        self._idx, self._idx_ts, self._idx_lock = None, 0.0, threading.Lock()

    # ---- low level ----
    def api(self, ep, body, user=None):
        return _req("POST", f"{self.base}/api/{ep}", body=body,
                    token=user.token if user else None, host_header=self.host_header)
    def mapi(self, method, path, body=None, user=None):
        return _req(method, f"{self.base}/api/v1{path}", body=body,
                    token=user.token if user else None, host_header=self.host_header)

    def mapi2(self, method, path, body=None, user=None):
        """Mastodon API v2 (search). Birchtree serves only /api/v2/search; /api/v1/search is a 404."""
        return _req(method, f"{self.base}/api/v2{path}", body=body,
                    token=user.token if user else None, host_header=self.host_header)

    # ---- identity ----
    def local_id(self, user):
        if self.family:
            s, v = self.api("users/show", {"username": user.name}, user)
            assert s == 200, (self.node, "users/show self", s, v)
            return v["id"]
        s, v = self.mapi("GET", "/accounts/verify_credentials", user=user)
        assert s == 200, (self.node, "verify_credentials", s, v)
        return v["id"]

    def resolve(self, acct, user):
        """Resolve 'name@host' to this node's local user id (fetching remotely if needed)."""
        if acct in self._ids: return self._ids[acct]
        name, host = acct.rsplit("@", 1)
        if self.family:
            if ":" in host:
                # Misskey's users/show drops the port and WebFingers `name@` with an empty host
                # (EBADNAME), so a host:port peer (the birchtree nodes) is resolved by actor URI.
                s, v = self.api("ap/show", {"uri": f"http://{host}/users/{name}"}, user)
                if s == 200 and isinstance(v, dict) and v.get("type") == "User":
                    self._ids[acct] = v["object"]["id"]; return v["object"]["id"]
                raise AssertionError((self.node, "resolve", acct, s, str(v)[:200]))
            for attempt in range(2):
                s, v = self.api("users/show", {"username": name, "host": host}, user)
                if s == 200: break
                # Misskey's first lookups of one new remote server can collide on a duplicate-key
                # insert; the second attempt succeeds. A real failure fails both.
                time.sleep(2)
            assert s == 200, (self.node, "resolve", acct, s, v)
            self._ids[acct] = v["id"]; return v["id"]
        for attempt in range(5):
            s, v = self.mapi("GET", "/accounts/lookup?" + urllib.parse.urlencode({"acct": acct}), user=user)
            if s == 200 and isinstance(v, dict) and v.get("id"):
                self._ids[acct] = v["id"]; return v["id"]
            s, v = self.mapi2("GET", "/search?" + urllib.parse.urlencode(
                {"q": acct, "resolve": "true", "type": "accounts"}), user=user)
            if s == 200 and isinstance(v, dict):
                accs = v.get("accounts", [])
                if accs:
                    self._ids[acct] = accs[0]["id"]; return accs[0]["id"]
            time.sleep(4 * (attempt + 1))
        raise AssertionError((self.node, "resolve", acct, s, str(v)[:200]))

    # ---- follows ----
    def follow(self, user, acct):
        rid = self.resolve(acct, user)
        if self.family:
            s, v = self.api("following/create", {"userId": rid}, user)
            code = ((v or {}).get("error") or {}).get("code") if isinstance(v, dict) else None
            if code in ("ALREADY_FOLLOWING",): s = 200
            return s, v
        return self.mapi("POST", f"/accounts/{rid}/follow", body={}, user=user)

    def followers_accts(self, uid, user, max_pages=20):
        """Every follower of local account `uid` as a set of 'name@host'. Mastodon-API nodes cap a page
        at 80, so follow the Link rel=next header; the Misskey family returns up to 100 per page and
        pages by untilId."""
        out = set()
        if self.family:
            until = None
            for _ in range(max_pages):
                body = {"userId": uid, "limit": 100}
                if until: body["untilId"] = until
                s, v = self.api("users/followers", body, user)
                if s != 200 or not v: break
                for f in v:
                    fu = f.get("follower") if isinstance(f.get("follower"), dict) else f
                    out.add(f"{fu.get('username')}@{fu.get('host') or HOSTS[self.node]}")
                until = v[-1].get("id")
                if len(v) < 100 or not until: break
            return out
        url = f"{self.base}/api/v1/accounts/{uid}/followers?limit=80"
        for _ in range(max_pages):
            h = {"Accept": "application/json", "Authorization": f"Bearer {user.token}"}
            if self.host_header: h["Host"] = self.host_header
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=30) as resp:
                    page = json.loads(resp.read() or b"[]")
                    link = resp.headers.get("Link", "")
            except Exception:
                break
            out.update(f["acct"] for f in page)
            m = [p.split(";")[0].strip(" <>") for p in link.split(",") if 'rel="next"' in p]
            if not page or not m: break
            # Keep our own scheme and host: birchtree sends a relative Link target, and Mastodon an https://
            # one while the harness reaches it over http (it does not trust the lab CA).
            nxt = urllib.parse.urlsplit(m[0])
            url = self.base + nxt.path + ("?" + nxt.query if nxt.query else "")
        return out

    def following_count(self, user):
        if self.family:
            s, v = self.api("users/show", {"username": user.name}, user); return (v or {}).get("followingCount")
        s, v = self.mapi("GET", "/accounts/verify_credentials", user=user); return (v or {}).get("following_count")

    # ---- posting ----
    def post(self, user, text=None, cw=None, visibility="public", reply_to=None,
             renote_of=None, quote_of=None, media=None, poll=None):
        """reply_to/renote_of/quote_of are LOCAL note ids on this node.
        media: list of local drive/attachment ids. poll: {'choices': [...], 'expires_in': sec}."""
        if self.family:
            body = {"visibility": visibility}
            if text: body["text"] = text
            if cw: body["cw"] = cw
            if reply_to: body["replyId"] = reply_to
            if renote_of: body["renoteId"] = renote_of
            if quote_of: body["renoteId"] = quote_of  # misskey quote = renote + text
            if media: body["fileIds"] = media
            if poll:
                body["poll"] = {"choices": poll["choices"], "multiple": False,
                                "expiredAfter": int(poll.get("expires_in", 3600) * 1000)}
            s, v = self.api("notes/create", body, user)
            assert s == 200, (self.node, "post", s, v)
            return v["createdNote"]["id"]
        body = {"status": text or "", "visibility":
                {"public": "public", "followers": "private", "home": "unlisted"}.get(visibility, visibility)}
        if cw: body["spoiler_text"] = cw
        if reply_to:
            uri = self.note_uri(reply_to, user)
            body["in_reply_to_id"] = reply_to if self._looks_like_local_id(reply_to) else None
            if body["in_reply_to_id"] is None:
                # akkoma ids ARE local ids (numeric); reply_to is always an akkoma id here
                body["in_reply_to_id"] = reply_to
        if media: body["media_ids"] = media
        if poll:
            body["poll"] = {"options": poll["choices"], "multiple": False,
                            "expires_in": int(poll.get("expires_in", 3600))}
        s, v = self.mapi("POST", "/statuses", body=body, user=user)
        assert s == 200, (self.node, "post", s, v)
        return v["id"]

    @staticmethod
    def _looks_like_local_id(x): return True

    def note_uri(self, note_id, user):
        s, v = (self.api("notes/show", {"noteId": note_id}, user) if self.family
                else self.mapi("GET", f"/statuses/{note_id}", user=user))
        assert s == 200, (self.node, "note/show", note_id, s, v)
        return (v.get("uri") or v.get("url") or
                f"{self.base}/{('notes/' if self.family else 'statuses/')}{note_id}")

    # ---- reactions ----
    def react(self, user, note_id, reaction="🎉"):
        if self.family:
            s, v = self.api("notes/reactions/create", {"noteId": note_id, "reaction": reaction}, user)
            return s, v
        return self.mapi("POST", f"/statuses/{note_id}/favourite", body={}, user=user)

    def unreac(self, user, note_id):
        """Misskey: delete own reaction (notes/reactions/delete). Akkoma: unfavourite."""
        if self.family:
            return self.api("notes/reactions/delete", {"noteId": note_id}, user)
        return self.mapi("POST", f"/statuses/{note_id}/unfavourite", body={}, user=user)

    def reactions(self, note_id, user):
        """-> {reaction_name: [acct,...]}. Misskey 2026 packs the reaction under
        `type`; older shape used `reaction`. Pinetree has no notes/reactions
        endpoint (404 UNKNOWN_API_ENDPOINT) — fall back to the note's own
        reaction map (count-level, no per-user identity)."""
        if self.family:
            s, v = self.api("notes/reactions", {"noteId": note_id, "limit": 100}, user)
            if s == 200 and isinstance(v, list):
                out = {}
                for r in v:
                    u = r.get("user") or {}
                    acct = f"{u.get('username')}@{u.get('host') or HOSTS[self.node]}"
                    key = r.get("type") or r.get("reaction") or "?"
                    out.setdefault(key, []).append(acct)
                return out
            # pinetree fallback: count-level from notes/show
            s, v = self.api("notes/show", {"noteId": note_id}, user)
            if s == 200 and v is not None:
                return dict(v.get("reactions") or {})
            return {}
        s, v = self.mapi("GET", f"/statuses/{note_id}/favourited_by", user=user)
        assert s == 200, (self.node, "favourited_by", note_id, s, v)
        return {"❤": [a.get("acct") for a in v]}

    # ---- renote/boost ----
    def boost(self, user, note_id):
        if self.family:
            s, v = self.api("notes/create", {"renoteId": note_id}, user)
            assert s == 200, (self.node, "renote", s, v); return v["createdNote"]["id"]
        s, v = self.mapi("POST", f"/statuses/{note_id}/reblog", body={}, user=user)
        assert s == 200, (self.node, "reblog", s, v); return v["id"]

    def renote_count(self, note_id, user):
        if self.family:
            s, v = self.api("notes/show", {"noteId": note_id}, user)
            return (v or {}).get("renoteCount", 0)
        s, v = self.mapi("GET", f"/statuses/{note_id}", user=user)
        return (v or {}).get("reblogs_count", 0)

    # ---- notifications ----
    def notifications(self, user, limit=20):
        """Misskey-family: `i/notifications` (pinetree + misskey; the bare
        `notifications` name does not exist). Akkoma: /api/v1/notifications."""
        if self.family:
            s, v = self.api("i/notifications", {"limit": limit}, user)
            return v if s == 200 and isinstance(v, list) else []
        s, v = self.mapi("GET", f"/notifications?limit={limit}", user=user)
        return v if s == 200 and isinstance(v, list) else []

    # ---- polls ----
    def poll_vote(self, user, note_id, choice):
        if self.family:
            return self.api("notes/polls/vote", {"noteId": note_id, "choice": choice}, user)
        # akkoma: the vote URL uses the POLL's own id, not the status id
        s, v = self.mapi("GET", f"/statuses/{note_id}", user=user)
        if not isinstance(v, dict): return (s if isinstance(s, int) else 400, f"non-json: {str(v)[:80]}")
        pid = ((v or {}).get("poll") or {}).get("id")
        if not pid: return (404, "no poll on status")
        # Mastodon/Akkoma/birchtree require plural `choices` (verified against
        # Mastodon 4.7.3 2026-10-19: singular `choice` answers 400
        # "param is missing or the value is empty or invalid: choices").
        return self.mapi("POST", f"/polls/{pid}/votes", body={"choices": [choice]}, user=user)

    def poll(self, note_id, user):
        """-> {'choices': [{'text','votes'}], 'expired': bool} or None.
        Misskey/pinetree: poll.choices[].votes (no top-level votes array).
        Akkoma: poll.id + options[].{title,votes_count}."""
        if self.family:
            s, v = self.api("notes/show", {"noteId": note_id}, user)
            p = (v or {}).get("poll")
            if not p: return None
            return {"choices": [{"text": c.get("text"), "votes": c.get("votes")}
                                for c in p.get("choices", [])],
                    "expired": p.get("expired", False)}
        s, v = self.mapi("GET", f"/statuses/{note_id}", user=user)
        if not isinstance(v, dict): return None
        p = (v or {}).get("poll")
        if not p: return None
        return {"choices": [{"text": o.get("title"), "votes": o.get("votes_count")}
                            for o in p.get("options", [])],
                "expired": p.get("expired", False)}

    # ---- media ----
    def upload(self, user, data: bytes, name="bench.png", ctype="image/png"):
        if self.family:
            body, hdrs = _multipart({"i": ""}, {})  # placeholder, replaced below
            body, hdrs = _multipart({"i": user.token}, {"file": (name, data, ctype)})
            s, v = _req("POST", f"{self.base}/api/drive/files/create", body=body, headers=hdrs)
            assert s == 200, (self.node, "upload", s, v)
            return v["id"], v.get("url")
        body, hdrs = _multipart({}, {"file": (name, data, ctype)})
        s, v = _req("POST", f"{self.base}/api/v1/media", body=body, headers=hdrs,
                    token=user.token)
        assert s == 200, (self.node, "upload", s, v)
        return v["id"], v.get("url") or v.get("remote_url")

    # ---- notes ----
    def note(self, note_id, user):
        if self.family:
            s, v = self.api("notes/show", {"noteId": note_id}, user)
        else:
            s, v = self.mapi("GET", f"/statuses/{note_id}", user=user)
        return v if s == 200 else None

    def note_by_uri(self, uri, user):
        """Resolve uri to a local note id (misskey-family: ap/show; akkoma: search,
        falling back to a home-timeline scan — this rig's Akkoma has no search
        backend, so /api/v1/search answers 404 Not implemented)."""
        if self.family:
            s, v = self.api("ap/show", {"uri": uri}, user)
            if s == 200 and v.get("type") == "Note": return v["object"] if "object" in v else v
            return None
        s, v = self.mapi2("GET", "/search?" + urllib.parse.urlencode({"q": uri, "resolve": "true"}), user=user)
        if isinstance(v, dict) and v.get("statuses"):
            return v["statuses"][0]
        # fallback: look the uri up in a cached index of every local user's home timeline
        return self._timeline_lookup(uri)

    INDEX_TTL_S = 20
    INDEX_PAGES = 32  # x40 = 1280 notes per user; the old 10-page window missed the oldest S1 notes

    def _timeline_lookup(self, uri):
        """uri/url -> note over all local users' home timelines. The index is built once and
        rebuilt on a miss at most every INDEX_TTL_S, so a miss costs one scan, not one per call."""
        with self._idx_lock:
            for attempt in (0, 1):
                if attempt == 1 or self._idx is None:
                    if self._idx is not None and time.time() - self._idx_ts < self.INDEX_TTL_S:
                        return None
                    self._idx, self._idx_ts = self._build_index(), time.time()
                hit = self._idx.get(uri)
                if hit: return hit
        return None

    def _build_index(self):
        from concurrent.futures import ThreadPoolExecutor
        def scan(u):
            out, mx = [], None
            for _ in range(self.INDEX_PAGES):
                q = "/timelines/home?limit=40" + (f"&max_id={mx}" if mx else "")
                s, tl = self.mapi("GET", q, user=u)
                if s != 200 or not tl: break
                out.extend(tl); mx = tl[-1]["id"]
            return out
        idx = {}
        with ThreadPoolExecutor(max_workers=8) as ex:
            for items in ex.map(scan, self.users):
                for n in items:
                    for k in (n.get("uri"), n.get("url")):
                        if k: idx.setdefault(k, n)
        return idx

    def uri_of(self, note_id, user):
        v = self.note(note_id, user)
        if not v: return None
        if self.family:
            return v.get("uri") or f"{self.base}/notes/{note_id}"
        return v.get("uri") or v.get("url")

    def delete(self, user, note_id):
        if self.family:
            return self.api("notes/delete", {"noteId": note_id}, user)
        return self.mapi("DELETE", f"/statuses/{note_id}", user=user)

    # ---- timelines ----
    def home(self, user, limit=30, pages=1):
        out = []
        if self.family:
            until = None
            for _ in range(pages):
                body = {"limit": limit}
                if until: body["untilId"] = until
                s, v = self.api("notes/timeline", body, user)
                assert s == 200, (self.node, "home", s, v)
                if not v: break
                out.extend(v)
                until = v[-1]["id"]
        else:
            url = f"/timelines/home?limit={limit}"
            for _ in range(pages):
                s, v = self.mapi("GET", url, user=user)
                assert s == 200, (self.node, "home", s, v)
                if not v: break
                out.extend(v)
                url = f"/timelines/home?limit={limit}&max_id={v[-1]['id']}"
        return out

def load_nodes():
    accs = json.load(open(os.path.join(BENCH_DIR, "accounts.json")))
    return {n: Node(n, accs[n]) for n in NODES if n in accs}

def preflight(nodes, quiet=False):
    """Fail fast on stale state (missing accounts, dead tokens, unreachable nodes) before a suite
    starts, so it is not recorded as a federation failure. Returns a list of problems."""
    from concurrent.futures import ThreadPoolExecutor
    def check(nu):
        node, u = nu
        try:
            node.local_id(u); return None
        except Exception as e:
            return f"{node.node}/{u.name}: {str(e)[:160]}"
    jobs = [(n, u) for n in nodes.values() for u in n.users]
    with ThreadPoolExecutor(max_workers=8) as ex:
        problems = [p for p in ex.map(check, jobs) if p]
    if problems and not quiet:
        print(f"PREFLIGHT FAILED: {len(problems)} of {len(jobs)} accounts unusable", flush=True)
        for p in problems[:20]: print("  " + p, flush=True)
    return problems

def build_ids():
    """{port: {pid, exe, sha256, mtime, cmdline}} for each local listening node process, so a run
    can be tied to the binary that served it. Linux only; empty where /proc is unavailable."""
    import re, subprocess
    out = {}
    try: ss = subprocess.run(["ss", "-ltnpH"], capture_output=True, text=True, timeout=10).stdout
    except Exception: return out
    ports = {int(addr.split(":")[1]) for addr, _ in NODE_OVERRIDE.values()} | {8080, 8081, 8082}
    for line in ss.splitlines():
        m = re.search(r":(\d+)\s.*pid=(\d+)", line)
        if not m or int(m.group(1)) not in ports: continue
        port, pid = int(m.group(1)), m.group(2)
        try:
            exe = os.path.realpath(f"/proc/{pid}/exe")
            out[port] = {"pid": int(pid), "exe": exe, "mtime": int(os.path.getmtime(exe)),
                         "sha256": hashlib.sha256(open(exe, "rb").read()).hexdigest()[:16],
                         "cmdline": open(f"/proc/{pid}/cmdline").read().replace("\0", " ").strip()}
        except Exception:
            continue
    return out

def run_header(suite, nodes):
    """Preflight + build ids for one suite run. Prints and returns the header; exits 3 when
    preflight fails unless BENCH_SKIP_PREFLIGHT=1."""
    import sys
    hdr = {"suite": suite, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "builds": build_ids()}
    print("RUN_HEADER " + json.dumps(hdr), flush=True)
    if not os.environ.get("BENCH_SKIP_PREFLIGHT") and preflight(nodes):
        sys.exit(3)
    return hdr

def sha(b): return hashlib.sha256(b).hexdigest()[:16]

LAB_CA = "/root/parity/lab/certs/lab-ca.crt"

def fetch_bytes(url, timeout=30):
    try:
        if url.startswith("https://") and os.path.exists(LAB_CA):
            import ssl
            ctx = ssl.create_default_context(cafile=LAB_CA)
            with urllib.request.urlopen(url, timeout=timeout, context=ctx) as r:
                return r.status, r.read()
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:200]
    except Exception as e:
        return "exc", str(e)
