#!/usr/bin/env python3
"""Normalized adapters for the 5-node bench matrix.

Nodes:
  pt-ks   pinetree    Misskey API subset   http://pt-ks.test
  pt-mk   pinetree    Misskey API subset   http://pt-mk.test
  misskey Misskey     Misskey API          http://misskey.test
  catodon Catodon     Misskey API          http://catodon.test
  akkoma  Akkoma      Mastodon API v1      http://akkoma.test

Accounts: /tmp/opencode/bench/accounts.json  {node: [{user, token}]}.
Every method is normalized; raw fields kept under .raw where useful.
"""
import io, json, time, urllib.request, urllib.parse, urllib.error, hashlib

PASS = "password123"
HOSTS = {"pt-ks": "pt-ks.test", "pt-mk": "pt-mk.test",
         "misskey": "misskey.test", "catodon": "catodon.test", "akkoma": "akkoma.test"}
MISSKEY_FAMILY = {"pt-ks", "pt-mk", "misskey", "catodon"}
NODES = list(HOSTS)

def _req(method, url, body=None, token=None, form=None, headers=None, timeout=30):
    h = {"Accept": "application/json"}
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
        try:
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                raw = resp.read()
                ct = resp.headers.get("Content-Type", "")
                if "json" in ct and raw:
                    return resp.status, json.loads(raw)
                return resp.status, raw.decode(errors="replace") if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            try: out = json.loads(raw) if raw else None
            except Exception: out = raw.decode(errors="replace")[:400]
            if e.code == 429:  # rate limited: back off and retry
                time.sleep(2 * (attempt + 1)); last = (e.code, out); continue
            return e.code, out
        except Exception as e:
            last = ("exc", str(e)); time.sleep(1)
    return last

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
        self.base = f"http://{HOSTS[node]}"
        self.family = node in MISSKEY_FAMILY
        self.users = [User(node, a["user"], a["token"]) for a in accounts]
        self._ids = {}  # "user@host" -> local user id

    # ---- low level ----
    def api(self, ep, body, user=None):
        return _req("POST", f"{self.base}/api/{ep}", body=body,
                    token=user.token if user else None)
    def mapi(self, method, path, body=None, user=None):
        return _req(method, f"{self.base}/api/v1{path}", body=body,
                    token=user.token if user else None)

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
            s, v = self.api("users/show", {"username": name, "host": host}, user)
            assert s == 200, (self.node, "resolve", acct, s, v)
            self._ids[acct] = v["id"]; return v["id"]
        for attempt in range(5):
            s, v = self.mapi("GET", "/accounts/lookup?" + urllib.parse.urlencode({"acct": acct}), user=user)
            if s == 200 and isinstance(v, dict) and v.get("id"):
                self._ids[acct] = v["id"]; return v["id"]
            s, v = self.mapi("GET", "/search?" + urllib.parse.urlencode(
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
        return self.mapi("POST", f"/polls/{pid}/votes", body={"choice": choice}, user=user)

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
        s, v = self.mapi("GET", "/search?" + urllib.parse.urlencode({"q": uri, "resolve": "true"}), user=user)
        if isinstance(v, dict) and v.get("statuses"):
            return v["statuses"][0]
        # fallback: scan local users' home timelines for the uri
        order = [user] + [x for x in self.users if x is not user]
        for u in order:
            mx = None
            for _ in range(10):
                q = "/timelines/home?limit=40" + (f"&max_id={mx}" if mx else "")
                s2, tl = self.mapi("GET", q, user=u)
                if s2 != 200 or not tl: break
                for st in tl:
                    if (st.get("uri") or st.get("url")) == uri:
                        return st
                mx = tl[-1]["id"]
        return None

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
    accs = json.load(open("/tmp/opencode/bench/accounts.json"))
    return {n: Node(n, accs[n]) for n in NODES if n in accs}

def sha(b): return hashlib.sha256(b).hexdigest()[:16]

def fetch_bytes(url, timeout=30):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:200]
    except Exception as e:
        return "exc", str(e)
