"""Scratch-only patch: writes a harness-fixed copy of phase-stress.py.

Fixes the four harness counters documented in state/logs/2026-10-17-bench-fixes-p6-and-reverify.md
and docs/handoff-fediverse-bench-fixes.md. Product code is not touched. The repo copy is left as-is.
  S1 dupes:  count only duplicates with a post-restart createdAt (+ dupes of this run's tag)
  S2 204:    204 counts as success on react and undo
  S3 votes:  204 counts as success; per-origin attempts/ok/tally summary
  S4 check:  eventual attachment check (poll up to 60 s after resolve), per-destination summary
"""
import sys

SRC = "/root/pinetree/scripts/fediverse-bench/phase-stress.py"
DST = "/tmp/opencode/bench-rerun/scripts/phase-stress.py"
s = open(SRC).read()

PATCHES = [
(
'''        if s != 200: return ("react_fail", s, v)''',
'''        if s not in (200, 204): return ("react_fail", s, v)  # 204 = success (harness fix)''',
),
(
'''            elif out[0][0] in ("unresolved", "exc"):''',
'''            elif out[0] in ("unresolved", "exc"):''',
),
(
'''        if s == 200: undon += 1''',
'''        if s in (200, 204): undon += 1''',
),
(
'''    ok = sum(1 for o in outs if o and o[2] == 200)''',
'''    ok = sum(1 for o in outs if o and o[2] in (200, 204))''',
),
(
'''        if o and o[2] == 200:
            expect[o[1]] = expect.get(o[1], 0) + 1''',
'''        if o and o[2] in (200, 204):
            expect[o[1]] = expect.get(o[1], 0) + 1''',
),
(
'''    R["phases"]["S3_poll_storm"] = {
        "polls": len(polls), "votes_attempted": len(jobs), "votes_ok": ok,''',
'''    by_origin = {}
    for src, _su, nid, _uri in polls:
        by_origin.setdefault(src, {"attempts_non_akkoma": 0, "ok": 0, "tallied": 0})
        by_origin[src]["tallied"] += tallied.get(nid) or 0
    for (rn, _ru, pi) in jobs:
        if rn != "akkoma": by_origin[polls[pi][0]]["attempts_non_akkoma"] += 1
    for o in outs:
        if o and o[2] in (200, 204): by_origin[o[0]]["ok"] += 1
    R["phases"]["S3_poll_storm"] = {
        "polls": len(polls), "votes_attempted": len(jobs), "votes_ok": ok, "by_origin": by_origin,''',
),
(
'''def s4():''',
'''ATT_LAT = []

def s4():''',
),
(
'''        local = nodes[dst].note_by_uri(uri, du)
        if not local: return (src, dst, "unresolved")
        v = nodes[dst].note(local, du)
        files = (v or {}).get("files") or (v or {}).get("attachments") or []
        if not files: return (src, dst, "no-attachment")''',
'''        t_res = time.time()
        local = nodes[dst].note_by_uri(uri, du)
        if not local: return (src, dst, "unresolved")
        # eventual check (P5): attachment linking is async; poll up to 60 s after resolve
        while True:
            v = nodes[dst].note(local, du)
            files = (v or {}).get("files") or (v or {}).get("attachments") or []
            if files or time.time() - t_res > 60: break
            time.sleep(1)
        if not files: return (src, dst, "no-attachment-60s")
        ATT_LAT.append(round(time.time() - t_res, 2))''',
),
(
'''    bad = [c for c in checks if c[2] != "ok"]
    R["phases"]["S4_media_burst"] = {
        "uploads": len(outs), "verified_ok": len(checks) - len(bad),''',
'''    bad = [c for c in checks if c[2] != "ok"]
    by_dst = {}
    for c in checks: by_dst.setdefault(c[1], {"ok": 0, "bad": 0})["ok" if c[2] == "ok" else "bad"] += 1
    R["phases"]["S4_media_burst"] = {
        "uploads": len(outs), "verified_ok": len(checks) - len(bad), "by_dst": by_dst,
        "attach_wait_s": {"n": len(ATT_LAT), "p50": pct(ATT_LAT, 50), "max": max(ATT_LAT) if ATT_LAT else None},''',
),
(
'''            dupes = len(texts) - len({(n.get("uri") or n.get("id")) for n in items})''',
'''            dupes = len(texts) - len({(n.get("uri") or n.get("id")) for n in items})  # legacy: includes pre-restart rows
            dupes_post = dupes_in(items, lambda n: (created_ts(n) or 0) >= RESTART_TS)
            dupes_tag = dupes_in(items, lambda n: f"|{TAG}" in (note_text(dst, n) or ""))''',
),
(
'''            snap[dst] = {"per_src": per_src, "total_markers": sum(per_src.values()),
                         "dupes": dupes, "items": len(items)}''',
'''            snap[dst] = {"per_src": per_src, "total_markers": sum(per_src.values()),
                         "dupes": dupes, "dupes_post_restart": dupes_post,
                         "dupes_this_run": dupes_tag, "items": len(items)}''',
),
(
'''# ---------------- S1 post storm ----------------''',
'''# ---- harness fix helpers (2026-10-17): dupes by createdAt, not raw uri count ----
from datetime import datetime
RESTART_TS = datetime.fromisoformat(os.environ.get("PT_RESTART_UTC", "2026-10-17T17:46:23+00:00")).timestamp()
def created_ts(n):
    v = n.get("createdAt") or n.get("created_at")
    try: return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp() if v else None
    except Exception: return None
def dupes_in(items, pred):
    groups = {}
    for n in items: groups.setdefault(n.get("uri") or n.get("id"), []).append(n)
    return sum(len(g) - 1 for g in groups.values() if len(g) > 1 and any(pred(n) for n in g))

# ---------------- S1 post storm ----------------''',
),
]

for old, new in PATCHES:
    c = s.count(old)
    assert c == 1, (c, old[:90])
    s = s.replace(old, new)
open(DST, "w").write(s)
print("patched", len(PATCHES), "sites ->", DST)
