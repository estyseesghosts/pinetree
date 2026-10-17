"""Scratch-only patch: harness fixes for phase-features.py (2026-10-17 rerun 2).

Same bug class as P3: a wait_until predicate returns a truthy *string* while the condition is
still false, so wait_until stops on its first poll. Affected: reply, boost, unboost. Predicates
now return None until the condition holds. Product code is not touched.
"""
SRC = "/root/pinetree/scripts/fediverse-bench/phase-features.py"
DST = "/tmp/opencode/bench-rerun/scripts/phase-features.py"
s = open(SRC).read()
PATCHES = [
(
'''                return rid == A or f"rid={rid},A={A}"''',
'''                return True if rid == A else None  # was a truthy string while pending (harness bug)''',
),
(
'''            out = wait_until(lambda: (S.renote_count(A, su) or 0) > before or
                             f"count={S.renote_count(A, su)},before={before}", timeout=120)''',
'''            def boost_seen():
                return True if (S.renote_count(A, su) or 0) > before else None
            out = wait_until(boost_seen, timeout=120)''',
),
(
'''            out = wait_until(lambda: (S.renote_count(A, su) or 0) <= before or
                             f"count={S.renote_count(A, su)},before={before}", timeout=120)''',
'''            def unboost_seen():
                return True if (S.renote_count(A, su) or 0) <= before else None
            out = wait_until(unboost_seen, timeout=120)''',
),
]
for old, new in PATCHES:
    c = s.count(old)
    assert c == 1, (c, old[:80])
    s = s.replace(old, new)
open(DST, "w").write(s)
print("patched", len(PATCHES), "sites ->", DST)
