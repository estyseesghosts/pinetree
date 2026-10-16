# 2026-10-16 — Live demo handoff prompt (as given to the next implementer)

Recorded verbatim; delivered in chat as a copy-paste block.

---

```
You are taking over on the pinetree repo (/root/pinetree). Your job: bring
up a LIVE browsable demo and seed it with federated content (including
media) so the owner can click through it from a Windows browser and see
that this thing works. Master is green (Phase 3 + audit fixes committed);
do not change product code unless the demo exposes a bug (if it does, log
it and fix minimally with a test).

## 0. Read first

AGENTS.md, docs/testing.md (the topology table + bring-up commands are
the recipe — follow them), docs/phase-2-interop-howto.md (gotchas), and
the latest state/logs/ entries.

## 1. Target array (all four, all browsable from Windows)

| Combo | Frontend | Backend | User |
|---|---|---|---|
| pinetree/keystone | Keystone vite :5173 (default mode) | pinetree pt5 :3000 (existing data dir keeps ks1) | ks1/password123 |
| pinetree/misskey | Misskey UI via mk-proxy :8182 + Misskey vite dev (stop Keystone :5173 first — same port) | pinetree :3103 | pt3/password123 |
| misskey/keystone | Keystone vite :5176 (--mode misskey) | Misskey :3101 | mk1/password123 |
| sharkey/sharkey | Sharkey native UI at :3102 + its vite :5174 | Sharkey :3102 (restart with VITE_PORT=5174) | sk1/password123 |

Guest IP for the Windows portproxy rows: check `hostname -I` (was
172.28.54.228; one netsh + firewall rule PER port). All passwords are
password123.

## 2. Bring-up order

1. Fresh base: KEEP=1 SHARKEY_NODE_BIN=/usr/sbin bash
   scripts/smoke-phase2.sh → SMOKE_OK (fresh DBs; recreates mk1/sk1/pt3/pt4).
   If its completion pipe wedges post-INTEROP_OK (known flake: lingering
   start_bg subshells hold the pipe), kill the lingering script processes —
   backends survive — and continue.
2. pt5 (:3000, same data dir so ks1 survives) + the three Keystone vites
   (:5173/:5176/:5177, Node 24). Verify each /api/meta names the right backend.
3. Sharkey-native: frontend prebuilds (Node 22) + vite :5174, restart :3102
   backend with VITE_PORT=5174 EMBED_VITE_PORT=5175 (same DB). Verify `/`
   shell + `/vite` 200 + sk1 signin-flow.
4. pinetree/misskey rig LAST (it needs :5173 for the Misskey vite — port
   conflict with Keystone :5173): PROXY_LISTEN=127.0.0.1:8182
   PINETREE_UPSTREAM=127.0.0.1:3103 bun /tmp/opencode/mk-proxy.mjs.
   Tell the owner this combo displaces Keystone :5173 while it runs.

## 3. Seed federated content (via APIs, then verify cross-server)

- Text posts on all four nodes; replies across servers (e.g. mk1 replies
  to a pt5 note, sk1 replies to mk1); renotes across servers; at least one
  followers-only post (visible to followers, absent on anonymous timelines).
- Media: upload a PNG to pt5 (multipart drive/files/create), attach it to a
  federated post; confirm the DriveFile + /files bytes from another node.
- Follows: ks1↔mk1, ks1↔sk1, pt3↔mk1 (unlock pt3 first — the interop test
  leaves it locked; warm RSA keys before federating).
- Verify API-side: every seeded note retrievable cross-server
  (notes/show + timelines on at least two nodes per item), notifications
  present for mentions/replies/follows, one live-streaming check
  (WS /streaming globalTimeline receives a federated note).

## 4. Rules

- Never bench or load-test the play stack.
- Never rewrite state/logs/; append one dated log with what runs, what was
  seeded (ids/URLs), and anything that failed.
- If a UI fails to load, check in order: backend up? vite up? VITE_PORT in
  /proc/<pid>/environ correct? /vite assets 200 (not ECONNREFUSED)?
  Windows portproxy row present for that port?
- Known upstream quirk: Sharkey's in-memory followers cache can stall
  delivery after follow churn — restart its backend if fan-out stalls.

Report back: per-combo URL + login + what to click to see federation
working (3–5 concrete click paths), the seeded content ids, any failures,
and the state log path. Leave everything running.
```
