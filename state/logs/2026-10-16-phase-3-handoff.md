# 2026-10-16 — Phase 3 handoff prompt (as given to the next implementer)

Context: owner asked for a copy-paste handoff prompt for the next implementer
to complete Phase 3 in entirety — with subagent use, test-along-the-way, and
the four-server test topology (pinetree+misskey, pinetree+keystone,
misskey+keystone, sharkey-native). Delivered in chat; recorded here verbatim.

---

```
You are taking over as the implementer on the pinetree repo (/root/pinetree).
Your job: COMPLETE PHASE 3 (Client compatibility and streaming) IN ITS ENTIRETY.

## 0. House rules (non-negotiable)

- Read FIRST, in order: AGENTS.md, sns-server-proposal.md §§6+9 (proposal wins
  on any disagreement), docs/README.md, docs/wiki/Roadmap.md ("Phase 3 in more
  detail"), docs/wiki/Current-State.md ("Known gaps"), docs/endpoint-manifest.md
  (frozen — changes only via a logged superseding decision), the latest entries
  in state/logs/, and state/logs/2026-10-16-phase-2-audit.md (Phase 2 audit).
- Architecture invariants (proposal §§2–5): domain ← application ← adapters;
  Misskey API + ActivityPub share AppServices ops; one tx commits state +
  outbox jobs; FDB limits respected; no hostnames in public IDs/URLs;
  federation input is untrusted (SSRF/sizes/timeouts/signatures).
- USE SUBAGENTS aggressively: this phase decomposes into parallel workstreams
  (streaming, endpoint coverage, contract tests, client verification). Fan out
  explore/implementer subagents, keep slices small and landable, one task at a
  time per agent.
- TEST ALONG THE WAY, not at the end: every slice lands with `cargo test`
  green, relevant live-stack checks executed, and real command output pasted or
  referenced in the state log. Never claim verification you did not execute.
  "Not yet verified" when it wasn't run.
- STATE LOGS every session: append one dated state/logs/YYYY-MM-DD-<topic>.md
  (what changed, evidence, open questions). Never rewrite past logs.

## 1. Phase 3 exit criterion (proposal §9 — you are done when this holds)

Every endpoint in the DECLARED COMPATIBILITY PROFILE passes contract tests
against the TARGET CLIENT VERSIONS. Non-goals (do not build): Mastodon API;
antennas/clips/pages/channels/chat/Reversi and legacy Misskey features (unless
a target client requires them); polls/_misskey_votes and quote-metadata
federation; RFC 9421 signatures; OAuth; multi-node/FDB (Phase 4); scale tuning
(Phase 5). Goal is "real clients work against a small fast server," NOT parity.

## 2. Step one: declare the profile

Name exact target client versions (minimum: pinned Misskey web client
2026.10.1-alpha.0, misskey-js, Keystone, Sharkey 2025.4.7 web client), list
every endpoint/channel/field in scope, and extend docs/endpoint-manifest.md.
Known starting gaps (Current-State.md): NO /streaming route (src/streaming/*
are stubs, StreamingSink is a no-op); missing users/notes, notes/replies,
notes/renotes, users/followers|following ENDPOINTS (stores exist —
endpoint-only work), drive listing/folders, richer note/profile fields,
i/update subset.

## 3. Workstreams (parallelize with subagents)

A. Streaming: authenticated WS /streaming, connect/disconnect, channels
   (main/homeTimeline/localTimeline/hybridTimeline/globalTimeline), note
   capture (subNote/unsubNote/noteUpdated), notification events — fed from
   StreamingSink jobs. B. Endpoint coverage per the profile. C. Contract tests
   vs the REAL clients (no mocks) for every profile endpoint/channel.
   Suggested order per proposal §6: core flows → streaming → interactions →
   profile-optionals (quotes/polls/emoji/mute/block/lists/search) ONLY if in
   the profile.

## 4. Test topology — FOUR live servers (this is the current standing rig)

Bring up with: KEEP=1 SHARKEY_NODE_BIN=/usr/sbin scripts/smoke-phase2.sh
(Node 24 default for Misskey/re2, Node 22 via SHARKEY_NODE_BIN for Sharkey;
fresh pg/redis; SMOKE_OK expected). Then:
  1. pinetree+misskey — pinetree backend :3103 fronted by the Misskey UI via
     the /tmp/opencode/mk-proxy.mjs rig (:8182, PINETREE_UPSTREAM=127.0.0.1:3103;
     needs Misskey vite dev). See state/logs/2026-10-15-interop-browser-rig.md.
  2. pinetree+keystone — pinetree pt5 :3000 (public 127.0.1.5 + proxy),
     Keystone default vite :5173. User ks1/password123.
  3. misskey+keystone — Misskey backend-only :3101 (KEYSTONE_BACKEND_ONLY=1
     patch in packages/backend/src/server/ServerService.ts + built output),
     Keystone --mode misskey :5176. User mk1/password123.
  4. sharkey-native — Sharkey backend :3102 with SHARKEY'S OWN frontend
     (VITE_PORT=5174 vite dev, backend restarted with VITE_PORT=5174
     EMBED_VITE_PORT=5175, MK_DISABLE_CLUSTERING=1, WITHOUT backend-only mode).
     User sk1/password123. All passwords are password123.
Docs: docs/phase-2-interop-howto.md (topology, proxy, accounts, gotchas).
Quirks you must know: Misskey/Sharkey federation defaults to none → set all;
NODE_ENV=development + MISSKEY_WEBFINGER_USE_HTTP=true; 127.0.1.x:80 via
scripts/interop/proxy.mjs (WebFinger drops ports); warm pinetree RSA keys
(lazy keygen is slow in dev); unlock pt3 after the interop test (it leaves it
locked); Sharkey has a 30-min in-memory followers cache that stalls delivery
after follow churn (restart backend); Misskey reuses Follow/Undo ids
(dedup forgets them — already handled); media URLs embed 127.0.1.x (won't
render off-box; text interop unaffected).

## 5. Environment notes (as of 2026-10-16 handoff)

- Keystone vites currently on 0.0.0.0 (:5173/:5176/:5177) for tailnet access;
  Windows host needs netsh portproxy + firewall allows per port; guest IP
  (currently 172.28.54.228) CHANGES on WSL restart — durable fix is mirrored
  networking in .wslconfig (needs WSL restart; kills the stack).
- A fresh KEEP smoke re-run may be in flight when you arrive — check
  `ss -tlnp`, /tmp/pinetree-interop/*.log, and state/logs/ before touching
  backends. Stop everything: scripts/smoke-phase2.sh stop (+ pt5/proxy/vites).
- Bench: scripts/bench-local.py against a THROWAWAY pinetree (:3210), never
  the play stack.

Report back: profile declaration, slice plan with workstream→subagent mapping,
then per-slice evidence. Do not stop at "code complete" — exit needs executed
contract-test proof against every target client.
```
