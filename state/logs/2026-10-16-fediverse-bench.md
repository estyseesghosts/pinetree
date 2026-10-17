# 2026-10-16 — Fediverse compatibility bench (5-node matrix, stress + gaps)

## What this session did

Built a five-node local fediverse matrix and stress-tested pinetree against
Misskey, Catodon, and Akkoma. Full report: **`docs/fediverse-bench-2026-10-16.md`**
(topology, metrics, per-bug findings P1–P6, upstream findings U1–U6, harness-bug
appendix). Harness + raw data copied into **`scripts/fediverse-bench/`**
(original working dir `/tmp/opencode/bench/`).

## Rig (still running; browse it)

| Node | Software | Browse to | Federation URL |
|---|---|---|---|
| pt-ks | pinetree + Keystone | `http://localhost:5173` | `http://pt-ks.test` |
| pt-mk | pinetree + Misskey UI (mk-proxy) | `http://localhost:8182` | `http://pt-mk.test` |
| misskey | Misskey 2026.10.1-alpha.0 + Keystone | `http://localhost:5176` | `http://misskey.test` |
| catodon | Catodon 26.10.1-dev.0 (self-contained) | `http://catodon.test` | `http://catodon.test` |
| akkoma | Akkoma 3.20.1 + pleroma-fe | `http://localhost:8099` | `http://akkoma.test` |

30 accounts (6/node, all `password123`): `pk1..6`, `pm1..6`, `mk1..6`,
`cd1..6`, `ak1..6@akkoma.test`. Misskey/Catodon set to `federation: all`.
Akkoma client rate limits lifted via a `prod.secret.exs` bench override (rig
config only, not product code). Full-mesh follow graph (870 edges) verified.
`/etc/hosts` + Windows hosts carry the `.test` names → WSL guest.

## Verified by execution

- Mesh: 870/870 follow edges. Probe matrix: 96/120 (all 24 failures = the
  rig's Akkoma has no search backend; delivery into Akkoma proven by census).
- Feature matrix (11 features × 20 pairs = 260 checks): green on posts, CW,
  followers-only (+no global leak), delete, quotes (native + link), mention
  notifications. Systematic failures narrowed to P1–P6.
- Stress: 3× 900-post storms, 0 create failures, ~36 s wall each,
  p50 ~180–250 ms; full 5-node convergence within +15 s on clean batches.
  205/205 cross-node reactions landed; votes tallied on 4/5 nodes; media
  byte-verified 25/25; 240-reply storm 60/60 parent linkage; reaction RTT
  p50 1.02 s; 2 900-char posts intact on 5/5.

## Pinetree bugs/gaps found (details + repro in the report)

- **P1** duplicate note rows under multi-source fan-in (same AP `uri`, two
  local ids → duplicated timeline entries). Highest priority.
- **P2** `notes/reactions` (list) unimplemented (documented gap, but
  Misskey-client-visible 404).
- **P3** reaction-undo non-convergence under load (all misskey-family pairs
  incl. stock Misskey↔Catodon — not purely pinetree; low-load undo is fine).
- **P4** `replyId` loss at pinetree origin for remote replies (load-sensitive
  race: on-demand `ap/show` vs inbox parent-linking).
- **P5** remote-media attachment linking slow under load (>90 s; instant when
  idle).
- **P6** poll-vote tallying at pinetree origin delayed under load.

## Open questions / next steps

- Is P3 an upstream 26.10 regression (stock Misskey↔Catodon control run) or a
  shared load-sensitivity? Pinetree's P1 duplicate rows may compound it.
- P1 root cause: dedup keyed by local id rather than AP `uri`; confirm whether
  the on-demand `ap/show` insert races the inbox insert.
- Upstream (not pinetree): Akkoma favs/mentions invisible to Misskey-family
  (U1/U2); Misskey inbound lag under cold burst (U3); Akkoma shows deleted
  notes in timelines (U4).

## Methodology lesson (recorded, not repeated)

Running the probe matrix concurrently with the feature matrix produced
phantom followers-only/reply/renote failures (timeline floods + rate limits).
All reported phases were re-run sequentially. Several apparent bugs were
traced to harness errors (wrong reaction/notifications/poll field shapes,
`204` vs `200`) — each dismissed only after raw-wire confirmation; see the
report's §7 appendix so they aren't re-litigated.

## State-log convention note

This entry supersedes nothing; it is additive. Prior 2026-10-16 logs
(misskey-parity-complete, demo-live, audit-*) are untouched. New files only:
`docs/fediverse-bench-2026-10-16.md`, `scripts/fediverse-bench/**`, and this
log. No product code changed; the only repo edits are these docs/scripts.
