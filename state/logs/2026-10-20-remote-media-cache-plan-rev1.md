# 2026-10-20 — revise remote-media cache plan from review

Two parallel review subagents returned verdicts; all load-bearing claims
spot-verified in-tree before revising (no review point applied on trust).

## Review outcome (pinetree)

- Blocker confirmed with nuance: `delete_media` (`media.rs:147-151`) is real
  but same-owner dups are impossible (`upload_media` early-return,
  `media.rs:43-46`) and remote rows are unreachable via that path
  (`get_media_for` owner-gate, `media.rs:91-100`); the two deletes are also
  non-transactional. Fixed: §2 rewritten with cross-owner scope,
  atomicity rule (metadata in txn, blob delete outside + reconciler), and
  three regression cases.
- Majors applied: §3.5 hedge replaced with precise `delivery.rs` facts
  (mid-body abort `:991-995`, pre-body reject `:928-932`, curl
  `--max-filesize` `:469-470`, chunked slack `:986-1004`) + stream-to-disk
  remaining work (`remote_cache.rs:174,232-254`); per-note semantics pinned
  (per job run, first always attempted; cap = 16 = `MAX_FILE_IDS`);
  §6.8 (ex-§6.7) adds Phase-4 shared S3-compatible store per proposal §2;
  reference index marked new schema; reconciler covers pre-slice-8 MD5 rows
  (`domain/media.rs:21-27`) + sidecar mismatches; §7 gains in-flight,
  HTTPS/chunked, and per-note-semantics tests; §8 notes Birchtree's
  different starting point.

## Changed

- Rewrote `docs/remote-media-cache-plan.md` (Status stays Draft) per above.

## Verified

- Cited code re-read during revision (`delivery.rs`, `media.rs:43-46,91-100`,
  `remote_cache.rs:28,133-136`, `domain/note.rs:15`, `ports.rs:1778-1786`,
  `domain/media.rs:21-27`). Docs-only session; no test run.

## Open

- `download_and_sniff` stream-to-disk (§3.5/§6.6 ordering); whether remote
  rows become directly deletable (§2); parity-plan note if budget diverges
  from Misskey expectations.
