# 2026-10-20 — remote-media cache plan (storage-cache.md review)

## Changed

- New `docs/remote-media-cache-plan.md` (Status: Draft): Pinetree-specific
  forward plan from `/root/storage-cache.md`. Records existing blocks
  (`src/media/remote_cache.rs`, per-note 64 MiB cap, SHA-256 blob store),
  the fix-first shared-blob deletion bug
  (`src/application/commands/media.rs:147-151`), ordered changes
  (reference-safe deletion → URL/variant index → metadata-preserving
  eviction → on-demand default → pool separation → global/class budgets),
  shared policy (disabled/on_demand/warm, 512 MiB starting budget), FDB
  adapter note, and acceptance tests.
- `docs/README.md`: indexed the new plan.

## Verified

- Spot-checked cited files (`src/media/remote_cache.rs`,
  `src/application/commands/media.rs:145-152`, `src/media/storage.rs`)
  before writing. No product code changed; no test run (docs-only session).

## Open

- Land the §2 shared-blob deletion fix + regression test before any
  eviction work; verify `download_and_sniff` aborts mid-stream on
  dishonest/absent `Content-Length`.
- If the global/class budget diverges from Misskey expectations, note it in
  `docs/misskey-parity-plan.md`.
