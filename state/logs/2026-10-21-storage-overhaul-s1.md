# 2026-10-21 — pinetree storage overhaul S1 (shared contracts)

## What changed (parent slice, committed `5b623ae`)
- `src/config.rs`: `RemoteCacheMode` (`disabled|on_demand|warm`, default
  on-demand) + 10 storage keys (1 GiB durable quota, 512 MiB global cache,
  128/256/128 class reserves, TTLs 30/7/3, warming off). All optional with
  defaults; `validate()` covers all; round-trip + rejection tests added.
- `src/application/ports.rs`: `MediaClass`, `RemoteCacheRecord`,
  `CacheStats`; `MediaStore` gains quota/refcount/remote-cache/accounting/
  reservation methods (with safe defaults); `BlobStore` gains
  `byte_len`/`import_file` defaults; `set_cache_cap` hook.
- `src/application/commands/mod.rs`: `AppServices` gains `cache_blobs`
  (remote pool; `blobs` stays durable) + tuning fields. All 14 in-repo
  harness literals updated (script, `blobs.clone()` for tests; real split
  in `main.rs`), except `src/media/remote_cache.rs` (subagent B owns it).
- `src/persistence/redb/mod.rs`: new tables `BLOB_REFCOUNT`,
  `DURABLE_BYTES_BY_OWNER`, `REMOTE_CACHE`, `CACHE_ACCOUNTING` in the single
  `pinetree.redb`; quota-checked insert (`DRIVE_CAPACITY_EXCEEDED`),
  accounting-consistent delete, reserve/release, touch/LRU candidates,
  stats; open-time backfill (`migration:storage_accounting:v1`) rebuilds
  refcounts/totals/cache index from `MEDIA_META` (grandfathered rows kept).
- `src/main.rs`: `blobs_durable/` + `blobs_cache/` split, one-time legacy
  `blobs/` rename, CLI flags for every new key.
- `src/api/misskey/endpoints/mod.rs`: `serve_file` pool split + evicted
  re-fetch via `fetch_on_first_request` (lands with subagent B);
  `drive/cache/stats` (`read:drive`) shell-free observability.
- `src/media/mod.rs`: declares `fetch_stream` + `reconciler` (subagent
  files landing next).
- Docs: `docs/TODOS.md` P1 WIP banner; `docs/remote-media-cache-plan.md`
  2026-10-21 WIP banner (addenda only, no history rewritten).

## Delegation (disjoint file sets, background subagents)
- A (`ses_ea2fd4de6ffeAfo0GDTZhhh1Vv`): `src/domain/media.rs`,
  `src/media/storage.rs`, `src/media/fetch_stream.rs` (new).
- B (`ses_ea2fd4de6ffe9aBQTFsaxjVpzV`): `src/application/commands/media.rs`,
  `src/media/reconciler.rs` (new), `src/media/remote_cache.rs`.
- C (`ses_ea2fd4de4ffePbNlQ8G6mkdhs2`): `tests/storage_durable_quota.rs`,
  `tests/storage_cache_budget.rs`, `tests/storage_eviction_heal.rs` (new).
- Shared files parent-owned; subagents instructed to code against the exact
  S1 interfaces above and leave changes uncommitted.

## Verification
- `cargo test --offline` + `bash scripts/smoke-all.sh` deferred until all
  three subagents land and the tree compiles (S1 alone forward-references
  `fetch_on_first_request`/`fetch_stream`/`reconciler`).
- Pre-existing untracked `docs/database.md` left untouched; new untracked
  `docs/gaps.md` appeared during subagent work — will triage at merge.

## Open questions / merge risks
- `MediaClass` now defined in `ports.rs`; agent A was asked for a domain
  enum (domain must not import application). At merge, keep the
  `media_class: String` field and reconcile to one canonical definition.
- `serve_file` passes `"original"` variant until agent A's `meta.variant`
  lands (one-line merge fix).
- `reserve_cache_bytes(bytes)` uses a persisted `cap` via `set_cache_cap`;
  `main.rs` must call it at boot (merge task).
