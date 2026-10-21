\## pinetree storage overhaul — agent implementation plan



Parent reads first: `sns-server-proposal.md §§2,5`, `docs/remote-media-cache-plan.md §§2-6`, `docs/TODOS.md P1`, `AGENTS.md` delegation + config rules, latest `state/logs/`. `git status --short` clean before start. All work leaves uncommitted, no `commit/stash/reset/checkout`.



\### Decision (binding)



\* Single `pinetree.redb`. No split-metadata-DBs — breaks one-txn `state+JOBS+media-metadata` atomicity, triples backup/lock pain, complicates FDB (one store, keyspaces).

\* Two byte pools, two `FsBlobStore` roots:

&#x20; \* `blobs\_durable/` = local uploads. Never evicted. \*\*Per-user quota now.\*\*

&#x20; \* `blobs\_cache/` = remote fetches. Global + per-class caps, LRU+TTL, evict-bytes-only, keep metadata + `source\_url` for re-fetch.

\* Hot small (avatar/banner/emoji) stays a reserved class inside cache, not a third store: 128 MiB / 30d + URL revalidate.



\### Parent-owned shared files (subagents propose snippets, never edit)



\* `docs/remote-media-cache-plan.md`, `docs/endpoint-manifest.md`, `docs/TODOS.md`, `docs/testing.md`

\* `src/application/ports.rs` (`BlobStore`, `MediaStore`, `JobKind`)

\* `src/persistence/redb/mod.rs` (new tables/indexes)

\* `src/config.rs` + `src/main.rs` wiring

\* `src/api/misskey/endpoints/mod.rs` (error/drive usage surfacing)

\* `tests/conformance.rs`



Parent Step 0 lands skeletons only:

1\. Config keys (all optional, defaults, `validate()`, round-trip test per `AGENTS.md`, `deny\_unknown\_fields` stays):

```json

{

&#x20; "durable\_dir": "./data/blobs\_durable",

&#x20; "cache\_dir": "./data/blobs\_cache",

&#x20; "durable\_per\_user\_cap\_bytes": 1073741824,

&#x20; "remote\_cache": {"mode":"on\_demand","global\_cap\_bytes":536870912,"avatar\_emoji\_cap\_bytes":134217728,"preview\_cap\_bytes":268435456,"full\_cap\_bytes":134217728,"avatar\_retention\_days":30,"warming":false}

}

```

Defaults: 1 GiB/user durable, 512 MiB global cache. `validate()`: caps >0, `cache\_dir != durable\_dir`, global >= sum reserved.

2\. New redb tables (names only, no logic): `BLOB\_REFCOUNT{blob\_hash->u64}`, `DURABLE\_BYTES\_BY\_OWNER{owner\_id->u64}`, `REMOTE\_CACHE{source\_url,variant->blob\_hash,class,last\_use}`, `CACHE\_ACCOUNTING{singleton}`. `REMOTE\_CACHE` key = `(source\_url, variant)`, value holds SHA-256 store-key — index ≠ bytes.

3\. `JobKind`: `CacheEvict`, `ReconcileStorage`, `BackfillDurableAccounting`. Payloads <64 KiB, bounded batches 500, no blobs in txn.



\### Agent partition (disjoint writes, unique tests)



\*\*A — durable quota + ref-safe delete.\*\*

Owns: `src/domain/media.rs`, `src/application/commands/media.rs`, `tests/storage\_durable\_quota.rs` (new).

Proposes snippets for: `ports.rs` (`MediaStore::inc/dec\_durable`, `refcount` fns), `redb/mod.rs` table access, `endpoints/mod.rs` error mapping.

Tasks: quota check in `upload\_media` + `upload-from-url` local path before bytes touch disk; in-txn `metadata + DURABLE\_BYTES + REFCOUNT` update; `delete\_media` → decrement, physical `delete` only when count==0, outside txn; over-quota → `400 DRIVE\_CAPACITY\_EXCEEDED` (document divergence); existing over-quota grandfathered (reads/deletes ok, new uploads block); admin same quota v1. Tests: over-quota block, cross-owner same-SHA survivor, local-vs-identical-remote survivor, crash-between-metadata-and-bytes heals.



\*\*B — two stores + migration.\*\*

Owns: `src/media/storage.rs`, `src/media/ingest.rs`, `src/media/mod.rs`, `tests/storage\_two\_pools.rs` (new).

Proposes snippets for: `main.rs` (two `FsBlobStore::new`), `ports.rs` (`BlobStore` handle enum `Durable|Cache`).

Tasks: `FsBlobStore` stays unchanged internally (tmp+rename + `.len` sidecar); add two-instance constructor; migration: keep current `blobs/objects` as durable path, new `blobs\_cache/objects`; lazy move — remote rows with bytes in durable stay readable, reconciler copies + repoints; no flag day. Tests: durable write never lands in cache root and vice versa, pre-migration blob still `get()`s.



\*\*C — remote cache accounting + eviction + on-demand flip.\*\*

Owns: `src/media/remote\_cache.rs`, `src/jobs/worker.rs` (evict/reconcile arms only — coordinate with parent, no other worker edits), `tests/storage\_cache\_evict.rs` (new).

Proposes snippets for: `ports.rs` (`CacheStore`), `redb/mod.rs` cache fns, `federation/inbox.rs` ingest path.

Tasks: ingest records `REMOTE\_CACHE` metadata only; `GET /files|/proxy` triggers fetch-on-first-request with in-flight reservation (concurrent fetches cannot overshoot cap); byte-LRU + TTL per class, reserved avatar/emoji never borrowed by images; evict = remove mapping → update accounting → `delete` if unreferenced; warming stays `false`, relay-excluded. Tests: concurrent-download cap-never-exceeded, evicted re-fetches while source lives, avatar reserve survives image pressure.



\*\*D — stream-to-disk + reconciler + observability.\*\*

Owns: `src/media/fetch\_stream.rs` (new), `src/media/reconciler.rs` (new), `tests/storage\_reconcile.rs` (new).

Proposes snippets for: `ports.rs` (`put\_streamed`), `endpoints/mod.rs` (`server-info|stats` bytes fields), `observability.rs` counters.

Tasks: `fetch\_and\_attach` streams to `tmp/{hash}.pid.seq` under byte cap (plain-HTTP abort mid-body + `curl --max-filesize` preserved, dishonest-length/chunked covered); reconciler idempotent sweep: orphans, missing blobs, stale metadata, `.len` mismatches, pre-slice-8 MD5 rows (purge/migrate, document); counters without shell: `bytes\_used, per\_class, hits/misses, fetched, evictions, failed\_origins`. Tests: oversize aborted mid-stream HTTP+HTTPS, torn write reads `None` then heals, reconciler converges to zero orphans.



FDB parity explicitly out of this batch (Phase 4). No `use foundationdb`, no S3 — but no new code that assumes local-FS-only paths; all pool access via `BlobStore` trait.



\### Verification gate (parent runs)



```bash

cargo test --offline

cargo test --test storage\_durable\_quota --test storage\_two\_pools --test storage\_cache\_evict --test storage\_reconcile

bash scripts/smoke-all.sh

```



Plus: fill cache to 110% with concurrent jobs → stays ≤cap; `rm` a blob file → reconciler re-fetches or marks missing; over-quota user → upload 413-style error, delete then upload succeeds. Paste real output into `state/logs/YYYY-MM-DD-storage-overhaul.md`, update `TODOS.md` + cache plan in place, never rewrite old logs.

